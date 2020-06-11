// This file is part of Hangfire.PostgreSql.
// Copyright © 2014 Frank Hommers <http://hmm.rs/Hangfire.PostgreSql>.
// 
// Hangfire.PostgreSql is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.PostgreSql  is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.PostgreSql. If not, see <http://www.gnu.org/licenses/>.
//
// This work is based on the work of Sergey Odinokov, author of 
// Hangfire. <http://hangfire.io/>
//   
//    Special thanks goes to him.

using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    public class PostgreSqlStorage : JobStorage
    {
        private readonly NpgsqlConnection _existingConnection;
        private readonly Action<NpgsqlConnection> _connectionSetup;
        private readonly PostgreSqlStorageOptions _options;
        private readonly string _connectionString;

        public PostgreSqlStorage(string nameOrConnectionString)
            : this(nameOrConnectionString, new PostgreSqlStorageOptions())
        {
        }

        public PostgreSqlStorage(string nameOrConnectionString, PostgreSqlStorageOptions options)
            : this(nameOrConnectionString, null, options)
        {
        }

        /// <summary>
        /// Initializes PostgreSqlStorage from the provided PostgreSqlStorageOptions and either the provided connection
        /// string or the connection string with provided name pulled from the application config file.       
        /// </summary>
        /// <param name="nameOrConnectionString">Either a SQL Server connection string or the name of 
        /// a SQL Server connection string located in the connectionStrings node in the application config</param>
        /// <param name="connectionSetup">Optional setup action to apply to created connections</param>
        /// <param name="options"></param>
        /// <exception cref="ArgumentNullException"><paramref name="nameOrConnectionString"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> argument is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="nameOrConnectionString"/> argument is neither 
        /// a valid SQL Server connection string nor the name of a connection string in the application
        /// config file.</exception>
        public PostgreSqlStorage(string nameOrConnectionString, Action<NpgsqlConnection> connectionSetup,
	        PostgreSqlStorageOptions options)
        {
	        if (nameOrConnectionString == null) throw new ArgumentNullException(nameof(nameOrConnectionString));

	        _options = options ?? throw new ArgumentNullException(nameof(options));

	        if (IsConnectionString(nameOrConnectionString))
	        {
		        _connectionString = nameOrConnectionString;
	        }
	        else
	        {
		        throw new ArgumentException(
			        $"Could not find connection string with name '{nameOrConnectionString}' in application config file");
	        }

	        _connectionSetup = connectionSetup;

	        if (options.PrepareSchemaIfNecessary)
	        {
		        using (var connection = CreateAndOpenConnection())
		        {
			        PostgreSqlObjectsInstaller.Install(connection, options.SchemaName);
		        }
	        }

	        InitializeQueueProviders();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgreSqlStorage"/> class with
        /// explicit instance of the <see cref="NpgsqlConnection"/> class that will be used
        /// to query the data.
        /// </summary>
        /// <param name="existingConnection">Existing connection</param>
        /// <param name="options">PostgreSqlStorageOptions</param>
        public PostgreSqlStorage(NpgsqlConnection existingConnection, PostgreSqlStorageOptions options)
        {
            if (existingConnection == null) throw new ArgumentNullException(nameof(existingConnection));
            if (options == null) throw new ArgumentNullException(nameof(options));
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(existingConnection.ConnectionString);

            if (!options.EnableTransactionScopeEnlistment)
            {
                if (connectionStringBuilder.Enlist)
                    throw new ArgumentException(
                        $"TransactionScope enlistment must be enabled by setting {nameof(PostgreSqlStorageOptions)}.{nameof(options.EnableTransactionScopeEnlistment)} to `true`.");
            }

            _existingConnection = existingConnection;
            _options = options;

            InitializeQueueProviders();
        }

        public PostgreSqlStorage(NpgsqlConnection existingConnection)
        {
            if (existingConnection == null) throw new ArgumentNullException(nameof(existingConnection));
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(existingConnection.ConnectionString);
            if (connectionStringBuilder.Enlist)
                throw new ArgumentException(
                    $"TransactionScope enlistment must be enabled by setting {nameof(PostgreSqlStorageOptions)}.{nameof(PostgreSqlStorageOptions.EnableTransactionScopeEnlistment)} to `true`.");

            _existingConnection = existingConnection;
            _options = new PostgreSqlStorageOptions();

            InitializeQueueProviders();
        }

        public PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new PostgreSqlMonitoringApi(this, _options, QueueProviders);
        }

        public override IStorageConnection GetConnection()
        {
            var connection = _existingConnection ?? CreateAndOpenConnection();

            return new PostgreSqlConnection(connection, QueueProviders, _options, _existingConnection == null);
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(this, _options);
            //TODO: add counters aggregator? (like https://github.com/HangfireIO/Hangfire/blob/master/src/Hangfire.SqlServer/SqlServerStorage.cs#L154)
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.InfoFormat("    Queue poll interval: {0}.", _options.QueuePollInterval);
            logger.InfoFormat("    Invisibility timeout: {0}.", _options.InvisibilityTimeout);
        }

        public override string ToString()
        {
            const string canNotParseMessage = "<Connection string can not be parsed>";

            try
            {
                var connectionStringBuilder = new NpgsqlConnectionStringBuilder(_connectionString);
                var builder = new StringBuilder();

                builder.Append("Host: ");
                builder.Append(connectionStringBuilder.Host);
                builder.Append(", DB: ");
                builder.Append(connectionStringBuilder.Database);
                builder.Append(", Schema: ");
                builder.Append(_options.SchemaName);

                return builder.Length != 0
                    ? $"PostgreSQL Server: {builder}"
                    : canNotParseMessage;
            }
            catch (Exception)
            {
                return canNotParseMessage;
            }
        }

        internal NpgsqlConnection CreateAndOpenConnection()
        {
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(_connectionString);
            if (!_options.EnableTransactionScopeEnlistment)
            {
                connectionStringBuilder.Enlist = false;
            }

            var connection = new NpgsqlConnection(connectionStringBuilder.ToString());

            _connectionSetup?.Invoke(connection);
            connection.Open();

            return connection;
        }

        internal T UseConnection<T>(Func<IDbConnection, T> func)
        {
	        IDbConnection connection = null;

	        try
	        {
		        connection = CreateAndOpenConnection();
		        return func(connection);
	        }
	        finally
	        {
		        ReleaseConnection(connection);
	        }
        }

        internal void ReleaseConnection(IDbConnection connection)
        {
	        if (connection != null && !IsExistingConnection(connection))
                connection.Dispose();
        }

        private bool IsExistingConnection(IDbConnection connection)
        {
	        return connection != null && ReferenceEquals(connection, _existingConnection);
        }

        private void InitializeQueueProviders()
        {
            var defaultQueueProvider = new PostgreSqlJobQueueProvider(this, _options);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
        }

        private bool IsConnectionString(string nameOrConnectionString)
        {
            return nameOrConnectionString.Contains(";");
        }
    }
}