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
using System.Configuration;
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
		private readonly PostgreSqlStorageOptions _options;
		private readonly string _connectionString;

		public PostgreSqlStorage(string nameOrConnectionString)
			: this(nameOrConnectionString, new PostgreSqlStorageOptions())
		{
		}

		/// <summary>
		/// Initializes PostgreSqlStorage from the provided PostgreSqlStorageOptions and either the provided connection
		/// string or the connection string with provided name pulled from the application config file.       
		/// </summary>
		/// <param name="nameOrConnectionString">Either a SQL Server connection string or the name of 
		/// a SQL Server connection string located in the connectionStrings node in the application config</param>
		/// <param name="options"></param>
		/// <exception cref="ArgumentNullException"><paramref name="nameOrConnectionString"/> argument is null.</exception>
		/// <exception cref="ArgumentNullException"><paramref name="options"/> argument is null.</exception>
		/// <exception cref="ArgumentException"><paramref name="nameOrConnectionString"/> argument is neither 
		/// a valid SQL Server connection string nor the name of a connection string in the application
		/// config file.</exception>
		public PostgreSqlStorage(string nameOrConnectionString, PostgreSqlStorageOptions options)
		{
			if (nameOrConnectionString == null) throw new ArgumentNullException("nameOrConnectionString");
			if (options == null) throw new ArgumentNullException("options");

			_options = options;

			if (IsConnectionString(nameOrConnectionString))
			{
				_connectionString = nameOrConnectionString;
			}
			else if (IsConnectionStringInConfiguration(nameOrConnectionString))
			{
				_connectionString = ConfigurationManager.ConnectionStrings[nameOrConnectionString].ConnectionString;
			}
			else
			{
				throw new ArgumentException(
					string.Format("Could not find connection string with name '{0}' in application config file",
						nameOrConnectionString));
			}

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
			if (existingConnection == null) throw new ArgumentNullException("existingConnection");
			if (options == null) throw new ArgumentNullException("options");
			var connectionStringBuilder = new NpgsqlConnectionStringBuilder(existingConnection.ConnectionString);
			if (connectionStringBuilder.Enlist)
				throw new ArgumentException(
					"Npgsql is not fully compatible with TransactionScope yet, only connections without Enlist = true are accepted.");

			_existingConnection = existingConnection;
			_options = options;

			InitializeQueueProviders();
		}


		public PersistentJobQueueProviderCollection QueueProviders { get; private set; }

		public override IMonitoringApi GetMonitoringApi()
		{
			return new PostgreSqlMonitoringApi(_connectionString, _options, QueueProviders);
		}

		public override IStorageConnection GetConnection()
		{
			var connection = _existingConnection ?? CreateAndOpenConnection();
			return new PostgreSqlConnection(connection, QueueProviders, _options, _existingConnection == null);
		}

		public override IEnumerable<IServerComponent> GetComponents()
		{
			yield return new ExpirationManager(this, _options);
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
					? string.Format("PostgreSQL Server: {0}", builder)
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
			connectionStringBuilder.Enlist = false; //Npgsql is not fully compatible with TransactionScope yet.

			var connection = new NpgsqlConnection(connectionStringBuilder.ToString());
			connection.Open();

			return connection;
		}

		private void InitializeQueueProviders()
		{
			var defaultQueueProvider = new PostgreSqlJobQueueProvider(_options);
			QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
		}

		private bool IsConnectionString(string nameOrConnectionString)
		{
			return nameOrConnectionString.Contains(";");
		}

		private bool IsConnectionStringInConfiguration(string connectionStringName)
		{
			var connectionStringSetting = ConfigurationManager.ConnectionStrings[connectionStringName];

			return connectionStringSetting != null;
		}
	}
}