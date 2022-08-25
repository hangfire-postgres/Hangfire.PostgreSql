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
using System.Data.Common;
using System.Text;
using System.Transactions;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;
using IsolationLevel = System.Transactions.IsolationLevel;

namespace Hangfire.PostgreSql
{
  public class PostgreSqlStorage : JobStorage
  {
    private readonly IConnectionFactory _connectionFactory;
    private readonly Action<NpgsqlConnection> _connectionSetup;
    private readonly NpgsqlConnectionStringBuilder _connectionStringBuilder;
    private readonly NpgsqlConnection _existingConnection;

    public PostgreSqlStorage(string connectionString)
      : this(connectionString, new PostgreSqlStorageOptions()) { }

    public PostgreSqlStorage(string connectionString, PostgreSqlStorageOptions options)
      : this(connectionString, null, options) { }

    public PostgreSqlStorage(IConnectionFactory connectionFactory, PostgreSqlStorageOptions options)
    {
      _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
      Options = options ?? throw new ArgumentNullException(nameof(options));

      if (options.PrepareSchemaIfNecessary)
      {
        using NpgsqlConnection connection = CreateAndOpenConnection();
        PostgreSqlObjectsInstaller.Install(connection, options.SchemaName);
      }

      InitializeQueueProviders();
    }

    /// <summary>
    ///   Initializes PostgreSqlStorage from the provided PostgreSqlStorageOptions and either the provided connection string.
    /// </summary>
    /// <param name="connectionString">PostgreSQL connection string</param>
    /// <param name="connectionSetup">Optional setup action to apply to created connections</param>
    /// <param name="options">Storage options</param>
    /// <exception cref="ArgumentNullException"><paramref name="connectionString" /> argument is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options" /> argument is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="connectionString" /> argument not a valid PostgreSQL connection string config file.</exception>
    public PostgreSqlStorage(
      string connectionString,
      Action<NpgsqlConnection> connectionSetup,
      PostgreSqlStorageOptions options)
    {
      if (connectionString == null)
      {
        throw new ArgumentNullException(nameof(connectionString));
      }

      Options = options ?? throw new ArgumentNullException(nameof(options));

      if (!TryCreateConnectionStringBuilder(connectionString, out NpgsqlConnectionStringBuilder builder))
      {
        throw new ArgumentException($"Connection string [{connectionString}] is not valid", nameof(connectionString));
      }

      _connectionStringBuilder = SetupConnectionStringBuilderParameters(builder);
      _connectionSetup = connectionSetup;

      if (options.PrepareSchemaIfNecessary)
      {
        using NpgsqlConnection connection = CreateAndOpenConnection();
        PostgreSqlObjectsInstaller.Install(connection, options.SchemaName);
      }

      InitializeQueueProviders();
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="PostgreSqlStorage" /> class with
    ///   explicit instance of the <see cref="NpgsqlConnection" /> class that will be used
    ///   to query the data.
    /// </summary>
    /// <param name="existingConnection">Existing connection</param>
    /// <param name="options">PostgreSqlStorageOptions</param>
    public PostgreSqlStorage(NpgsqlConnection existingConnection, PostgreSqlStorageOptions options)
    {
      if (existingConnection == null)
      {
        throw new ArgumentNullException(nameof(existingConnection));
      }

      if (options == null)
      {
        throw new ArgumentNullException(nameof(options));
      }

      NpgsqlConnectionStringBuilder connectionStringBuilder = new(existingConnection.ConnectionString);

      if (!options.EnableTransactionScopeEnlistment)
      {
        if (connectionStringBuilder.Enlist)
        {
          throw new ArgumentException($"TransactionScope enlistment must be enabled by setting {nameof(PostgreSqlStorageOptions)}.{nameof(options.EnableTransactionScopeEnlistment)} to `true`.");
        }
      }

      _existingConnection = existingConnection;
      Options = options;

      InitializeQueueProviders();
    }

    public PostgreSqlStorage(NpgsqlConnection existingConnection)
    {
      if (existingConnection == null)
      {
        throw new ArgumentNullException(nameof(existingConnection));
      }

      NpgsqlConnectionStringBuilder connectionStringBuilder = new(existingConnection.ConnectionString);
      if (connectionStringBuilder.Enlist)
      {
        throw new ArgumentException($"TransactionScope enlistment must be enabled by setting {nameof(PostgreSqlStorageOptions)}.{nameof(PostgreSqlStorageOptions.EnableTransactionScopeEnlistment)} to `true`.");
      }

      _existingConnection = existingConnection;
      Options = new PostgreSqlStorageOptions();

      InitializeQueueProviders();
    }

    public PersistentJobQueueProviderCollection QueueProviders { get; internal set; }

    internal PostgreSqlStorageOptions Options { get; }

    public override IMonitoringApi GetMonitoringApi()
    {
      return new PostgreSqlMonitoringApi(this, QueueProviders);
    }

    public override IStorageConnection GetConnection()
    {
      return new PostgreSqlConnection(this);
    }

    public override IEnumerable<IServerComponent> GetComponents()
    {
      yield return new ExpirationManager(this);
      //TODO: add counters aggregator? (like https://github.com/HangfireIO/Hangfire/blob/master/src/Hangfire.SqlServer/SqlServerStorage.cs#L154)
    }

    public override void WriteOptionsToLog(ILog logger)
    {
      logger.Info("Using the following options for SQL Server job storage:");
      logger.InfoFormat("    Queue poll interval: {0}.", Options.QueuePollInterval);
      logger.InfoFormat("    Invisibility timeout: {0}.", Options.InvisibilityTimeout);
    }

    public override string ToString()
    {
      const string canNotParseMessage = "<Connection string can not be parsed>";

      try
      {
        StringBuilder builder = new();

        builder.Append("Host: ");
        builder.Append(_connectionStringBuilder.Host);
        builder.Append(", DB: ");
        builder.Append(_connectionStringBuilder.Database);
        builder.Append(", Schema: ");
        builder.Append(Options.SchemaName);

        return builder.Length != 0
          ? $"PostgreSQL Server: {builder}"
          : canNotParseMessage;
      }
      catch (Exception)
      {
        return canNotParseMessage;
      }
    }

    /// <summary>
    /// Timezone must be UTC for compatibility with Npgsql 6 and our usage of "timestamp without time zone" columns
    /// See https://github.com/frankhommers/Hangfire.PostgreSql/issues/221
    /// </summary>
    /// <param name="connectionStringBuilder">The ConnectionStringBuilder to set the Timezone property for</param>
    internal static void SetTimezoneToUtcForNpgsqlCompatibility(NpgsqlConnectionStringBuilder connectionStringBuilder)
    {
      connectionStringBuilder.Timezone = "UTC";
    }

    internal NpgsqlConnection CreateAndOpenConnection()
    {
      NpgsqlConnection connection;

      if (_connectionFactory is not null)
      {
        connection = _connectionFactory.GetOrCreateConnection();

        if (!Options.EnableTransactionScopeEnlistment)
        {
          NpgsqlConnectionStringBuilder connectionStringBuilder;
#if !USING_NPGSQL_VERSION_5
          connectionStringBuilder = connection.Settings;
#else
          connectionStringBuilder = new(connection.ConnectionString);
#endif
          if (connectionStringBuilder.Enlist)
          {
            throw new ArgumentException(
              $"TransactionScope enlistment must be enabled by setting {nameof(PostgreSqlStorageOptions)}.{nameof(Options.EnableTransactionScopeEnlistment)} to `true`.");
          }
        }

      }
      else
      {
        connection = _existingConnection;
        if (connection == null)
        {
          connection = new NpgsqlConnection(_connectionStringBuilder.ToString());
          _connectionSetup?.Invoke(connection);
        }
      }

      try
      {
        if (connection.State == ConnectionState.Closed)
        {
          connection.Open();
        }

        return connection;
      }
      catch
      {
        ReleaseConnection(connection);
        throw;
      }
    }

    internal void UseTransaction(DbConnection dedicatedConnection,
      [InstantHandle] Action<DbConnection, IDbTransaction> action,
      IsolationLevel? isolationLevel = null)
    {
      UseTransaction(dedicatedConnection, (connection, transaction) => {
        action(connection, transaction);
        return true;
      }, isolationLevel);
    }

    internal T UseTransaction<T>(DbConnection dedicatedConnection,
      [InstantHandle] Func<DbConnection, IDbTransaction, T> func,
      IsolationLevel? isolationLevel = null)
    {
      // Use isolation level of an already opened transaction in order to avoid isolation level conflict
      isolationLevel ??= Transaction.Current?.IsolationLevel ?? IsolationLevel.ReadCommitted;

      if (!EnvironmentHelpers.IsMono())
      {
        using TransactionScope transaction = CreateTransaction(isolationLevel);
        T result = UseConnection(dedicatedConnection, connection => {
          connection.EnlistTransaction(Transaction.Current);
          return func(connection, null);
        });

        transaction.Complete();

        return result;
      }

      return UseConnection(dedicatedConnection, connection => {
        System.Data.IsolationLevel transactionIsolationLevel = ConvertIsolationLevel(isolationLevel) ?? System.Data.IsolationLevel.ReadCommitted;
        using DbTransaction transaction = connection.BeginTransaction(transactionIsolationLevel);
        T result;

        try
        {
          result = func(connection, transaction);
          transaction.Commit();
        }
        catch
        {
          if (transaction.Connection != null)
          {
            // Don't rely on implicit rollback when calling the Dispose
            // method, because some implementations may throw the
            // NullReferenceException, although it's prohibited to throw
            // any exception from a Dispose method, according to the
            // .NET Framework Design Guidelines:
            // https://github.com/dotnet/efcore/issues/12864
            // https://github.com/HangfireIO/Hangfire/issues/1494
            transaction.Rollback();
          }

          throw;
        }

        return result;
      });
    }

    internal void UseTransaction(DbConnection dedicatedConnection, Action<DbConnection, DbTransaction> action, Func<TransactionScope> transactionScopeFactory)
    {
      UseTransaction(dedicatedConnection, (connection, transaction) => {
        action(connection, transaction);
        return true;
      }, transactionScopeFactory);
    }

    internal T UseTransaction<T>(DbConnection dedicatedConnection, Func<DbConnection, DbTransaction, T> func, Func<TransactionScope> transactionScopeFactory)
    {
      return UseConnection(dedicatedConnection, connection => {
        using TransactionScope transaction = transactionScopeFactory();
        connection.EnlistTransaction(Transaction.Current);

        T result = func(connection, null);

        // TransactionCompleted event is required here, because if this TransactionScope is enlisted within an ambient TransactionScope, the ambient TransactionScope controls when the TransactionScope completes.
        Transaction.Current.TransactionCompleted += Current_TransactionCompleted;
        transaction.Complete();

        return result;
      });
    }

    private static void Current_TransactionCompleted(object sender, TransactionEventArgs e)
    {
      if (e.Transaction.TransactionInformation.Status == TransactionStatus.Committed)
      {
        PostgreSqlJobQueue._newItemInQueueEvent.Set();
      }
    }

    private static TransactionScope CreateTransaction(IsolationLevel? isolationLevel)
    {
      return isolationLevel != null
        ? new TransactionScope(TransactionScopeOption.Required,
          new TransactionOptions { IsolationLevel = isolationLevel.Value })
        : new TransactionScope();
    }

    private static System.Data.IsolationLevel? ConvertIsolationLevel(IsolationLevel? isolationLevel)
    {
      return isolationLevel switch {
        IsolationLevel.Chaos => System.Data.IsolationLevel.Chaos,
        IsolationLevel.ReadCommitted => System.Data.IsolationLevel.ReadCommitted,
        IsolationLevel.ReadUncommitted => System.Data.IsolationLevel.ReadUncommitted,
        IsolationLevel.RepeatableRead => System.Data.IsolationLevel.RepeatableRead,
        IsolationLevel.Serializable => System.Data.IsolationLevel.Serializable,
        IsolationLevel.Snapshot => System.Data.IsolationLevel.Snapshot,
        IsolationLevel.Unspecified => System.Data.IsolationLevel.Unspecified,
        null => null,
        var _ => throw new ArgumentOutOfRangeException(nameof(isolationLevel), isolationLevel, null),
      };
    }

    internal void UseConnection(DbConnection dedicatedConnection, [InstantHandle] Action<DbConnection> action)
    {
      UseConnection(dedicatedConnection, connection => {
        action(connection);
        return true;
      });
    }

    internal T UseConnection<T>(DbConnection dedicatedConnection, Func<DbConnection, T> func)
    {
      DbConnection connection = null;

      try
      {
        connection = dedicatedConnection ?? CreateAndOpenConnection();
        return func(connection);
      }
      finally
      {
        if (dedicatedConnection == null)
        {
          ReleaseConnection(connection);
        }
      }
    }

    internal void ReleaseConnection(DbConnection connection)
    {
      if (connection != null && !IsExistingConnection(connection))
      {
        connection.Dispose();
      }
    }

    private bool IsExistingConnection(IDbConnection connection)
    {
      return connection != null && ReferenceEquals(connection, _existingConnection);
    }

    private void InitializeQueueProviders()
    {
      PostgreSqlJobQueueProvider defaultQueueProvider = new(this, Options);
      QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
    }

    private bool TryCreateConnectionStringBuilder(string connectionString, out NpgsqlConnectionStringBuilder builder)
    {
      try
      {
        builder = new NpgsqlConnectionStringBuilder(connectionString);
        return true;
      }
      catch (ArgumentException)
      {
        builder = null;
        return false;
      }
    }

    private NpgsqlConnectionStringBuilder SetupConnectionStringBuilderParameters(NpgsqlConnectionStringBuilder builder)
    {
      // The connection string must not be modified when transaction enlistment is enabled, otherwise it will cause
      // prepared transactions and probably fail when other statements (outside of hangfire) ran within the same
      // transaction. Also see #248.
      if (!Options.EnableTransactionScopeEnlistment)
      {
        builder.Enlist = false;
        SetTimezoneToUtcForNpgsqlCompatibility(builder);
      }

      return builder;
    }
  }
}
