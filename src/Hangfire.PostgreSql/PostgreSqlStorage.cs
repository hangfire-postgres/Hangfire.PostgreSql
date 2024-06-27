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
using Hangfire.PostgreSql.Factories;
using Hangfire.PostgreSql.Utils;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;
using IsolationLevel = System.Transactions.IsolationLevel;

namespace Hangfire.PostgreSql
{
  public class PostgreSqlStorage : JobStorage
  {
    private readonly IConnectionFactory _connectionFactory;
    
    private readonly Dictionary<string, bool> _features =
      new(StringComparer.OrdinalIgnoreCase)
      {
        { JobStorageFeatures.JobQueueProperty, true },
        { JobStorageFeatures.Connection.BatchedGetFirstByLowest, true }
      };

    [Obsolete("Will be removed in 2.0, please use the overload with IConnectionFactory argument")]
    public PostgreSqlStorage(string connectionString) : this(connectionString, new PostgreSqlStorageOptions()) { }

    [Obsolete("Will be removed in 2.0, please use the overload with IConnectionFactory argument")]
    public PostgreSqlStorage(string connectionString, PostgreSqlStorageOptions options) : this(connectionString, null, options) { }

    /// <summary>
    ///   Initializes PostgreSqlStorage from the provided PostgreSqlStorageOptions and either the provided connection string.
    /// </summary>
    /// <param name="connectionString">PostgreSQL connection string</param>
    /// <param name="connectionSetup">Optional setup action to apply to created connections</param>
    /// <param name="options">Storage options</param>
    /// <exception cref="ArgumentNullException"><paramref name="connectionString" /> argument is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options" /> argument is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="connectionString" /> argument not a valid PostgreSQL connection string config file.</exception>
    [Obsolete("Will be removed in 2.0, please use the overload with IConnectionFactory argument")]
    public PostgreSqlStorage(string connectionString, Action<NpgsqlConnection> connectionSetup, PostgreSqlStorageOptions options) : this(new NpgsqlConnectionFactory(connectionString, options, connectionSetup), options) { }

    [Obsolete("Will be removed in 2.0, please use the overload with IConnectionFactory argument")]
    public PostgreSqlStorage(NpgsqlConnection existingConnection) : this(existingConnection, new PostgreSqlStorageOptions()) { }

    /// <summary>
    ///   Initializes a new instance of the <see cref="PostgreSqlStorage" /> class with
    ///   explicit instance of the <see cref="NpgsqlConnection" /> class that will be used
    ///   to query the data.
    /// </summary>
    /// <param name="existingConnection">Existing connection</param>
    /// <param name="options">PostgreSqlStorageOptions</param>
    [Obsolete("Will be removed in 2.0, please use the overload with IConnectionFactory argument")]
    public PostgreSqlStorage(NpgsqlConnection existingConnection, PostgreSqlStorageOptions options) : this(new ExistingNpgsqlConnectionFactory(existingConnection, options), options) { }

    public PostgreSqlStorage(IConnectionFactory connectionFactory) : this(connectionFactory, new PostgreSqlStorageOptions()) { }

    public PostgreSqlStorage(IConnectionFactory connectionFactory, PostgreSqlStorageOptions options)
    {
      _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
      Options = options ?? throw new ArgumentNullException(nameof(options));

      if (options.PrepareSchemaIfNecessary)
      {
        NpgsqlConnection connection = CreateAndOpenConnection();
        try
        {
          PostgreSqlObjectsInstaller.Install(connection, options.SchemaName);
        }
        finally
        {
          if (connectionFactory is not ExistingNpgsqlConnectionFactory)
          {
            connection.Dispose();
          }
        }
      }

      InitializeQueueProviders();
      if (Options.UseSlidingInvisibilityTimeout)
      {
        HeartbeatProcess = new PostgreSqlHeartbeatProcess();
      }
    }

    public PersistentJobQueueProviderCollection QueueProviders { get; internal set; }

    internal PostgreSqlStorageOptions Options { get; }

    internal PostgreSqlHeartbeatProcess HeartbeatProcess { get; }

    public override IMonitoringApi GetMonitoringApi()
    {
      return new PostgreSqlMonitoringApi(this, QueueProviders);
    }

    public override IStorageConnection GetConnection()
    {
      return new PostgreSqlConnection(this);
    }

#pragma warning disable CS0618
    public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore CS0618
    {
      yield return new ExpirationManager(this);
      yield return new CountersAggregator(this, Options.CountersAggregateInterval);
      if (Options.UseSlidingInvisibilityTimeout)
      {
        // This is only used to update the sliding invisibility timeouts, so if not enabled then do not use it
        yield return HeartbeatProcess;
      }
    }

    public override void WriteOptionsToLog(ILog logger)
    {
      logger.Info("Using the following options for PostgreSQL job storage:");
      logger.InfoFormat("    Queue poll interval: {0}.", Options.QueuePollInterval);
      logger.InfoFormat("    Invisibility timeout: {0}.", Options.InvisibilityTimeout);
      logger.InfoFormat("    Use sliding invisibility timeout: {0}.", Options.UseSlidingInvisibilityTimeout);
    }

    public override string ToString()
    {
      const string canNotParseMessage = "<Connection string can not be parsed>";

      try
      {
        StringBuilder builder = new();

        if (_connectionFactory is NpgsqlInstanceConnectionFactoryBase connectionFactory)
        {
          NpgsqlConnectionStringBuilder connectionStringBuilder = connectionFactory.ConnectionString;
          builder.Append("Host: ");
          builder.Append(connectionStringBuilder.Host);
          builder.Append(", DB: ");
          builder.Append(connectionStringBuilder.Database);
          builder.Append(", ");
        }

        builder.Append("Schema: ");
        builder.Append(Options.SchemaName);

        return builder.Length != 0 ? $"PostgreSQL Server: {builder}" : canNotParseMessage;
      }
      catch (Exception)
      {
        return canNotParseMessage;
      }
    }

    internal NpgsqlConnection CreateAndOpenConnection()
    {
      NpgsqlConnection connection = _connectionFactory.GetOrCreateConnection();

      try
      {
        if (connection.State == ConnectionState.Closed)
        {
          connection.Open();
        }

        if (Options.EnableLongPolling && !connection.SupportsNotifications())
        {
          throw new InvalidOperationException("Long polling is supported only with PostgreSQL version 11 or higher.");
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
        T result = UseConnection(dedicatedConnection, connection => {

          using TransactionScope transaction = CreateTransactionScope(isolationLevel);
          connection.EnlistTransaction(Transaction.Current);
          T result = func(connection, null);
          transaction.Complete();
          return result;

        });

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

        transaction.Complete();

        return result;
      });
    }

    internal TransactionScope CreateTransactionScope(IsolationLevel? isolationLevel, TimeSpan? timeout = null)
    {
      return TransactionHelpers.CreateTransactionScope(isolationLevel, Options.EnableTransactionScopeEnlistment, timeout);
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
      return connection != null && _connectionFactory is ExistingNpgsqlConnectionFactory && ReferenceEquals(connection, _connectionFactory.GetOrCreateConnection());
    }

    private void InitializeQueueProviders()
    {
      PostgreSqlJobQueueProvider defaultQueueProvider = new(this, Options);
      QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
    }

    public override bool HasFeature(string featureId)
    {
      if (featureId == null)
      {
        throw new ArgumentNullException(nameof(featureId));
      }

      return _features.TryGetValue(featureId, out bool isSupported) 
        ? isSupported
        : base.HasFeature(featureId);
    }
  }
}
