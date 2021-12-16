﻿// This file is part of Hangfire.PostgreSql.
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
using Hangfire.PostgreSql.Utils;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;
using IsolationLevel = System.Transactions.IsolationLevel;

namespace Hangfire.PostgreSql
{
  public class PostgreSqlStorage : JobStorage
  {
    private readonly Action<NpgsqlConnection> _connectionSetup;
    private readonly string _connectionString;
    private readonly NpgsqlConnection _existingConnection;

    public PostgreSqlStorage(string nameOrConnectionString)
      : this(nameOrConnectionString, new PostgreSqlStorageOptions()) { }

    public PostgreSqlStorage(string nameOrConnectionString, PostgreSqlStorageOptions options)
      : this(nameOrConnectionString, null, options) { }

    /// <summary>
    ///   Initializes PostgreSqlStorage from the provided PostgreSqlStorageOptions and either the provided connection
    ///   string or the connection string with provided name pulled from the application config file.
    /// </summary>
    /// <param name="nameOrConnectionString">
    ///   Either a SQL Server connection string or the name of
    ///   a SQL Server connection string located in the connectionStrings node in the application config
    /// </param>
    /// <param name="connectionSetup">Optional setup action to apply to created connections</param>
    /// <param name="options"></param>
    /// <exception cref="ArgumentNullException"><paramref name="nameOrConnectionString" /> argument is null.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="options" /> argument is null.</exception>
    /// <exception cref="ArgumentException">
    ///   <paramref name="nameOrConnectionString" /> argument is neither
    ///   a valid SQL Server connection string nor the name of a connection string in the application
    ///   config file.
    /// </exception>
    public PostgreSqlStorage(string nameOrConnectionString,
      Action<NpgsqlConnection> connectionSetup,
      PostgreSqlStorageOptions options)
    {
      if (nameOrConnectionString == null) throw new ArgumentNullException(nameof(nameOrConnectionString));

      Options = options ?? throw new ArgumentNullException(nameof(options));

      if (IsConnectionString(nameOrConnectionString))
      {
        _connectionString = nameOrConnectionString;
      }
      else
      {
        throw new ArgumentException($"Could not find connection string with name '{nameOrConnectionString}' in application config file");
      }

      _connectionSetup = connectionSetup;

      if (options.PrepareSchemaIfNecessary)
      {
        DbQueryHelper.IsUpperCase = options.IsUpperCaseDatabaseObject;
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
      if (existingConnection == null) throw new ArgumentNullException(nameof(existingConnection));
      if (options == null) throw new ArgumentNullException(nameof(options));
      NpgsqlConnectionStringBuilder connectionStringBuilder = new NpgsqlConnectionStringBuilder(existingConnection.ConnectionString);

      if (!options.EnableTransactionScopeEnlistment)
      {
        if (connectionStringBuilder.Enlist)
        {
          throw new ArgumentException(
            $"TransactionScope enlistment must be enabled by setting {nameof(PostgreSqlStorageOptions)}.{nameof(options.EnableTransactionScopeEnlistment)} to `true`.");
        }
      }

      _existingConnection = existingConnection;
      Options = options;

      InitializeQueueProviders();
    }

    public PostgreSqlStorage(NpgsqlConnection existingConnection)
    {
      if (existingConnection == null) throw new ArgumentNullException(nameof(existingConnection));
      NpgsqlConnectionStringBuilder connectionStringBuilder = new NpgsqlConnectionStringBuilder(existingConnection.ConnectionString);
      if (connectionStringBuilder.Enlist)
      {
        throw new ArgumentException(
          $"TransactionScope enlistment must be enabled by setting {nameof(PostgreSqlStorageOptions)}.{nameof(PostgreSqlStorageOptions.EnableTransactionScopeEnlistment)} to `true`.");
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
        NpgsqlConnectionStringBuilder connectionStringBuilder = new NpgsqlConnectionStringBuilder(_connectionString);
        StringBuilder builder = new StringBuilder();

        builder.Append("Host: ");
        builder.Append(connectionStringBuilder.Host);
        builder.Append(", DB: ");
        builder.Append(connectionStringBuilder.Database);
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

    internal NpgsqlConnection CreateAndOpenConnection()
    {
      NpgsqlConnection connection = _existingConnection;
      if (connection == null)
      {
        NpgsqlConnectionStringBuilder connectionStringBuilder = new NpgsqlConnectionStringBuilder(_connectionString);
        if (!Options.EnableTransactionScopeEnlistment)
        {
          connectionStringBuilder.Enlist = false;
        }

        connection = new NpgsqlConnection(connectionStringBuilder.ToString());
        _connectionSetup?.Invoke(connection);
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
      isolationLevel = isolationLevel ?? IsolationLevel.ReadCommitted;

      if (IsRunningOnWindows())
      {
        using (TransactionScope transaction = CreateTransaction(isolationLevel))
        {
          T result = UseConnection(dedicatedConnection, connection => {
            connection.EnlistTransaction(Transaction.Current);
            return func(connection, null);
          });

          transaction.Complete();

          return result;
        }
      }

      return UseConnection(dedicatedConnection, connection => {
        System.Data.IsolationLevel transactionIsolationLevel = ConvertIsolationLevel(isolationLevel) ?? System.Data.IsolationLevel.ReadCommitted;
        using (DbTransaction transaction = connection.BeginTransaction(transactionIsolationLevel))
        {
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
        }
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
        using (TransactionScope transaction = transactionScopeFactory())
        {
          connection.EnlistTransaction(Transaction.Current);

          T result = func(connection, null);

          // TransactionCompleted event is required here, because if this TransactionScope is enlisted within an ambient TransactionScope, the ambient TransactionScope controls when the TransactionScope completes.
          Transaction.Current.TransactionCompleted += Current_TransactionCompleted;
          transaction.Complete();

          return result;
        }
      });
    }

    private static void Current_TransactionCompleted(object sender, TransactionEventArgs e)
    {
      if (e.Transaction.TransactionInformation.Status == TransactionStatus.Committed)
      {
        PostgreSqlJobQueue._newItemInQueueEvent.Set();
      }
    }

    private static bool IsRunningOnWindows()
    {
#if !NETSTANDARD1_3
      return Environment.OSVersion.Platform == PlatformID.Win32NT;
#else
            return System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows);
#endif
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
      switch (isolationLevel)
      {
        case IsolationLevel.Chaos:
          return System.Data.IsolationLevel.Chaos;
        case IsolationLevel.ReadCommitted:
          return System.Data.IsolationLevel.ReadCommitted;
        case IsolationLevel.ReadUncommitted:
          return System.Data.IsolationLevel.ReadUncommitted;
        case IsolationLevel.RepeatableRead:
          return System.Data.IsolationLevel.RepeatableRead;
        case IsolationLevel.Serializable:
          return System.Data.IsolationLevel.Serializable;
        case IsolationLevel.Snapshot:
          return System.Data.IsolationLevel.Snapshot;
        case IsolationLevel.Unspecified:
          return System.Data.IsolationLevel.Unspecified;
        case null:
          return null;
        default:
          throw new ArgumentOutOfRangeException(nameof(isolationLevel), isolationLevel, null);
      }
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
        connection.Dispose();
    }

    private bool IsExistingConnection(IDbConnection connection)
    {
      return connection != null && ReferenceEquals(connection, _existingConnection);
    }

    private void InitializeQueueProviders()
    {
      PostgreSqlJobQueueProvider defaultQueueProvider = new PostgreSqlJobQueueProvider(this, Options);
      QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
    }

    private bool IsConnectionString(string nameOrConnectionString)
    {
      return nameOrConnectionString.Contains(";");
    }
  }
}
