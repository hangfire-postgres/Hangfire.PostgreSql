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
using System.Transactions;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;
using IsolationLevel = System.Transactions.IsolationLevel;

namespace Hangfire.PostgreSql;

public class PostgreSqlStorage : JobStorage
{
  private readonly Dictionary<string, bool> _features =
    new(StringComparer.OrdinalIgnoreCase)
    {
      { JobStorageFeatures.JobQueueProperty, true },
      { JobStorageFeatures.Connection.BatchedGetFirstByLowest, true },
    };

  public PostgreSqlStorage(IConnectionFactory connectionFactory) : this(connectionFactory, new PostgreSqlStorageOptions()) { }

  public PostgreSqlStorage(IConnectionFactory connectionFactory, PostgreSqlStorageOptions options, PersistentJobQueueProviderCollection? persistentJobQueueProviderCollection = null)
  {
    Context = new PostgreSqlStorageContext(options, new PostgreSqlDbConnectionManager(connectionFactory, options));
    PostgreSqlObjectsInstaller.Install(Context);

    if (persistentJobQueueProviderCollection == null)
    {
      PostgreSqlJobQueueProvider defaultQueueProvider = new(Context);
      persistentJobQueueProviderCollection = new PersistentJobQueueProviderCollection(defaultQueueProvider);
    }
    Context.QueueProviders = persistentJobQueueProviderCollection;
  }
  
  internal PostgreSqlStorageContext Context { get; }

  public override IMonitoringApi GetMonitoringApi()
  {
    return new PostgreSqlMonitoringApi(Context);
  }

  public override IStorageConnection GetConnection()
  {
    return new PostgreSqlConnection(Context);
  }

#pragma warning disable CS0618
  public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore CS0618
  {
    yield return new ExpirationManager(Context);
    yield return new CountersAggregator(Context);
    if (Context.HeartbeatProcess != null)
    {
      // This is only used to update the sliding invisibility timeouts, so if not enabled then do not use it
      yield return Context.HeartbeatProcess;
    }
  }

  public override void WriteOptionsToLog(ILog logger)
  {
    PostgreSqlStorageOptions options = Context.Options;
    logger.Info("Using the following options for PostgreSQL job storage:");
    logger.InfoFormat("    Queue poll interval: {0}.", options.QueuePollInterval);
    logger.InfoFormat("    Invisibility timeout: {0}.", options.InvisibilityTimeout);
    logger.InfoFormat("    Use sliding invisibility timeout: {0}.", options.UseSlidingInvisibilityTimeout);
  }

  public override string ToString()
  {
    return Context.ConnectionManager.ToString();
  }

  internal NpgsqlConnection CreateAndOpenConnection()
  {
    return Context.ConnectionManager.CreateAndOpenConnection();
  }

  internal void UseTransaction(DbConnection? dedicatedConnection,
    [InstantHandle] Action<DbConnection, IDbTransaction?> action,
    IsolationLevel? isolationLevel = null)
  {
    Context.ConnectionManager.UseTransaction(dedicatedConnection, action, isolationLevel);
  }

  internal T UseTransaction<T>(DbConnection? dedicatedConnection,
    [InstantHandle] Func<DbConnection, IDbTransaction?, T> func,
    IsolationLevel? isolationLevel = null)
  {
    return Context.ConnectionManager.UseTransaction(dedicatedConnection, func, isolationLevel);
  }

  internal void UseTransaction(DbConnection? dedicatedConnection, Action<DbConnection, DbTransaction?> action, Func<TransactionScope> transactionScopeFactory)
  {
    Context.ConnectionManager.UseTransaction(dedicatedConnection, action, transactionScopeFactory);
  }

  internal T UseTransaction<T>(DbConnection? dedicatedConnection, Func<DbConnection, DbTransaction?, T> func, Func<TransactionScope> transactionScopeFactory)
  {
    return Context.ConnectionManager.UseTransaction(dedicatedConnection, func, transactionScopeFactory);
  }

  internal TransactionScope CreateTransactionScope(IsolationLevel? isolationLevel, TimeSpan? timeout = null)
  {
    return Context.ConnectionManager.CreateTransactionScope(isolationLevel, timeout);
  }

  internal void UseConnection(DbConnection? dedicatedConnection, [InstantHandle] Action<DbConnection> action)
  {
    Context.ConnectionManager.UseConnection(dedicatedConnection, action);
  }

  internal T UseConnection<T>(DbConnection? dedicatedConnection, Func<DbConnection, T> func)
  {
    return Context.ConnectionManager.UseConnection(dedicatedConnection, func);
  }

  internal void ReleaseConnection(DbConnection? connection)
  {
    Context.ConnectionManager.ReleaseConnection(connection);
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