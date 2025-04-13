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
using System.Data;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.PostgreSql;
#pragma warning disable CS0618
internal class ExpirationManager : IBackgroundProcess, IServerComponent
#pragma warning restore CS0618
{
  private const string DistributedLockKey = "locks:expirationmanager";

  private static readonly TimeSpan _defaultLockTimeout = TimeSpan.FromMinutes(5);
  private static readonly TimeSpan _delayBetweenPasses = TimeSpan.FromSeconds(1);
  private static readonly ILog _logger = LogProvider.GetLogger(typeof(ExpirationManager));

  private static readonly string[] _processedCounters = [
    "stats:succeeded",
    "stats:deleted",
  ];
  private static readonly string[] _processedTables = [
    "aggregatedcounter",
    "counter",
    "job",
    "list",
    "set",
    "hash",
  ];

  private readonly PostgreSqlStorageContext _context;
  private readonly TimeSpan _checkInterval;

  public ExpirationManager(PostgreSqlStorageContext context)
    : this(context ?? throw new ArgumentNullException(nameof(context)), context.Options.JobExpirationCheckInterval) { }

  public ExpirationManager(PostgreSqlStorageContext context, TimeSpan checkInterval)
  {
    _context = context ?? throw new ArgumentNullException(nameof(context));
    _checkInterval = checkInterval;
  }

  public void Execute(BackgroundProcessContext context)
  {
    Execute(context.StoppingToken);
  }

  public void Execute(CancellationToken cancellationToken)
  {
    foreach (string table in _processedTables)
    {
      _logger.DebugFormat("Removing outdated records from table '{0}'...", table);

      UseConnectionDistributedLock(connection => {
        int removedCount;
        do
        {
          using IDbTransaction transaction = connection.BeginTransaction();
          string query = _context.QueryProvider.GetQuery(schemaName =>
            $"""
            DELETE FROM {schemaName}.{table}
            WHERE id IN (
              SELECT id
              FROM {schemaName}.{table}
              WHERE expireat < NOW()
              LIMIT @Count
            )
            """);
          removedCount = connection.Execute(query, new { Count = _context.Options.DeleteExpiredBatchSize }, transaction);

          if (removedCount <= 0)
          {
            continue;
          }

          transaction.Commit();
          _logger.InfoFormat("Removed {0} outdated record(s) from '{1}' table.", removedCount, table);

          cancellationToken.WaitHandle.WaitOne(_delayBetweenPasses);
          cancellationToken.ThrowIfCancellationRequested();
        }
        while (removedCount != 0);
      });
    }

    AggregateCounters(cancellationToken);
    cancellationToken.WaitHandle.WaitOne(_checkInterval);
  }

  public override string ToString()
  {
    return "SQL Records Expiration Manager";
  }

  private void AggregateCounters(CancellationToken cancellationToken)
  {
    foreach (string processedCounter in _processedCounters)
    {
      AggregateCounter(processedCounter);
      cancellationToken.ThrowIfCancellationRequested();
    }
  }

  private void AggregateCounter(string counterName)
  {
    UseConnectionDistributedLock(connection => {
      using IDbTransaction transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
      string query = _context.QueryProvider.GetQuery(static schemaName =>
        $"""
        WITH counters AS (
          DELETE FROM {schemaName}.counter
          WHERE key = @Key
          AND expireat IS NULL
          RETURNING *
        )
        SELECT SUM(value) FROM counters
        """);

      long aggregatedValue = connection.ExecuteScalar<long>(query, new { Key = counterName }, transaction);
      transaction.Commit();

      if (aggregatedValue > 0)
      {
        string insertQuery = _context.QueryProvider.GetQuery(static schemaName => $"INSERT INTO {schemaName}.counter (key, value) VALUES (@Key, @Value)");
        connection.Execute(insertQuery, new { Key = counterName, Value = aggregatedValue });
      }
    });
  }

  private void UseConnectionDistributedLock(Action<IDbConnection> action)
  {
    try
    {
      _context.ConnectionManager.UseConnection(null, connection => {
        PostgreSqlDistributedLock.Acquire(connection, DistributedLockKey, _defaultLockTimeout, _context);

        try
        {
          action(connection);
        }
        finally
        {
          PostgreSqlDistributedLock.Release(connection, DistributedLockKey, _context);
        }
      });
    }
    catch (DistributedLockTimeoutException e) when (e.Resource == DistributedLockKey)
    {
      // DistributedLockTimeoutException here doesn't mean that outdated records weren't removed.
      // It just means another Hangfire server did this work.
      _logger.Log(LogLevel.Debug,
        () =>
          $"An exception was thrown during acquiring distributed lock on the {DistributedLockKey} resource within {_defaultLockTimeout.TotalSeconds} seconds. Outdated records were not removed. It will be retried in {_checkInterval.TotalSeconds} seconds.",
        e);
    }
  }
}