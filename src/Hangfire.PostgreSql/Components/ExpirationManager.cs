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

using System.Data;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.PostgreSql.Components;
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
        using IDbTransaction transaction = connection.BeginTransaction();
        do
        {
          string query = _context.QueryProvider.GetQuery(
            """
            DELETE FROM hangfire.{0}
            WHERE id IN (
              SELECT id
              FROM hangfire.{0}
              WHERE expireat < NOW()
              LIMIT $1
            )
            """, table);
          removedCount = connection.Process(query, transaction).WithParameter(_context.Options.DeleteExpiredBatchSize).Execute();
          if (removedCount <= 0)
          {
            continue;
          }

          _logger.InfoFormat("Removed {0} outdated record(s) from '{1}' table.", removedCount, table);

          cancellationToken.WaitHandle.WaitOne(_delayBetweenPasses);
          cancellationToken.ThrowIfCancellationRequested();
        }
        while (removedCount != 0);

        transaction.Commit();
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
      string query = _context.QueryProvider.GetQuery(
        // language=sql
        """
        WITH counters AS (
          DELETE FROM hangfire.counter
          WHERE key = $1 AND expireat IS NULL
          RETURNING *
        )
        SELECT SUM(value) FROM counters
        """);

      long aggregatedValue = connection.Process(query, transaction)
        .WithParameter(counterName)
        .Select(reader => reader.IsDBNull(0) ? 0 : reader.GetInt64(0))
        .Single();

      if (aggregatedValue > 0)
      {
        string insertQuery = _context.QueryProvider.GetQuery("INSERT INTO hangfire.counter (key, value) VALUES ($1, $2)");
        connection.Process(insertQuery, transaction).WithParameters(counterName, aggregatedValue).Execute();
      }

      transaction.Commit();
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