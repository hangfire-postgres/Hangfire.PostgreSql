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
using System.Globalization;
using Hangfire.PostgreSql.Properties;
using Hangfire.PostgreSql.Utils;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql;

public interface IPersistentJobQueue
{
  IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);
  void Enqueue(IDbConnection connection, string queue, string jobId);
}

public class PostgreSqlJobQueue : IPersistentJobQueue
{
  private const string JobNotificationChannel = "new_job";

  internal static readonly AutoResetEventRegistry _queueEventRegistry = new();
  private readonly PostgreSqlStorageContext _context;

  internal PostgreSqlJobQueue(PostgreSqlStorageContext context)
  {
    _context = context ?? throw new ArgumentNullException(nameof(context));
    SignalDequeue = new AutoResetEvent(false);
    JobQueueNotification = new AutoResetEvent(false);
  }

  private AutoResetEvent SignalDequeue { get; }
  private AutoResetEvent JobQueueNotification { get; }

  [NotNull]
  public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
  {
    Task? listenTask = null;
    CancellationTokenSource? cancelListenSource = null;

    if (_context.Options.EnableLongPolling)
    {
      cancelListenSource = new CancellationTokenSource();
      listenTask = ListenForNotificationsAsync(cancelListenSource.Token);
    }

    try
    {
      return _context.Options.UseNativeDatabaseTransactions
        ? Dequeue_Transaction(queues, cancellationToken)
        : Dequeue_UpdateCount(queues, cancellationToken);
    }
    finally
    {
      try
      {
        cancelListenSource?.Cancel();
        listenTask?.Wait();
      }
      catch (AggregateException ex)
      {
        if (ex.InnerException is not TaskCanceledException)
        {
          throw;
        }

        // Otherwise do nothing, cancel exception is expected.
      }
      finally
      {
        cancelListenSource?.Dispose();
      }
    }
  }

  public void Enqueue(IDbConnection connection, string queue, string jobId)
  {
    string query = _context.QueryProvider.GetQuery("INSERT INTO hangfire.jobqueue (jobid, queue) VALUES ($1, $2)");
    connection.Process(query).WithParameters(jobId.ParseJobId(), queue).Execute();

    if (_context.Options.EnableLongPolling)
    {
      connection.Process($"NOTIFY {JobNotificationChannel}").Execute();
    }
  }

  /// <summary>
  ///   Signal the waiting Thread to lookup a new Job
  /// </summary>
  public void FetchNextJob()
  {
    SignalDequeue.Set();
  }


  [NotNull]
  internal IFetchedJob Dequeue_Transaction(string[] queues, CancellationToken cancellationToken)
  {
    if (queues == null)
    {
      throw new ArgumentNullException(nameof(queues));
    }

    if (queues.Length == 0)
    {
      throw new ArgumentException("Queue array must be non-empty.", nameof(queues));
    }

    FetchedJob? fetchedJob;

    string query = _context.QueryProvider.GetQuery(
      """
      UPDATE hangfire.jobqueue
      SET fetchedat = NOW()
      WHERE id = (
        SELECT id
        FROM hangfire.jobqueue
        WHERE queue = ANY ($1) AND (fetchedat IS NULL OR fetchedat < NOW() - $2)
        ORDER BY fetchedat NULLS FIRST, queue, jobid
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      )
      RETURNING id, jobid, queue, fetchedat
      """);

    WaitHandle[] nextFetchIterationWaitHandles = new[] {
      cancellationToken.WaitHandle,
      SignalDequeue,
      JobQueueNotification,
    }.Concat(_queueEventRegistry.GetWaitHandles(queues)).ToArray();

    do
    {
      cancellationToken.ThrowIfCancellationRequested();

      Utils.Utils.TryExecute(() => {
          NpgsqlConnection connection = _context.ConnectionManager.CreateAndOpenConnection();

          try
          {
            using NpgsqlTransaction trx = connection.BeginTransaction(IsolationLevel.ReadCommitted);
            FetchedJob? jobToFetch = connection.Process(query)
              .WithParameters(queues.ToList(), _context.Options.InvisibilityTimeout)
              .Select(reader => new FetchedJob {
                Id = reader.GetInt64(0),
                JobId = reader.GetInt64(1),
                Queue = reader.GetString(2),
                FetchedAt = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
              })
              .SingleOrDefault();

            trx.Commit();

            return jobToFetch;
          }
          catch (InvalidOperationException)
          {
            // thrown by .SingleOrDefault(): stop the exception propagation if the fetched job was concurrently fetched by another worker
          }
          finally
          {
            _context.ConnectionManager.ReleaseConnection(connection);
          }

          return null;
        },
        out fetchedJob,
        ex => ex is PostgresException { SqlState: PostgresErrorCodes.SerializationFailure });

      if (fetchedJob == null)
      {
        WaitHandle.WaitAny(nextFetchIterationWaitHandles, _context.Options.QueuePollInterval);
      }
    }
    while (fetchedJob == null);

    return new PostgreSqlFetchedJob(_context,
      fetchedJob.Id,
      fetchedJob.JobId.ToString(CultureInfo.InvariantCulture),
      fetchedJob.Queue,
      fetchedJob.FetchedAt);
  }

  [NotNull]
  internal IFetchedJob Dequeue_UpdateCount(string[] queues, CancellationToken cancellationToken)
  {
    if (queues == null)
    {
      throw new ArgumentNullException(nameof(queues));
    }

    if (queues.Length == 0)
    {
      throw new ArgumentException("Queue array must be non-empty.", nameof(queues));
    }

    FetchedJob? markJobAsFetched = null;

    string jobToFetchQuery = _context.QueryProvider.GetQuery(
      """
      SELECT id, jobid, queue, fetchedat, updatecount
      FROM hangfire.jobqueue 
      WHERE queue = ANY ($1) AND (fetchedat IS NULL OR fetchedat < NOW() - $2)
      ORDER BY fetchedat NULLS FIRST, queue, jobid
      LIMIT 1
      """);

    string markJobAsFetchedQuery = _context.QueryProvider.GetQuery(
      """
      UPDATE hangfire.jobqueue 
      SET fetchedat = NOW(), updatecount = (updatecount + 1) % 2000000000
      WHERE id = $1 AND updatecount = $2
      RETURNING id, jobid, queue, fetchedat
      """);

    do
    {
      cancellationToken.ThrowIfCancellationRequested();

      FetchedJob? jobToFetch = _context.ConnectionManager.UseConnection(null, connection => connection
        .Process(jobToFetchQuery)
        .WithParameters(queues.ToList(), _context.Options.InvisibilityTimeout)
        .Select(reader => new FetchedJob {
          Id = reader.GetInt64(0),
          JobId = reader.GetInt64(1),
          Queue = reader.GetString(2),
          FetchedAt = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
          UpdateCount = reader.GetInt32(4),
        })
        .SingleOrDefault());

      if (jobToFetch == null)
      {
        WaitHandle.WaitAny([
            cancellationToken.WaitHandle,
            SignalDequeue,
            JobQueueNotification,
          ],
          _context.Options.QueuePollInterval);

        cancellationToken.ThrowIfCancellationRequested();
      }
      else
      {
        markJobAsFetched = _context.ConnectionManager.UseConnection(null, connection => connection
          .Process(markJobAsFetchedQuery)
          .WithParameters(jobToFetch.Id, jobToFetch.UpdateCount)
          .Select(reader => new FetchedJob {
            Id = reader.GetInt64(0),
            JobId = reader.GetInt64(1),
            Queue = reader.GetString(2),
            FetchedAt = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
          })
          .SingleOrDefault());
      }
    }
    while (markJobAsFetched == null);

    return new PostgreSqlFetchedJob(_context,
      markJobAsFetched.Id,
      markJobAsFetched.JobId.ToString(CultureInfo.InvariantCulture),
      markJobAsFetched.Queue,
      markJobAsFetched.FetchedAt);
  }

  private Task ListenForNotificationsAsync(CancellationToken cancellationToken)
  {
    NpgsqlConnection connection = _context.ConnectionManager.CreateAndOpenConnection();

    try
    {
      if (!connection.SupportsNotifications())
      {
        return Task.CompletedTask;
      }

      // CreateAnOpenConnection can return the same connection over and over if an existing connection
      //  is passed in the constructor of PostgreSqlStorage. We must use a separate dedicated
      //  connection to listen for notifications.
      NpgsqlConnection clonedConnection = connection.CloneWith(connection.ConnectionString);

      return Task.Run(async () => {
        try
        {
          if (clonedConnection.State != ConnectionState.Open)
          {
            await clonedConnection.OpenAsync(cancellationToken); // Open so that SqlQueryProcessor doesn't auto-close.
          }

          while (!cancellationToken.IsCancellationRequested)
          {
            await clonedConnection.Process($"LISTEN {JobNotificationChannel}").ExecuteAsync();
            await clonedConnection.WaitAsync(cancellationToken);
            JobQueueNotification.Set();
          }
        }
        catch (TaskCanceledException)
        {
          // Do nothing, cancellation requested so just end.
        }
        finally
        {
          _context.ConnectionManager.ReleaseConnection(clonedConnection);
        }

      }, cancellationToken);

    }
    finally
    {
      _context.ConnectionManager.ReleaseConnection(connection);
    }
  }

  [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
  private class FetchedJob
  {
    public long Id { get; set; }
    public long JobId { get; set; }
    public string Queue { get; set; } = null!;
    public DateTime? FetchedAt { get; set; }
    public int UpdateCount { get; set; }
  }
}