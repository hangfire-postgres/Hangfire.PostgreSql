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
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Hangfire.PostgreSql.Properties;
using Hangfire.PostgreSql.Utils;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
  public class PostgreSqlJobQueue : IPersistentJobQueue
  {
    private const string JobNotificationChannel = "new_job";

    internal static readonly AutoResetEventRegistry _queueEventRegistry = new();
    private readonly PostgreSqlStorage _storage;

    public PostgreSqlJobQueue(PostgreSqlStorage storage)
    {
      _storage = storage ?? throw new ArgumentNullException(nameof(storage));
      SignalDequeue = new AutoResetEvent(false);
      JobQueueNotification = new AutoResetEvent(false);
    }

    private AutoResetEvent SignalDequeue { get; }
    private AutoResetEvent JobQueueNotification { get; }

    [NotNull]
    public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
    {
      Task listenTask = null;
      CancellationTokenSource cancelListenSource = null;

      if (_storage.Options.EnableLongPolling)
      {
        cancelListenSource = new CancellationTokenSource();
        listenTask = ListenForNotificationsAsync(cancelListenSource.Token);
      }

      try
      {
        return _storage.Options.UseNativeDatabaseTransactions
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
      string enqueueJobSql = $@"
        INSERT INTO ""{_storage.Options.SchemaName}"".""jobqueue"" (""jobid"", ""queue"") 
        VALUES (@JobId, @Queue);
      ";

      connection.Execute(enqueueJobSql,
        new { JobId = Convert.ToInt64(jobId, CultureInfo.InvariantCulture), Queue = queue });

      if (_storage.Options.EnableLongPolling)
      {
        connection.Execute($"NOTIFY {JobNotificationChannel}");
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

      long timeoutSeconds = (long)_storage.Options.InvisibilityTimeout.Negate().TotalSeconds;
      FetchedJob fetchedJob;

      string fetchJobSql = $@"
        UPDATE ""{_storage.Options.SchemaName}"".""jobqueue"" 
        SET ""fetchedat"" = NOW()
        WHERE ""id"" = (
          SELECT ""id"" 
          FROM ""{_storage.Options.SchemaName}"".""jobqueue"" 
          WHERE ""queue"" = ANY (@Queues)
          AND (""fetchedat"" IS NULL OR ""fetchedat"" < NOW() + INTERVAL '{timeoutSeconds.ToString(CultureInfo.InvariantCulture)} SECONDS')
          ORDER BY ""fetchedat"" NULLS FIRST, ""queue"", ""jobid""
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        )
        RETURNING ""id"" AS ""Id"", ""jobid"" AS ""JobId"", ""queue"" AS ""Queue"", ""fetchedat"" AS ""FetchedAt"";
      ";

      WaitHandle[] nextFetchIterationWaitHandles = new[] {
        cancellationToken.WaitHandle,
        SignalDequeue,
        JobQueueNotification,
      }.Concat(_queueEventRegistry.GetWaitHandles(queues)).ToArray();

      do
      {
        cancellationToken.ThrowIfCancellationRequested();

        Utils.Utils.TryExecute(() => {
          NpgsqlConnection connection = _storage.CreateAndOpenConnection();

          try
          {
            using NpgsqlTransaction trx = connection.BeginTransaction(IsolationLevel.ReadCommitted);
            FetchedJob jobToFetch = connection.Query<FetchedJob>(fetchJobSql,
                new { Queues = queues.ToList() }, trx)
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
            _storage.ReleaseConnection(connection);
          }

          return null;
        },
          out fetchedJob,
          ex => ex is PostgresException { SqlState: PostgresErrorCodes.SerializationFailure });

        if (fetchedJob == null)
        {
          WaitHandle.WaitAny(nextFetchIterationWaitHandles, _storage.Options.QueuePollInterval);
        }
      }
      while (fetchedJob == null);

      return new PostgreSqlFetchedJob(_storage,
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

      long timeoutSeconds = (long)_storage.Options.InvisibilityTimeout.Negate().TotalSeconds;
      FetchedJob markJobAsFetched = null;

      string jobToFetchSql = $@"
        SELECT ""id"" AS ""Id"", ""jobid"" AS ""JobId"", ""queue"" AS ""Queue"", ""fetchedat"" AS ""FetchedAt"", ""updatecount"" AS ""UpdateCount""
        FROM ""{_storage.Options.SchemaName}"".""jobqueue"" 
        WHERE ""queue"" = ANY (@Queues)
        AND (""fetchedat"" IS NULL OR ""fetchedat"" < NOW() + INTERVAL '{timeoutSeconds.ToString(CultureInfo.InvariantCulture)} SECONDS')
        ORDER BY ""fetchedat"" NULLS FIRST, ""queue"", ""jobid""
        LIMIT 1;
        ";

      string markJobAsFetchedSql = $@"
        UPDATE ""{_storage.Options.SchemaName}"".""jobqueue"" 
        SET ""fetchedat"" = NOW(), 
            ""updatecount"" = (""updatecount"" + 1) % 2000000000
        WHERE ""id"" = @Id 
        AND ""updatecount"" = @UpdateCount
        RETURNING ""id"" AS ""Id"", ""jobid"" AS ""JobId"", ""queue"" AS ""Queue"", ""fetchedat"" AS ""FetchedAt"";
      ";

      do
      {
        cancellationToken.ThrowIfCancellationRequested();

        FetchedJob jobToFetch = _storage.UseConnection(null, connection => connection.Query<FetchedJob>(jobToFetchSql,
            new { Queues = queues.ToList() })
          .SingleOrDefault());

        if (jobToFetch == null)
        {
          WaitHandle.WaitAny(new[] {
              cancellationToken.WaitHandle,
              SignalDequeue,
              JobQueueNotification,
            },
            _storage.Options.QueuePollInterval);

          cancellationToken.ThrowIfCancellationRequested();
        }
        else
        {
          markJobAsFetched = _storage.UseConnection(null, connection => connection.Query<FetchedJob>(markJobAsFetchedSql,
              jobToFetch)
            .SingleOrDefault());
        }
      }
      while (markJobAsFetched == null);

      return new PostgreSqlFetchedJob(_storage,
        markJobAsFetched.Id,
        markJobAsFetched.JobId.ToString(CultureInfo.InvariantCulture),
        markJobAsFetched.Queue,
        markJobAsFetched.FetchedAt);
    }

    private Task ListenForNotificationsAsync(CancellationToken cancellationToken)
    {
      NpgsqlConnection connection = _storage.CreateAndOpenConnection();

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
              await clonedConnection.OpenAsync(cancellationToken); // Open so that Dapper doesn't auto-close.
            }

            while (!cancellationToken.IsCancellationRequested)
            {
              await clonedConnection.ExecuteAsync($"LISTEN {JobNotificationChannel}");
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
            _storage.ReleaseConnection(clonedConnection);
          }

        }, cancellationToken);

      }
      finally
      {
        _storage.ReleaseConnection(connection);
      }
    }

    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    private class FetchedJob
    {
      public long Id { get; set; }
      public long JobId { get; set; }
      public string Queue { get; set; }
      public DateTime? FetchedAt { get; set; }
      public int UpdateCount { get; set; }
    }
  }
}
