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
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Extensions;
using Hangfire.PostgreSql.Properties;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
  public class PostgreSqlJobQueue : IPersistentJobQueue
  {
    internal static readonly AutoResetEvent _newItemInQueueEvent = new AutoResetEvent(true);
    private readonly PostgreSqlStorage _storage;

    public PostgreSqlJobQueue(PostgreSqlStorage storage)
    {
      _storage = storage ?? throw new ArgumentNullException(nameof(storage));
      SignalDequeue = new AutoResetEvent(false);
    }

    private AutoResetEvent SignalDequeue { get; }

    [NotNull]
    public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
    {
      if (_storage.Options.UseNativeDatabaseTransactions)
        return Dequeue_Transaction(queues, cancellationToken);

      return Dequeue_UpdateCount(queues, cancellationToken);
    }

    public void Enqueue(IDbConnection connection, string queue, string jobId)
    {
      string enqueueJobSql = $@"
        INSERT INTO ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"jobqueue".GetProperDbObjectName()}"" 
        (""{"jobid".GetProperDbObjectName()}"", ""{"queue".GetProperDbObjectName()}"") 
        VALUES (@JobId, @Queue);
      ";

      connection.Execute(enqueueJobSql,
        new { JobId = Convert.ToInt64(jobId, CultureInfo.InvariantCulture), Queue = queue});
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
      if (queues == null) throw new ArgumentNullException(nameof(queues));
      if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

      long timeoutSeconds = (long)_storage.Options.InvisibilityTimeout.Negate().TotalSeconds;
      FetchedJob fetchedJob;

      string fetchJobSqlTemplate = $@"
        UPDATE ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"jobqueue".GetProperDbObjectName()}"" 
        SET ""{"fetchedat".GetProperDbObjectName()}"" = NOW() AT TIME ZONE 'UTC'
        WHERE ""{"id".GetProperDbObjectName()}"" = (
          SELECT ""{"id".GetProperDbObjectName()}"" 
          FROM ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"jobqueue".GetProperDbObjectName()}"" 
          WHERE ""{"queue".GetProperDbObjectName()}"" = ANY (@Queues)
          AND ""{"fetchedat".GetProperDbObjectName()}"" {{0}}
          ORDER BY ""{"queue".GetProperDbObjectName()}"", ""{"fetchedat".GetProperDbObjectName()}"", ""{"jobid".GetProperDbObjectName()}""
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        )
        RETURNING ""{"id".GetProperDbObjectName()}"" AS ""Id"", 
                  ""{"jobid".GetProperDbObjectName()}"" AS ""JobId"", 
                  ""{"queue".GetProperDbObjectName()}"" AS ""Queue"", 
                  ""{"fetchedat".GetProperDbObjectName()}"" AS ""FetchedAt"";
      ";

      string[] fetchConditions = {
        "IS NULL",
        $"< NOW() AT TIME ZONE 'UTC' + INTERVAL '{timeoutSeconds.ToString(CultureInfo.InvariantCulture)} SECONDS'",
      };
      
      int fetchConditionsIndex = 0;
      do
      {
        cancellationToken.ThrowIfCancellationRequested();

        string fetchJobSql = string.Format(fetchJobSqlTemplate, fetchConditions[fetchConditionsIndex]);

        Utils.Utils.TryExecute(() => {
            NpgsqlConnection connection = _storage.CreateAndOpenConnection();

            try
            {
              using (NpgsqlTransaction trx = connection.BeginTransaction(IsolationLevel.ReadCommitted))
              {
                FetchedJob jobToFetch = connection.Query<FetchedJob>(fetchJobSql,
                    new { Queues = queues.ToList() }, trx)
                  .SingleOrDefault();

                trx.Commit();

                return jobToFetch;
              }
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
          ex => ex is PostgresException postgresException && postgresException.SqlState.Equals(PostgresErrorCodes.SerializationFailure));

        if (fetchedJob == null && fetchConditionsIndex == fetchConditions.Length - 1)
        {
          WaitHandle.WaitAny(new[] {
              cancellationToken.WaitHandle,
              _newItemInQueueEvent,
              SignalDequeue,
            },
            _storage.Options.QueuePollInterval);

          cancellationToken.ThrowIfCancellationRequested();
        }
        fetchConditionsIndex++;
        fetchConditionsIndex %= fetchConditions.Length;
      } 
      while (fetchedJob == null);

      return new PostgreSqlFetchedJob(_storage,
        fetchedJob.Id,
        fetchedJob.JobId.ToString(CultureInfo.InvariantCulture),
        fetchedJob.Queue);
    }


    [NotNull]
    internal IFetchedJob Dequeue_UpdateCount(string[] queues, CancellationToken cancellationToken)
    {
      if (queues == null) throw new ArgumentNullException("queues");
      if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", "queues");


      long timeoutSeconds = (long)_storage.Options.InvisibilityTimeout.Negate().TotalSeconds;
      FetchedJob markJobAsFetched = null;


      string jobToFetchSqlTemplate = $@"
        SELECT  ""{"id".GetProperDbObjectName()}"" AS ""Id"", 
                ""{"jobid".GetProperDbObjectName()}"" AS ""JobId"",
                ""{"queue".GetProperDbObjectName()}"" AS ""Queue"",
                ""{"fetchedat".GetProperDbObjectName()}"" AS ""FetchedAt"",
                ""{"updatecount".GetProperDbObjectName()}"" AS ""UpdateCount""
        FROM ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"jobqueue".GetProperDbObjectName()}"" 
        WHERE ""{"queue".GetProperDbObjectName()}"" = ANY (@Queues)
        AND ""{"fetchedat".GetProperDbObjectName()}"" {{0}} 
        ORDER BY ""{"queue".GetProperDbObjectName()}"", ""{"fetchedat".GetProperDbObjectName()}"", ""{"jobid".GetProperDbObjectName()}"" 
        LIMIT 1;
        ";

      string markJobAsFetchedSql = $@"
        UPDATE ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"jobqueue".GetProperDbObjectName()}"" 
        SET ""{"fetchedat".GetProperDbObjectName()}"" = NOW() AT TIME ZONE 'UTC', 
            ""{"updatecount".GetProperDbObjectName()}"" = (""{"updatecount".GetProperDbObjectName()}"" + 1) % 2000000000
        WHERE ""{"id".GetProperDbObjectName()}"" = @Id 
        AND ""{"updatecount".GetProperDbObjectName()}"" = @UpdateCount
        RETURNING ""{"id".GetProperDbObjectName()}"" AS ""Id"",
                  ""{"jobid".GetProperDbObjectName()}"" AS ""JobId"", 
                  ""{"queue".GetProperDbObjectName()}"" AS ""Queue"", 
                  ""{"fetchedat".GetProperDbObjectName()}"" AS ""FetchedAt"";
      ";

      string[] fetchConditions = {
        "IS NULL",
        $"< NOW() AT TIME ZONE 'UTC' + INTERVAL '{timeoutSeconds.ToString(CultureInfo.InvariantCulture)} SECONDS'",
      };

      int fetchConditionsIndex = 0;
      do
      {
        cancellationToken.ThrowIfCancellationRequested();

        string jobToFetchJobSql = string.Format(jobToFetchSqlTemplate, fetchConditions[fetchConditionsIndex]);

        FetchedJob jobToFetch = _storage.UseConnection(null, connection => connection.Query<FetchedJob>(jobToFetchJobSql,
            new { Queues = queues.ToList() })
          .SingleOrDefault());

        if (jobToFetch == null)
        {
          if (fetchConditionsIndex == fetchConditions.Length - 1)
          {
            WaitHandle.WaitAny(new[] {
                cancellationToken.WaitHandle,
                SignalDequeue,
              },
              _storage.Options.QueuePollInterval);

            cancellationToken.ThrowIfCancellationRequested();
          }
        }
        else
        {
          markJobAsFetched = _storage.UseConnection(null, connection => connection.Query<FetchedJob>(markJobAsFetchedSql,
              jobToFetch)
            .SingleOrDefault());
        }
        fetchConditionsIndex++;
        fetchConditionsIndex %= fetchConditions.Length;
      } 
      while (markJobAsFetched == null);

      return new PostgreSqlFetchedJob(_storage,
        markJobAsFetched.Id,
        markJobAsFetched.JobId.ToString(CultureInfo.InvariantCulture),
        markJobAsFetched.Queue);
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
