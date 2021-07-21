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
using Dapper;
using Hangfire.PostgreSql.Properties;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    public class PostgreSqlJobQueue : IPersistentJobQueue
    {
        internal static readonly AutoResetEvent NewItemInQueueEvent = new AutoResetEvent(true);
        private readonly PostgreSqlStorageOptions _options;
        private readonly PostgreSqlStorage _storage;

		private AutoResetEvent SignalDequeue { get; }

		public PostgreSqlJobQueue(PostgreSqlStorage storage, PostgreSqlStorageOptions options)
		{
			_options = options ?? throw new ArgumentNullException(nameof(options));
			_storage = storage ?? throw new ArgumentNullException(nameof(storage));
			SignalDequeue = new AutoResetEvent(false);
		}
        public PostgreSqlJobQueue(PostgreSqlStorage storage, PostgreSqlStorageOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }


        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (_options.UseNativeDatabaseTransactions)
                return Dequeue_Transaction(queues, cancellationToken);

		/// <summary>
		///		Signal the waiting Thread to lookup a new Job
		/// </summary>
		public void FetchNextJob()
		{
			SignalDequeue.Set();
		}

        public void Enqueue(IDbConnection connection, string queue, string jobId)
        {
            var enqueueJobSql = @"
INSERT INTO """ + _options.SchemaName + @""".""jobqueue"" (""jobid"", ""queue"") 
VALUES (@jobId, @queue);
";

            connection.Execute(enqueueJobSql,
                new {jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture), queue});
        }


        [NotNull]
        internal IFetchedJob Dequeue_Transaction(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            var timeoutSeconds = (long) _options.InvisibilityTimeout.Negate().TotalSeconds;
            FetchedJob fetchedJob;

            var fetchJobSqlTemplate = @"
UPDATE """ + _options.SchemaName + @""".""jobqueue"" 
SET ""fetchedat"" = NOW() AT TIME ZONE 'UTC'
WHERE ""id"" = (
    SELECT ""id"" 
    FROM """ + _options.SchemaName + @""".""jobqueue"" 
    WHERE ""queue"" = ANY (@queues)
    AND ""fetchedat"" {0}
    ORDER BY ""queue"", ""fetchedat"", ""jobid""
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING ""id"" AS ""Id"", ""jobid"" AS ""JobId"", ""queue"" AS ""Queue"", ""fetchedat"" AS ""FetchedAt"";
";

            var fetchConditions = new[]
            {
                "IS NULL",
                $"< NOW() AT TIME ZONE 'UTC' + INTERVAL '{timeoutSeconds.ToString(CultureInfo.InvariantCulture)} SECONDS'"
            };
            var currentQueryIndex = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                var fetchJobSql = string.Format(fetchJobSqlTemplate, fetchConditions[currentQueryIndex]);

                Utils.Utils.TryExecute(() =>
                    {
                        var connection = _storage.CreateAndOpenConnection();

                        try
                        {
                            using (var trx = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                            {
                                var jobToFetch = connection.Query<FetchedJob>(
                                        fetchJobSql,
                                        new {queues = queues.ToList()}, trx)
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
                    ex =>
                    {
                        var postgresException = ex as PostgresException;
                        var smoothException = false;

                        if (postgresException != null)
                            if (postgresException.SqlState.Equals("40001"))
                                smoothException = true;

                        return smoothException;
                    });

                if (fetchedJob == null && currentQueryIndex == fetchConditions.Length - 1)
                {
                    WaitHandle.WaitAny(new[]
                        {
                            cancellationToken.WaitHandle, 
                            NewItemInQueueEvent,
	                        SignalDequeue
                        },
                        _options.QueuePollInterval);

                    cancellationToken.ThrowIfCancellationRequested();
                }

                currentQueryIndex = (currentQueryIndex + 1) % fetchConditions.Length;
            } while (fetchedJob == null);

            return new PostgreSqlFetchedJob(
                _storage,
                _options,
                fetchedJob.Id,
                fetchedJob.JobId.ToString(CultureInfo.InvariantCulture),
                fetchedJob.Queue);
        }


        [NotNull]
        internal IFetchedJob Dequeue_UpdateCount(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException("queues");
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", "queues");


            var timeoutSeconds = (long) _options.InvisibilityTimeout.Negate().TotalSeconds;
            FetchedJob markJobAsFetched = null;


            var jobToFetchSqlTemplate = @"
SELECT ""id"" AS ""Id"", ""jobid"" AS ""JobId"", ""queue"" AS ""Queue"", ""fetchedat"" AS ""FetchedAt"", ""updatecount"" AS ""UpdateCount""
FROM """ + _options.SchemaName + @""".""jobqueue"" 
WHERE ""queue"" = ANY (@queues)
AND ""fetchedat"" {0} 
ORDER BY ""queue"", ""fetchedat"", ""jobid"" 
LIMIT 1;
";

            var markJobAsFetchedSql = @"
UPDATE """ + _options.SchemaName + @""".""jobqueue"" 
SET ""fetchedat"" = NOW() AT TIME ZONE 'UTC', 
    ""updatecount"" = (""updatecount"" + 1) % 2000000000
WHERE ""id"" = @Id 
AND ""updatecount"" = @UpdateCount
RETURNING ""id"" AS ""Id"", ""jobid"" AS ""JobId"", ""queue"" AS ""Queue"", ""fetchedat"" AS ""FetchedAt"";
";

            var fetchConditions = new[]
            {
                "IS NULL",
                $"< NOW() AT TIME ZONE 'UTC' + INTERVAL '{timeoutSeconds.ToString(CultureInfo.InvariantCulture)} SECONDS'"
            };
            var currentQueryIndex = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                var jobToFetchJobSql = string.Format(jobToFetchSqlTemplate, fetchConditions[currentQueryIndex]);

                var jobToFetch = _storage.UseConnection(connection => connection.Query<FetchedJob>(
                        jobToFetchJobSql,
                        new {queues = queues.ToList()})
                    .SingleOrDefault());

                if (jobToFetch == null)
                {
                    if (currentQueryIndex == fetchConditions.Length - 1)
                    {
                        cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }
                else
                {
                    markJobAsFetched = _storage.UseConnection(connection => connection.Query<FetchedJob>(
                            markJobAsFetchedSql,
                            jobToFetch)
                        .SingleOrDefault());
                }


                currentQueryIndex = (currentQueryIndex + 1) % fetchConditions.Length;
            } while (markJobAsFetched == null);

            return new PostgreSqlFetchedJob(
                _storage,
                _options,
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
