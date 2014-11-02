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
using Hangfire.PostgreSql.Annotations;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
    internal class PostgreSqlJobQueue : IPersistentJobQueue
    {
        private readonly PostgreSqlStorageOptions _options;
        private readonly IDbConnection _connection;

        public PostgreSqlJobQueue(IDbConnection connection, PostgreSqlStorageOptions options)
        {
            if (options == null) throw new ArgumentNullException("options");
            if (connection == null) throw new ArgumentNullException("connection");

            _options = options;
            _connection = connection;
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException("queues");
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", "queues");


            long timeoutSeconds = (long)_options.InvisibilityTimeout.Negate().TotalSeconds;
            FetchedJob fetchedJob;

            const string fetchJobSqlTemplate = @"
update ""hangfire"".""jobqueue"" 
set ""fetchedat"" = now() at time zone 'utc'
where ""id"" in (select ""id"" from ""hangfire"".""jobqueue"" where  ""queue"" = ANY @queues and ""fetchedat"" {0} order by ""queue"", ""fetchedat"" limit 1)
returning ""id"" as ""Id"", ""jobid"" as ""JobId"", ""queue"" as ""Queue"";
";

            var fetchConditions = new[] { "is null", string.Format("< now() at time zone 'utc' + interval '{0} seconds'", timeoutSeconds) };
            var currentQueryIndex = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                string fetchJobSql = string.Format(fetchJobSqlTemplate, fetchConditions[currentQueryIndex]);

                fetchedJob = _connection.Query<FetchedJob>(
                    fetchJobSql,
                    new { queues = queues.ToList() })
                    .SingleOrDefault();

                if (fetchedJob == null)
                {
                    if (currentQueryIndex == fetchConditions.Length - 1)
                    {
                        cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }

                currentQueryIndex = (currentQueryIndex + 1) % fetchConditions.Length;
            } while (fetchedJob == null);

            return new PostgreSqlFetchedJob(
                _connection,
                fetchedJob.Id,
                fetchedJob.JobId.ToString(CultureInfo.InvariantCulture),
                fetchedJob.Queue);
        }

        public void Enqueue(string queue, string jobId)
        {
            const string enqueueJobSql = @"
insert into ""hangfire"".""jobqueue"" (""jobid"", ""queue"") values (@jobId, @queue)";

            _connection.Execute(enqueueJobSql, new { jobId = Convert.ToInt32(jobId,CultureInfo.InvariantCulture), queue = queue });
        }

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        private class FetchedJob
        {
            public int Id { get; set; }
            public int JobId { get; set; }
            public string Queue { get; set; }
        }
    }
}