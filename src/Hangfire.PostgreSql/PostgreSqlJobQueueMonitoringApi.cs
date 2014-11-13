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
using System.Linq;
using Dapper;

namespace Hangfire.PostgreSql
{
    internal class PostgreSqlJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly IDbConnection _connection;

        public PostgreSqlJobQueueMonitoringApi(IDbConnection connection)
        {
            if (connection == null) throw new ArgumentNullException("connection");

            _connection = connection;
        }

        public IEnumerable<string> GetQueues()
        {
            const string sqlQuery = @"
SELECT DISTINCT ""queue"" 
FROM ""hangfire"".""jobqueue"";
";
            return _connection.Query(sqlQuery).Select(x => (string)x.queue).ToList();
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            return GetQueuedOrFetchedJobIds(queue, false, @from, perPage);
        }

        private IEnumerable<int> GetQueuedOrFetchedJobIds(string queue, bool fetched, int @from, int perPage)
        {
            string sqlQuery = string.Format(@"
SELECT j.""id"" 
FROM ""hangfire"".""jobqueue"" jq
LEFT JOIN ""hangfire"".""job"" j ON jq.""jobid"" = j.""id""
LEFT JOIN ""hangfire"".""state"" s ON s.""id"" = j.""stateid""
WHERE jq.""queue"" = @queue 
AND jq.""fetchedat"" {0}
LIMIT @count OFFSET @start;
", fetched ? "IS NOT NULL" : "IS NULL");

            return _connection.Query<int>(
                sqlQuery,
                new {queue = queue, start = @from, count = perPage})
                .ToList();
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            return GetQueuedOrFetchedJobIds(queue, true, @from, perPage);
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            const string sqlQuery = @"
SELECT (
        SELECT COUNT(*) 
        FROM ""hangfire"".""jobqueue"" 
        WHERE ""fetchedat"" IS NULL 
        AND ""queue"" = @queue
    ) ""EnqueuedCount"", 
    (
        SELECT COUNT(*) 
        FROM ""hangfire"".""jobqueue"" 
        WHERE ""fetchedat"" IS NOT NULL 
        AND ""queue"" = @queue
    ) ""FetchedCount"";
";

            var result = _connection.Query(sqlQuery, new { queue = queue }).Single();

            return new EnqueuedAndFetchedCountDto
            {
                EnqueuedCount = result.EnqueuedCount,
                FetchedCount = result.FetchedCount
            };
        }
    }
}