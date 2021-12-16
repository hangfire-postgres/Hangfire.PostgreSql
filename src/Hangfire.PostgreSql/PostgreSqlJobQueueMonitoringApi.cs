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
using System.Linq;
using Dapper;
using Hangfire.PostgreSql.Extensions;

namespace Hangfire.PostgreSql
{
  internal class PostgreSqlJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
  {
    private readonly PostgreSqlStorage _storage;

    public PostgreSqlJobQueueMonitoringApi(PostgreSqlStorage storage)
    {
      _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    public IEnumerable<string> GetQueues()
    {
      string sqlQuery = $@"SELECT DISTINCT ""{"queue".GetProperDbObjectName()}"" 
                           FROM ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"jobqueue".GetProperDbObjectName()}""";
      return _storage.UseConnection(null, connection => connection.Query<string>(sqlQuery).ToList());
    }

    public IEnumerable<long> GetEnqueuedJobIds(string queue, int from, int perPage)
    {
      return GetQueuedOrFetchedJobIds(queue, false, from, perPage);
    }

    public IEnumerable<long> GetFetchedJobIds(string queue, int from, int perPage)
    {
      return GetQueuedOrFetchedJobIds(queue, true, from, perPage);
    }

    public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
    {
      string sqlQuery = $@"
        SELECT (
            SELECT COUNT(*) 
            FROM ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"jobqueue".GetProperDbObjectName()}"" 
            WHERE ""{"fetchedat".GetProperDbObjectName()}"" IS NULL 
            AND ""{"queue".GetProperDbObjectName()}"" = @Queue
        ) ""EnqueuedCount"", 
        (
          SELECT COUNT(*) 
          FROM ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"jobqueue".GetProperDbObjectName()}"" 
          WHERE ""{"fetchedat".GetProperDbObjectName()}"" IS NOT NULL 
          AND ""{"queue".GetProperDbObjectName()}"" = @Queue
        ) ""FetchedCount"";
      ";

      (long enqueuedCount, long fetchedCount) = _storage.UseConnection(null, connection => 
        connection.QuerySingle<(long EnqueuedCount, long FetchedCount)>(sqlQuery, new { Queue = queue }));

      return new EnqueuedAndFetchedCountDto {
        EnqueuedCount = enqueuedCount,
        FetchedCount = fetchedCount,
      };
    }

    private IEnumerable<long> GetQueuedOrFetchedJobIds(string queue, bool fetched, int from, int perPage)
    {
      string sqlQuery = $@"
        SELECT j.""{"id".GetProperDbObjectName()}"" 
        FROM ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"jobqueue".GetProperDbObjectName()}"" jq
        LEFT JOIN ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"job".GetProperDbObjectName()}"" j 
        ON jq.""{"jobid".GetProperDbObjectName()}"" = j.""{"id".GetProperDbObjectName()}""
        WHERE jq.""{"queue".GetProperDbObjectName()}"" = @Queue 
        AND jq.""{"fetchedat".GetProperDbObjectName()}"" {(fetched ? "IS NOT NULL" : "IS NULL")}
        AND j.""{"id".GetProperDbObjectName()}"" IS NOT NULL
        LIMIT @Limit OFFSET @Offset;
      ";

      return _storage.UseConnection(null, connection => connection.Query<long>(sqlQuery,
          new { Queue = queue, Offset = from, Limit = perPage })
        .ToList());
    }
  }
}
