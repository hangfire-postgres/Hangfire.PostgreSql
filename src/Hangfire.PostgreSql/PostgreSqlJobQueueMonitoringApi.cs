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

using Hangfire.PostgreSql.Entities;

namespace Hangfire.PostgreSql;

public interface IPersistentJobQueueMonitoringApi
{
  ICollection<string> GetQueues();
  ICollection<long> GetEnqueuedJobIds(string queue, int from, int perPage);
  ICollection<long> GetFetchedJobIds(string queue, int from, int perPage);
  EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue);
}

internal class PostgreSqlJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
{
  private readonly PostgreSqlStorageContext _context;

  public PostgreSqlJobQueueMonitoringApi(PostgreSqlStorageContext context)
  {
    _context = context ?? throw new ArgumentNullException(nameof(context));
  }

  public ICollection<string> GetQueues()
  {
    string query = _context.QueryProvider.GetQuery("SELECT DISTINCT queue FROM hangfire.jobqueue");
    return _context.ConnectionManager.UseConnection(null, connection => connection.Process(query).Select(reader => reader.GetString(0)).ToList());
  }

  public ICollection<long> GetEnqueuedJobIds(string queue, int from, int perPage)
  {
    return GetQueuedOrFetchedJobIds(queue, false, from, perPage);
  }

  public ICollection<long> GetFetchedJobIds(string queue, int from, int perPage)
  {
    return GetQueuedOrFetchedJobIds(queue, true, from, perPage);
  }

  public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
  {
    string query = _context.QueryProvider.GetQuery(
      """
      SELECT (
          SELECT COUNT(*) 
          FROM hangfire.jobqueue 
          WHERE fetchedat IS NULL AND queue = $1
      ), 
      (
        SELECT COUNT(*) 
        FROM hangfire.jobqueue 
        WHERE fetchedat IS NOT NULL AND queue = $1
      )
      """);

    return _context.ConnectionManager.UseConnection(null, connection => connection
      .Process(query)
      .WithParameter(queue)
      .Select(reader => new EnqueuedAndFetchedCountDto {
        EnqueuedCount = reader.GetInt64(0),
        FetchedCount = reader.GetInt64(1),
      })
      .Single());
  }

  private IList<long> GetQueuedOrFetchedJobIds(string queue, bool fetched, int from, int perPage)
  {
    string query = _context.QueryProvider.GetQuery(
      """
      SELECT j.id
      FROM hangfire.jobqueue jq
      LEFT JOIN hangfire.job j ON jq.jobid = j.id
      WHERE jq.queue = $1 AND jq.fetchedat {0} AND j.id IS NOT NULL
      ORDER BY jq.fetchedat, jq.jobid
      LIMIT $2 OFFSET $3
      """, (fetched ? "IS NOT NULL" : "IS NULL"));

    return _context.ConnectionManager.UseConnection(null, connection => connection
      .Process(query)
      .WithParameters(queue, perPage, from)
      .Select(reader => reader.GetInt64(0))
      .ToList());
  }
}