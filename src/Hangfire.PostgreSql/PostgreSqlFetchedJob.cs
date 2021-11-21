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
using Dapper;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
  public class PostgreSqlFetchedJob : IFetchedJob
  {
    private readonly PostgreSqlStorage _storage;
    private bool _disposed;
    private bool _removedFromQueue;
    private bool _requeued;

    public PostgreSqlFetchedJob(
      PostgreSqlStorage storage,
      long id,
      string jobId,
      string queue)
    {
      _storage = storage ?? throw new ArgumentNullException(nameof(storage));

      Id = id;
      JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
      Queue = queue ?? throw new ArgumentNullException(nameof(queue));
    }

    public long Id { get; }
    public string Queue { get; }
    public string JobId { get; }

    public void RemoveFromQueue()
    {
      _storage.UseConnection(null, connection => connection.Execute($@"
        DELETE FROM ""{_storage.Options.SchemaName}"".""jobqueue"" WHERE ""id"" = @Id;
      ",
        new { Id }));

      _removedFromQueue = true;
    }

    public void Requeue()
    {
      _storage.UseConnection(null, connection => connection.Execute($@"
        UPDATE ""{_storage.Options.SchemaName}"".""jobqueue"" 
        SET ""fetchedat"" = NULL 
        WHERE ""id"" = @Id;
      ",
      new { Id }));

      _requeued = true;
    }

    public void Dispose()
    {
      if (_disposed) return;

      if (!_removedFromQueue && !_requeued)
      {
        Requeue();
      }

      _disposed = true;
    }
  }
}
