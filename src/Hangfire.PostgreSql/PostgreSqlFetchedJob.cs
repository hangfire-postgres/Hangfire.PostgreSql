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

using Hangfire.Logging;
using Hangfire.PostgreSql.Utils;
using Hangfire.Storage;

namespace Hangfire.PostgreSql;

internal class PostgreSqlFetchedJob : IFetchedJob
{
  private readonly ILog _logger = LogProvider.GetLogger(typeof(PostgreSqlFetchedJob));
  private readonly PostgreSqlStorageContext _context;
  private readonly TimeSpan _interval;
  private readonly object _syncRoot = new();

  private bool _disposed;
  private bool _removedFromQueue;
  private bool _requeued;
  private long _lastHeartbeat;
    
  public PostgreSqlFetchedJob(
    PostgreSqlStorageContext context,
    long id,
    string jobId,
    string queue,
    DateTime? fetchedAt)
  {
    _context = context ?? throw new ArgumentNullException(nameof(context));

    Id = id;
    JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
    Queue = queue ?? throw new ArgumentNullException(nameof(queue));
    FetchedAt = fetchedAt ?? throw new ArgumentNullException(nameof(fetchedAt));

    if (_context.HeartbeatProcess != null)
    {
      _lastHeartbeat = TimestampHelper.GetTimestamp();
      _interval = TimeSpan.FromSeconds(_context.Options.InvisibilityTimeout.TotalSeconds / 5);
      _context.HeartbeatProcess.Track(this);
    }
  }

  public long Id { get; }
  public string Queue { get; }
  public string JobId { get; }
  internal DateTime? FetchedAt { get; private set; }

  public void RemoveFromQueue()
  {
    lock (_syncRoot)
    {
      if (!FetchedAt.HasValue)
      {
        return;
      }

      string query = _context.QueryProvider.GetQuery("DELETE FROM hangfire.jobqueue WHERE id = $1 AND fetchedat = $2");
      _context.ConnectionManager.UseConnection(null, connection => connection.Process(query).WithParameters(Id, FetchedAt).Execute());

      _removedFromQueue = true;
    }
  }

  public void Requeue()
  {
    lock (_syncRoot)
    {
      if (!FetchedAt.HasValue)
      {
        return;
      }

      string query = _context.QueryProvider.GetQuery("UPDATE hangfire.jobqueue SET fetchedat = NULL WHERE id = $1 AND fetchedat = $2");
      _context.ConnectionManager.UseConnection(null, connection => connection.Process(query).WithParameters(Id, FetchedAt).Execute());

      FetchedAt = null;
      _requeued = true;
    }
  }

  public void Dispose()
  {
    if (_disposed)
    {
      return;
    }

    _disposed = true;

    DisposeTimer();

    lock (_syncRoot)
    {
      if (!_removedFromQueue && !_requeued)
      {
        Requeue();
      }
    }
  }
    
  internal void DisposeTimer()
  {
    if (_context.HeartbeatProcess != null)
    {
      _context.HeartbeatProcess.Untrack(this);
    }
  }
    
  internal void ExecuteKeepAliveQueryIfRequired()
  {
    long now = TimestampHelper.GetTimestamp();

    if (TimestampHelper.Elapsed(now, Interlocked.Read(ref _lastHeartbeat)) < _interval)
    {
      return;
    }
      
    lock (_syncRoot)
    {
      if (!FetchedAt.HasValue)
      {
        return;
      }

      if (_requeued || _removedFromQueue)
      {
        return;
      }

      string query = _context.QueryProvider.GetQuery(
        """
        UPDATE hangfire.jobqueue
        SET fetchedat = NOW()
        WHERE id = $1 AND fetchedat = $2
        RETURNING fetchedat
        """);
      
      try
      {
        FetchedAt = _context.ConnectionManager.UseConnection(null, connection => connection
          .Process(query)
          .WithParameters(Id, FetchedAt)
          .Select<DateTime?>(reader => reader.IsDBNull(0) ? null : reader.GetDateTime(0))
          .SingleOrDefault());

        if (!FetchedAt.HasValue)
        {
          _logger.Warn(
            $"Background job identifier '{JobId}' was fetched by another worker, will not execute keep alive.");
        }

        _logger.Trace($"Keep-alive query for message {Id} sent");
        Interlocked.Exchange(ref _lastHeartbeat, now);
      }
      catch (Exception ex) when (ex.IsCatchableExceptionType())
      {
        _logger.DebugException($"Unable to execute keep-alive query for message {Id}", ex);
      }
    }
  }
}
