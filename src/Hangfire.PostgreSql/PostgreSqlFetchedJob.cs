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
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.PostgreSql.Utils;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
  public class PostgreSqlFetchedJob : IFetchedJob
  {
    private readonly ILog _logger = LogProvider.GetLogger(typeof(PostgreSqlFetchedJob));
    
    private readonly PostgreSqlStorage _storage;
    private bool _disposed;
    private bool _removedFromQueue;
    private bool _requeued;

    private readonly object _syncRoot = new object();
    private long _lastHeartbeat;
    private readonly TimeSpan _interval;
    
    public PostgreSqlFetchedJob(
      PostgreSqlStorage storage,
      long id,
      string jobId,
      string queue,
      DateTime? fetchedAt)
    {
      _storage = storage ?? throw new ArgumentNullException(nameof(storage));

      Id = id;
      JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
      Queue = queue ?? throw new ArgumentNullException(nameof(queue));
      FetchedAt = fetchedAt ?? throw new ArgumentNullException(nameof(fetchedAt));

      if (storage.Options.UseSlidingInvisibilityTimeout)
      {
        _lastHeartbeat = TimestampHelper.GetTimestamp();
        _interval = TimeSpan.FromSeconds(storage.Options.InvisibilityTimeout.TotalSeconds / 5);
        storage.HeartbeatProcess.Track(this);
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
        
        _storage.UseConnection(null, connection => connection.Execute($@"
          DELETE FROM ""{_storage.Options.SchemaName}"".""jobqueue"" WHERE ""id"" = @Id AND ""fetchedat"" = @FetchedAt;
        ",
          new { Id, FetchedAt }));

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

        _storage.UseConnection(null, connection => connection.Execute($@"
          UPDATE ""{_storage.Options.SchemaName}"".""jobqueue"" 
          SET ""fetchedat"" = NULL 
          WHERE ""id"" = @Id AND ""fetchedat"" = @FetchedAt;
        ",
          new { Id, FetchedAt }));

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
      if (_storage.Options.UseSlidingInvisibilityTimeout)
      {
        _storage.HeartbeatProcess.Untrack(this);
      }
    }
    
    internal void ExecuteKeepAliveQueryIfRequired()
    {
      var now = TimestampHelper.GetTimestamp();

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
        
        string updateFetchAtSql = $@"
          UPDATE ""{_storage.Options.SchemaName}"".""jobqueue"" 
          SET ""fetchedat"" = NOW()
          WHERE ""id"" = @id AND ""fetchedat"" = @fetchedAt
          RETURNING ""fetchedat"" AS ""FetchedAt"";
        ";
        
        try
        {
          _storage.UseConnection(null, connection =>
          {
            FetchedAt = connection.ExecuteScalar<DateTime?>(updateFetchAtSql,
              new { queue = Queue, id = Id, fetchedAt = FetchedAt });
          });

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
}
