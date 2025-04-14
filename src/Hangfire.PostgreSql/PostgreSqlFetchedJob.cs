using Dapper;
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

      string query = _context.QueryProvider.GetQuery("DELETE FROM hangfire.jobqueue WHERE id = @Id AND fetchedat = @FetchedAt");
      _context.ConnectionManager.UseConnection(null, connection => connection.Execute(query, new { Id, FetchedAt }));

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

      string query = _context.QueryProvider.GetQuery("UPDATE hangfire.jobqueue SET fetchedat = NULL WHERE id = @Id AND fetchedat = @FetchedAt");
      _context.ConnectionManager.UseConnection(null, connection => connection.Execute(query, new { Id, FetchedAt }));

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
        WHERE id = @Id AND fetchedat = @FetchedAt
        RETURNING fetchedat AS FetchedAt
        """);
        
      try
      {
        _context.ConnectionManager.UseConnection(null,
          connection => FetchedAt = connection.ExecuteScalar<DateTime?>(query, new { queue = Queue, id = Id, fetchedAt = FetchedAt }));

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
