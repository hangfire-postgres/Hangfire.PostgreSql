using System.Globalization;
using Dapper;
using Hangfire.PostgreSql.Tests.Utils;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
  public class PostgreSqlFetchedJobFacts
  {
    private readonly DateTime _fetchedAt = DateTime.UtcNow; 

    private readonly PostgreSqlStorage _storage;

    public PostgreSqlFetchedJobFacts()
    {
      _storage = new PostgreSqlStorage(ConnectionUtils.GetDefaultConnectionFactory());
    }

    [Fact]
    [CleanDatabase]
    public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
    {
      // Arrange
      long id = CreateJobQueueRecord(_storage, "1", "default", _fetchedAt);
      PostgreSqlFetchedJob processingJob = new(_storage.Context, id, "1", "default", _fetchedAt);

      // Act
      processingJob.RemoveFromQueue();

      // Assert
      long count = _storage.Context.ConnectionManager.UseConnection(null, connection =>
        connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""jobqueue"""));
      Assert.Equal(0, count);
    }

    [Fact]
    [CleanDatabase]
    public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
    {
      // Arrange
      CreateJobQueueRecord(_storage, "1", "default", _fetchedAt);
      CreateJobQueueRecord(_storage, "1", "critical", _fetchedAt);
      CreateJobQueueRecord(_storage, "2", "default", _fetchedAt);

      PostgreSqlFetchedJob fetchedJob = new PostgreSqlFetchedJob(_storage.Context, 999, "1", "default", _fetchedAt);

      // Act
      fetchedJob.RemoveFromQueue();

      // Assert
      long count = _storage.Context.ConnectionManager.UseConnection(null, connection =>
        connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""jobqueue"""));
      Assert.Equal(3, count);
    }
    
    [Fact]
    [CleanDatabase]
    public void Requeue_SetsFetchedAtValueToNull()
    {
      // Arrange
      long id = CreateJobQueueRecord(_storage, "1", "default", _fetchedAt);
      PostgreSqlFetchedJob processingJob = new(_storage.Context, id, "1", "default", _fetchedAt);

      // Act
      processingJob.Requeue();

      // Assert
      dynamic record = _storage.Context.ConnectionManager.UseConnection(null, connection =>
        connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""jobqueue""").Single());
      Assert.Null(record.fetchedat);
    }

    [Fact]
    [CleanDatabase]
    public void Timer_UpdatesFetchedAtColumn()
    {
      _storage.Context.ConnectionManager.UseConnection(null, connection => {
        // Arrange
        DateTime fetchedAt = DateTime.UtcNow.AddMinutes(-5);
        long id = CreateJobQueueRecord(_storage, "1", "default", fetchedAt);
        using (PostgreSqlFetchedJob processingJob = new PostgreSqlFetchedJob(_storage.Context, id, "1", "default", fetchedAt))
        {
          processingJob.DisposeTimer();
          Thread.Sleep(TimeSpan.FromSeconds(10));
          processingJob.ExecuteKeepAliveQueryIfRequired();

          dynamic record = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""jobqueue""").Single();

          Assert.NotNull(processingJob.FetchedAt);
          Assert.Equal<DateTime?>(processingJob.FetchedAt, record.fetchedat);
          DateTime now = DateTime.UtcNow;
          Assert.True(now.AddSeconds(-5) < record.fetchedat, (now - record.fetchedat).ToString());
        }
      });
    }

    [Fact]
    [CleanDatabase]
    public void RemoveFromQueue_AfterTimer_RemovesJobFromTheQueue()
    {
      _storage.Context.ConnectionManager.UseConnection(null, connection => {
        // Arrange
        long id = CreateJobQueueRecord(_storage, "1", "default", _fetchedAt);
        using (PostgreSqlFetchedJob processingJob = new PostgreSqlFetchedJob(_storage.Context, id, "1", "default", _fetchedAt))
        {
          Thread.Sleep(TimeSpan.FromSeconds(10));
          processingJob.DisposeTimer();

          // Act
          processingJob.RemoveFromQueue();

          // Assert
          int count = connection.Query<int>($@"SELECT count(*) FROM ""{GetSchemaName()}"".""jobqueue""").Single();
          Assert.Equal(0, count);
        }
      });
    }

    [Fact]
    [CleanDatabase]
    public void RequeueQueue_AfterTimer_SetsFetchedAtValueToNull()
    {
      _storage.Context.ConnectionManager.UseConnection(null, connection => {
        // Arrange
        long id = CreateJobQueueRecord(_storage, "1", "default", _fetchedAt);
        using (PostgreSqlFetchedJob processingJob = new PostgreSqlFetchedJob(_storage.Context, id, "1", "default", _fetchedAt))
        {
          Thread.Sleep(TimeSpan.FromSeconds(10));
          processingJob.DisposeTimer();

          // Act
          processingJob.Requeue();

          // Assert
          dynamic record = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""jobqueue""").Single();
          Assert.Null(record.fetchedat);
        }
      });
    }
    
    [Fact]
    [CleanDatabase]
    public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
    {
      // Arrange
      long id = CreateJobQueueRecord(_storage, "1", "default", _fetchedAt);
      PostgreSqlFetchedJob processingJob = new(_storage.Context, id, "1", "default", _fetchedAt);

      // Act
      processingJob.Dispose();

      // Assert
      dynamic record = _storage.Context.ConnectionManager.UseConnection(null, connection =>
        connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""jobqueue""").Single());
      Assert.Null(record.fetchedat);
    }

    private static long CreateJobQueueRecord(PostgreSqlStorage storage, string jobId, string queue, DateTime? fetchedAt)
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""jobqueue"" (""jobid"", ""queue"", ""fetchedat"")
        VALUES (@Id, @Queue, @FetchedAt) RETURNING ""id""
      ";

      return
        storage.Context.ConnectionManager.UseConnection(null, connection =>
          connection.QuerySingle<long>(arrangeSql, 
            new { Id = Convert.ToInt64(jobId, CultureInfo.InvariantCulture), Queue = queue, FetchedAt = fetchedAt }));
    }

    private static string GetSchemaName()
    {
      return ConnectionUtils.GetSchemaName();
    }
  }
}
