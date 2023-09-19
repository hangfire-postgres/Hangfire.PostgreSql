using System;
using System.Globalization;
using System.Linq;
using Dapper;
using Hangfire.PostgreSql.Tests.Utils;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
  public class PostgreSqlFetchedJobFacts
  {
    private const string JobId = "id";
    private const string Queue = "queue";

    private readonly PostgreSqlStorage _storage;

    public PostgreSqlFetchedJobFacts()
    {
      _storage = new PostgreSqlStorage(ConnectionUtils.GetDefaultConnectionFactory());
    }

    [Fact]
    public void Ctor_ThrowsAnException_WhenStorageIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new PostgreSqlFetchedJob(null, 1, JobId, Queue));

      Assert.Equal("storage", exception.ParamName);
    }

    [Fact]
    public void Ctor_ThrowsAnException_WhenJobIdIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new PostgreSqlFetchedJob(_storage, 1, null, Queue));

      Assert.Equal("jobId", exception.ParamName);
    }

    [Fact]
    public void Ctor_ThrowsAnException_WhenQueueIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new PostgreSqlFetchedJob(_storage, 1, JobId, null));

      Assert.Equal("queue", exception.ParamName);
    }

    [Fact]
    public void Ctor_CorrectlySets_AllInstanceProperties()
    {
      PostgreSqlFetchedJob fetchedJob = new(_storage, 1, JobId, Queue);

      Assert.Equal(1, fetchedJob.Id);
      Assert.Equal(JobId, fetchedJob.JobId);
      Assert.Equal(Queue, fetchedJob.Queue);
    }

    [Fact]
    [CleanDatabase]
    public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
    {
      // Arrange
      long id = CreateJobQueueRecord(_storage, "1", "default");
      PostgreSqlFetchedJob processingJob = new(_storage, id, "1", "default");

      // Act
      processingJob.RemoveFromQueue();

      // Assert
      long count = _storage.UseConnection(null, connection =>
        connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""jobqueue"""));
      Assert.Equal(0, count);
    }

    [Fact]
    [CleanDatabase]
    public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
    {
      // Arrange
      CreateJobQueueRecord(_storage, "1", "default");
      CreateJobQueueRecord(_storage, "1", "critical");
      CreateJobQueueRecord(_storage, "2", "default");

      PostgreSqlFetchedJob fetchedJob = new PostgreSqlFetchedJob(_storage, 999, "1", "default");

      // Act
      fetchedJob.RemoveFromQueue();

      // Assert
      long count = _storage.UseConnection(null, connection =>
        connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""jobqueue"""));
      Assert.Equal(3, count);
    }

    [Fact]
    [CleanDatabase]
    public void Requeue_SetsFetchedAtValueToNull()
    {
      // Arrange
      long id = CreateJobQueueRecord(_storage, "1", "default");
      PostgreSqlFetchedJob processingJob = new(_storage, id, "1", "default");

      // Act
      processingJob.Requeue();

      // Assert
      dynamic record = _storage.UseConnection(null, connection =>
        connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""jobqueue""").Single());
      Assert.Null(record.FetchedAt);
    }

    [Fact]
    [CleanDatabase]
    public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
    {
      // Arrange
      long id = CreateJobQueueRecord(_storage, "1", "default");
      PostgreSqlFetchedJob processingJob = new(_storage, id, "1", "default");

      // Act
      processingJob.Dispose();

      // Assert
      dynamic record = _storage.UseConnection(null, connection =>
        connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""jobqueue""").Single());
      Assert.Null(record.fetchedat);
    }

    private static long CreateJobQueueRecord(PostgreSqlStorage storage, string jobId, string queue)
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""jobqueue"" (""jobid"", ""queue"", ""fetchedat"")
        VALUES (@Id, @Queue, NOW()) RETURNING ""id""
      ";

      return
        storage.UseConnection(null, connection => 
          connection.QuerySingle<long>(arrangeSql, new { Id = Convert.ToInt64(jobId, CultureInfo.InvariantCulture), Queue = queue }));
    }

    private static string GetSchemaName()
    {
      return ConnectionUtils.GetSchemaName();
    }
  }
}
