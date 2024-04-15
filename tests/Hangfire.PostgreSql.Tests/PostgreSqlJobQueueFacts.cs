using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.PostgreSql.Utils;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
  public class PostgreSqlJobQueueFacts : IClassFixture<PostgreSqlStorageFixture>
  {
    private static readonly string[] _defaultQueues = { "default" };

    private readonly PostgreSqlStorageFixture _fixture;

    public PostgreSqlJobQueueFacts(PostgreSqlStorageFixture fixture)
    {
      _fixture = fixture;
      _fixture.SetupOptions(o => o.UseSlidingInvisibilityTimeout = true);
    }

    [Fact]
    public void Ctor_ThrowsAnException_WhenStorageIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new PostgreSqlJobQueue(null));

      Assert.Equal("storage", exception.ParamName);
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
    {
      UseConnection((_, storage) => {
        PostgreSqlJobQueue queue = CreateJobQueue(storage, false);

        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => queue.Dequeue(null, CreateTimingOutCancellationToken()));

        Assert.Equal("queues", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldFetchAJob_FromQueueWithHigherPriority()
    {
      UseConnection((connection, storage) => {
        PostgreSqlJobQueue queue = CreateJobQueue(storage, false);
        CancellationToken token = CreateTimingOutCancellationToken();

        queue.Enqueue(connection, "1", "1");
        queue.Enqueue(connection, "2", "2");
        queue.Enqueue(connection, "3", "3");

        Assert.Equal("1", queue.Dequeue(new[] { "1", "2", "3" }, token).JobId);
        Assert.Equal("2", queue.Dequeue(new[] { "2", "3", "1" }, token).JobId);
        Assert.Equal("3", queue.Dequeue(new[] { "3", "1", "2" }, token).JobId);
      });
    }

    [Fact]
    [CleanDatabase]
    private void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty_WithUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty(true);
    }

    [Fact]
    [CleanDatabase]
    private void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty_WithoutUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty(false);
    }

    private void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty(bool useNativeDatabaseTransactions)
    {
      UseConnection((_, storage) => {
        PostgreSqlJobQueue queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

        ArgumentException exception = Assert.Throws<ArgumentException>(() => queue.Dequeue(Array.Empty<string>(), CreateTimingOutCancellationToken()));

        Assert.Equal("queues", exception.ParamName);
      });
    }

    [Fact]
    private void
      Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning_WithUseNativeDatabaseTransactions()
    {
      Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning(true);
    }

    [Fact]
    private void
      Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning_WithoutUseNativeDatabaseTransactions()
    {
      Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning(false);
    }

    private void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning(
      bool useNativeDatabaseTransactions)
    {
      UseConnection((_, storage) => {
        CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();
        PostgreSqlJobQueue queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

        Assert.Throws<OperationCanceledException>(() => queue.Dequeue(_defaultQueues, cts.Token));
      });
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs_WithUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs(true);
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs_WithoutUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs(false);
    }

    private void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs(bool useNativeDatabaseTransactions)
    {
      UseConnection((_, storage) => {
        CancellationTokenSource cts = new CancellationTokenSource(200);
        PostgreSqlJobQueue queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

        Assert.Throws<OperationCanceledException>(() => queue.Dequeue(_defaultQueues, cts.Token));
      });
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue_WithUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue(true);
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue_WithoutUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue(false);
    }

    private void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue(bool useNativeDatabaseTransactions)
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""jobqueue"" (""jobid"", ""queue"")
        VALUES (@JobId, @Queue) RETURNING ""id""
      ";

      // Arrange
      UseConnection((connection, storage) => {
        long id = connection.QuerySingle<long>(arrangeSql,
          new { JobId = 1, Queue = "default" });
        PostgreSqlJobQueue queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

        // Act
        PostgreSqlFetchedJob payload = (PostgreSqlFetchedJob)queue.Dequeue(_defaultQueues,
          CreateTimingOutCancellationToken());

        // Assert
        Assert.Equal(id, payload.Id);
        Assert.Equal("1", payload.JobId);
        Assert.Equal("default", payload.Queue);
      });
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue_WithUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue(true);
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue_WithoutUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue(false);
    }

    private void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue(bool useNativeDatabaseTransactions)
    {
      string arrangeSql = $@"
        WITH i AS (
          INSERT INTO ""{GetSchemaName()}"".""job"" (""invocationdata"", ""arguments"", ""createdat"")
          VALUES (@InvocationData, @Arguments, NOW())
          RETURNING ""id""
        )
        INSERT INTO ""{GetSchemaName()}"".""jobqueue"" (""jobid"", ""queue"")
        SELECT i.""id"", @Queue FROM i;
      ";

      // Arrange
      UseConnection((connection, storage) => {
        connection.Execute(arrangeSql,
          new { InvocationData = new JsonParameter(""), Arguments = new JsonParameter("", JsonParameter.ValueType.Array), Queue = "default" });
        PostgreSqlJobQueue queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

        // Act
        IFetchedJob payload = queue.Dequeue(_defaultQueues,
          CreateTimingOutCancellationToken());

        // Assert
        Assert.NotNull(payload);

        DateTime? fetchedAt = connection.QuerySingle<DateTime?>($@"SELECT ""fetchedat"" FROM ""{GetSchemaName()}"".""jobqueue"" WHERE ""jobid"" = @Id",
          new { Id = Convert.ToInt64(payload.JobId, CultureInfo.InvariantCulture) });

        Assert.NotNull(fetchedAt);
        Assert.True(fetchedAt > DateTime.UtcNow.AddMinutes(-1));
      });
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue_WithUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue(true, false);
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue_WithoutUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue(false, false);
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue_WithUseNativeDatabaseTransactionsAndSlidingInvisbility()
    {
      Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue(true, true);
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue_WithoutUseNativeDatabaseTransactionsAndSlidingInvisbility()
    {
      Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue(false, true);
    }
    
    private void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue(bool useNativeDatabaseTransactions, bool useSlidingInvisibilityTimeout)
    {
      string arrangeSql = $@"
        WITH i AS (
          INSERT INTO ""{GetSchemaName()}"".""job"" (""invocationdata"", ""arguments"", ""createdat"")
          VALUES (@InvocationData, @Arguments, NOW())
          RETURNING ""id""
        )
        INSERT INTO ""{GetSchemaName()}"".""jobqueue"" (""jobid"", ""queue"", ""fetchedat"")
        SELECT i.""id"", @Queue, @FetchedAt 
        FROM i;
      ";

      // Arrange
      UseConnection((connection, storage) => {
        connection.Execute(arrangeSql,
          new {
            Queue = "default",
            FetchedAt = DateTime.UtcNow.AddDays(-1),
            InvocationData = new JsonParameter(""),
            Arguments = new JsonParameter("", JsonParameter.ValueType.Array),
          });
        PostgreSqlJobQueue queue = CreateJobQueue(storage, useNativeDatabaseTransactions, useSlidingInvisibilityTimeout: useSlidingInvisibilityTimeout);

        // Act
        IFetchedJob payload = queue.Dequeue(_defaultQueues,
          CreateTimingOutCancellationToken());

        // Assert
        Assert.NotEmpty(payload.JobId);
      });
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob_WithUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob(true);
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob_WithoutUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob(false);
    }

    private void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob(bool useNativeDatabaseTransactions)
    {
      string arrangeSql = $@"
        WITH i AS (
          INSERT INTO ""{GetSchemaName()}"".""job"" (""invocationdata"", ""arguments"", ""createdat"")
          VALUES (@InvocationData, @Arguments, NOW())
          RETURNING ""id""
        )
        INSERT INTO ""{GetSchemaName()}"".""jobqueue"" (""jobid"", ""queue"")
        SELECT i.""id"", @Queue FROM i;
      ";

      UseConnection((connection, storage) => {
        connection.Execute(arrangeSql,
          new[] {
            new { Queue = "default", InvocationData = new JsonParameter(""), Arguments = new JsonParameter("", JsonParameter.ValueType.Array) },
            new { Queue = "default", InvocationData = new JsonParameter(""), Arguments = new JsonParameter("", JsonParameter.ValueType.Array) },
          });
        PostgreSqlJobQueue queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

        // Act
        IFetchedJob payload = queue.Dequeue(_defaultQueues,
          CreateTimingOutCancellationToken());

        // Assert
        DateTime? otherJobFetchedAt = connection.QuerySingle<DateTime?>($@"SELECT ""fetchedat"" FROM ""{GetSchemaName()}"".""jobqueue"" WHERE ""jobid"" <> @Id",
          new { Id = Convert.ToInt64(payload.JobId, CultureInfo.InvariantCulture) });

        Assert.Null(otherJobFetchedAt);
      });
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues_WithUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues(true);
    }

    [Fact]
    [CleanDatabase]
    public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues_WithoutUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues(false);
    }


    private void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues(bool useNativeDatabaseTransactions)
    {
      string arrangeSql = $@"
        WITH i AS (
          INSERT INTO ""{GetSchemaName()}"".""job"" (""invocationdata"", ""arguments"", ""createdat"")
          VALUES (@InvocationData, @Arguments, NOW())
          RETURNING ""id""
        )
        INSERT INTO ""{GetSchemaName()}"".""jobqueue"" (""jobid"", ""queue"")
        SELECT i.""id"", @Queue FROM i;
      ";
      UseConnection((connection, storage) => {
        PostgreSqlJobQueue queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

        connection.Execute(arrangeSql,
          new { Queue = "critical", InvocationData = new JsonParameter(""), Arguments = new JsonParameter("", JsonParameter.ValueType.Array) });

        Assert.Throws<OperationCanceledException>(() => queue.Dequeue(_defaultQueues,
          CreateTimingOutCancellationToken()));
      });
    }

    [Fact]
    [CleanDatabase]
    private void Dequeue_ShouldFetchJobs_FromMultipleQueues_WithUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldFetchJobs_FromMultipleQueues(true);
    }

    [Fact]
    [CleanDatabase]
    private void Dequeue_ShouldFetchJobs_FromMultipleQueues_WithoutUseNativeDatabaseTransactions()
    {
      Dequeue_ShouldFetchJobs_FromMultipleQueues(false);
    }

    private void Dequeue_ShouldFetchJobs_FromMultipleQueues(bool useNativeDatabaseTransactions)
    {
      string arrangeSql = $@"
        WITH i AS (
          INSERT INTO ""{GetSchemaName()}"".""job"" (""invocationdata"", ""arguments"", ""createdat"")
          VALUES (@InvocationData, @Arguments, NOW())
          RETURNING ""id""
        )
        INSERT INTO ""{GetSchemaName()}"".""jobqueue"" (""jobid"", ""queue"")
        SELECT i.""id"", @Queue FROM i;
      ";

      string[] queueNames = { "default", "critical" };

      UseConnection((connection, storage) => {
        connection.Execute(arrangeSql,
          new[] {
            new { Queue = queueNames.First(), InvocationData = new JsonParameter("") , Arguments = new JsonParameter("", JsonParameter.ValueType.Array) },
            new { Queue = queueNames.Last(), InvocationData = new JsonParameter(""), Arguments = new JsonParameter("", JsonParameter.ValueType.Array) },
          });

        PostgreSqlJobQueue queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

        PostgreSqlFetchedJob queueFirst = (PostgreSqlFetchedJob)queue.Dequeue(queueNames,
          CreateTimingOutCancellationToken());

        Assert.NotNull(queueFirst.JobId);
        Assert.Contains(queueFirst.Queue, queueNames);

        PostgreSqlFetchedJob queueLast = (PostgreSqlFetchedJob)queue.Dequeue(queueNames,
          CreateTimingOutCancellationToken());

        Assert.NotNull(queueLast.JobId);
        Assert.Contains(queueLast.Queue, queueNames);
      });
    }

    [Fact]
    [CleanDatabase]
    public void Enqueue_AddsAJobToTheQueue_WithUseNativeDatabaseTransactions()
    {
      Enqueue_AddsAJobToTheQueue(true);
    }

    [Fact]
    [CleanDatabase]
    public void Enqueue_AddsAJobToTheQueue_WithoutUseNativeDatabaseTransactions()
    {
      Enqueue_AddsAJobToTheQueue(false);
    }

    [Fact]
    [CleanDatabase]
    public void Queues_Should_Support_Long_Queue_Names()
    {
      UseConnection((connection, storage) => {
        PostgreSqlJobQueue queue = CreateJobQueue(storage, false);

        string name = "very_long_name_that_is_over_20_characters_long_or_something";

        Assert.True(name.Length > 21);

        queue.Enqueue(connection, name, "1");

        string retrievedName = connection.QuerySingle<string>($@"SELECT ""queue"" FROM ""{GetSchemaName()}"".""jobqueue""");
        Assert.Equal(name, retrievedName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void Queues_Can_Dequeue_On_Signal()
    {
      UseConnection((connection, storage) => {
        PostgreSqlJobQueue queue = CreateJobQueue(storage, false);
        IFetchedJob job = null;
        //as UseConnection does not support async-await we have to work with Thread.Sleep

        Task.Run(() => {
          //dequeue the job asynchronously
          job = queue.Dequeue(new[] { "default" }, CreateTimingOutCancellationToken());
        });
        //all sleeps are possibly way to high but this ensures that any race condition is unlikely
        //to ensure that the task would run 
        Thread.Sleep(1000);
        Assert.Null(job);
        //enqueue a job that does not trigger the existing queue to reevaluate its state
        queue.Enqueue(connection, "default", "1");
        Thread.Sleep(1000);
        //the job should still be unset
        Assert.Null(job);
        //trigger a reevaluation
        queue.FetchNextJob();
        //wait for the Dequeue to execute and return the next job
        Thread.Sleep(1000);
        Assert.NotNull(job);
      });
    }

    [Fact]
    [CleanDatabase]
    public void Queues_Can_Dequeue_On_Notification()
    {
      UseConnection((connection, storage) => {
        TimeSpan timeout = TimeSpan.FromSeconds(30);

        // Only for Postgres 11+ should we have a polling time greater than the timeout.
        if (connection.SupportsNotifications())
        {
          storage.Options.QueuePollInterval = TimeSpan.FromMinutes(2);
        }

        PostgreSqlJobQueue queue = CreateJobQueue(storage, false, true);
        IFetchedJob job = null;
        //as UseConnection does not support async-await we have to work with Thread.Sleep

        Task task = Task.Run(() => {
          //dequeue the job asynchronously
          CancellationTokenSource cancellationTokenSource = new(timeout);
          try
          {
            job = queue.Dequeue(new[] { "default" }, cancellationTokenSource.Token);
          }
          catch (OperationCanceledException)
          {
            // Do nothing, task was intentionally cancelled.
          }
          finally
          {
            cancellationTokenSource.Dispose();
          }
        });

        Thread.Sleep(2000); // Give thread time to startup.

        queue.Enqueue(connection, "default", "1");

        task.Wait(timeout);

        Assert.NotNull(job);
      });
    }

    private void Enqueue_AddsAJobToTheQueue(bool useNativeDatabaseTransactions)
    {
      UseConnection((connection, storage) => {
        PostgreSqlJobQueue queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

        queue.Enqueue(connection, "default", "1");

        dynamic record = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""jobqueue""").Single();
        Assert.Equal("1", record.jobid.ToString());
        Assert.Equal("default", record.queue);
        Assert.Null(record.FetchedAt);
      });
    }

    private static CancellationToken CreateTimingOutCancellationToken()
    {
      CancellationTokenSource source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
      return source.Token;
    }

#pragma warning disable xUnit1013 // Public method should be marked as test
    public static void Sample(string arg1, string arg2)
#pragma warning restore xUnit1013 // Public method should be marked as test
    { }

    private static PostgreSqlJobQueue CreateJobQueue(PostgreSqlStorage storage, bool useNativeDatabaseTransactions, bool enableLongPolling = false, bool useSlidingInvisibilityTimeout = false)
    {
      storage.Options.SchemaName = GetSchemaName();
      storage.Options.UseNativeDatabaseTransactions = useNativeDatabaseTransactions;
      storage.Options.EnableLongPolling = enableLongPolling;
      storage.Options.UseSlidingInvisibilityTimeout = useSlidingInvisibilityTimeout;

      return new PostgreSqlJobQueue(storage);
    }

    private void UseConnection(Action<IDbConnection, PostgreSqlStorage> action)
    {
      PostgreSqlStorage storage = _fixture.SafeInit();
      storage.UseConnection(null, connection => {
        action(connection, storage);

        return true;
      });
    }

    private static string GetSchemaName()
    {
      return ConnectionUtils.GetSchemaName();
    }
  }
}
