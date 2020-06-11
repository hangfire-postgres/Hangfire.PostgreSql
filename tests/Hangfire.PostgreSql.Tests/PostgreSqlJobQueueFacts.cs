using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
	public class PostgreSqlJobQueueFacts
	{
		private static readonly string[] DefaultQueues = {"default"};

		[Fact]
		public void Ctor_ThrowsAnException_WhenConnectionIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new PostgreSqlJobQueue(null, new PostgreSqlStorageOptions()));

			Assert.Equal("storage", exception.ParamName);
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new PostgreSqlJobQueue(new PostgreSqlStorage(ConnectionUtils.GetConnectionString()), null));

			Assert.Equal("options", exception.ParamName);
		}

		[Fact, CleanDatabase]
		public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
		{
			UseConnection((connection, storage) =>
			{
				var queue = CreateJobQueue(storage, false);

				var exception = Assert.Throws<ArgumentNullException>(
					() => queue.Dequeue(null, CreateTimingOutCancellationToken()));

				Assert.Equal("queues", exception.ParamName);
			});
		}

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchAJob_FromQueueWithHigherPriority()
        {
            UseConnection((connection, storage) =>
            {
                var queue = CreateJobQueue(storage, false);
                var token = CreateTimingOutCancellationToken();

                queue.Enqueue("1", "1");
                queue.Enqueue("2", "2");
                queue.Enqueue("3", "3");

                Assert.Equal("1", queue.Dequeue(new[] { "1", "2", "3" }, token).JobId);
                Assert.Equal("2", queue.Dequeue(new[] { "2", "3", "1" }, token).JobId);
                Assert.Equal("3", queue.Dequeue(new[] { "3", "1", "2" }, token).JobId);
            });
        }

        [Fact, CleanDatabase]
		private void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty_WithUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty(true);
		}

		[Fact, CleanDatabase]
		private void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty_WithoutUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty(false);
		}

		private void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty(bool useNativeDatabaseTransactions)
		{
			UseConnection((connection, storage) =>
			{
				var queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

				var exception = Assert.Throws<ArgumentException>(
					() => queue.Dequeue(new string[0], CreateTimingOutCancellationToken()));

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
			UseConnection((connection, storage) =>
			{
				var cts = new CancellationTokenSource();
				cts.Cancel();
				var queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

				Assert.Throws<OperationCanceledException>(
					() => queue.Dequeue(DefaultQueues, cts.Token));
			});
		}

		[Fact, CleanDatabase]
		public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs_WithUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs(true);
		}

		[Fact, CleanDatabase]
		public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs_WithoutUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs(false);
		}

		private void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs(bool useNativeDatabaseTransactions)
		{
			UseConnection((connection, storage) =>
			{
				var cts = new CancellationTokenSource(200);
				var queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

				Assert.Throws<OperationCanceledException>(
					() => queue.Dequeue(DefaultQueues, cts.Token));
			});
		}

		[Fact, CleanDatabase]
		public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue_WithUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue(true);
		}

		[Fact, CleanDatabase]
		public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue_WithoutUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue(false);
		}

		private void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue(bool useNativeDatabaseTransactions)
		{
			string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"")
values (@jobId, @queue) returning ""id""";

			// Arrange
			UseConnection((connection, storage) =>
			{
				var id = (long) connection.Query(
					arrangeSql,
					new {jobId = 1, queue = "default"}).Single().id;
				var queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

				// Act
				var payload = (PostgreSqlFetchedJob) queue.Dequeue(
					DefaultQueues,
					CreateTimingOutCancellationToken());

				// Assert
				Assert.Equal(id, payload.Id);
				Assert.Equal("1", payload.JobId);
				Assert.Equal("default", payload.Queue);
			});
		}

		[Fact, CleanDatabase]
		public void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue_WithUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue(true);
		}

		[Fact, CleanDatabase]
		public void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue_WithoutUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue(false);
		}

		private void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue(bool useNativeDatabaseTransactions)
		{
			string arrangeSql = @"
WITH i AS (
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values (@invocationData, @arguments, now() at time zone 'utc')
RETURNING ""id"")
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"")
select i.""id"", @queue from i;
";

			// Arrange
			UseConnection((connection, storage) =>
			{
				connection.Execute(
					arrangeSql,
					new {invocationData = "", arguments = "", queue = "default"});
				var queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

				// Act
				var payload = queue.Dequeue(
					DefaultQueues,
					CreateTimingOutCancellationToken());

				// Assert
				Assert.NotNull(payload);

				var fetchedAt = connection.Query<DateTime?>(
					@"select ""fetchedat"" from """ + GetSchemaName() + @""".""jobqueue"" where ""jobid"" = @id",
					new {id = Convert.ToInt32(payload.JobId, CultureInfo.InvariantCulture)}).Single();

				Assert.NotNull(fetchedAt);
				Assert.True(fetchedAt > DateTime.UtcNow.AddMinutes(-1));
			});
		}

		[Fact, CleanDatabase]
		public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue_WithUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue(true);
		}

		[Fact, CleanDatabase]
		public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue_WithoutUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue(false);
		}

		private void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue(bool useNativeDatabaseTransactions)
		{
			string arrangeSql = @"
WITH i AS (
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values (@invocationData, @arguments, now() at time zone 'utc')
RETURNING ""id"")
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"", ""fetchedat"")
select i.""id"", @queue, @fetchedAt from i;
";


			// Arrange
			UseConnection((connection, storage) =>
			{
				connection.Execute(
					arrangeSql,
					new
					{
						queue = "default",
						fetchedAt = DateTime.UtcNow.AddDays(-1),
						invocationData = "",
						arguments = ""
					});
				var queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

				// Act
				var payload = queue.Dequeue(
					DefaultQueues,
					CreateTimingOutCancellationToken());

				// Assert
				Assert.NotEmpty(payload.JobId);
			});
		}

		[Fact, CleanDatabase]
		public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob_WithUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob(true);
		}

		[Fact, CleanDatabase]
		public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob_WithoutUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob(false);
		}

		private void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob(bool useNativeDatabaseTransactions)
		{
			string arrangeSql = @"
WITH i AS (
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values (@invocationData, @arguments, now() at time zone 'utc')
RETURNING ""id"")
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"")
select i.""id"", @queue from i;
";

			UseConnection((connection, storage) =>
			{
				connection.Execute(
					arrangeSql,
					new[]
					{
						new {queue = "default", invocationData = "", arguments = ""},
						new {queue = "default", invocationData = "", arguments = ""}
					});
				var queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

				// Act
				var payload = queue.Dequeue(
					DefaultQueues,
					CreateTimingOutCancellationToken());

				// Assert
				var otherJobFetchedAt = connection.Query<DateTime?>(
					@"select ""fetchedat"" from """ + GetSchemaName() + @""".""jobqueue"" where ""jobid"" <> @id",
					new {id = Convert.ToInt32(payload.JobId, CultureInfo.InvariantCulture)}).Single();

				Assert.Null(otherJobFetchedAt);
			});
		}

		[Fact, CleanDatabase]
		public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues_WithUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues(true);
		}

		[Fact, CleanDatabase]
		public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues_WithoutUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues(false);
		}


		private void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues(bool useNativeDatabaseTransactions)
		{
			string arrangeSql = @"
WITH i AS (
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values (@invocationData, @arguments, now() at time zone 'utc')
RETURNING ""id"")
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"")
select i.""id"", @queue from i;
";
			UseConnection((connection, storage) =>
			{
				var queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

				connection.Execute(
					arrangeSql,
					new {queue = "critical", invocationData = "", arguments = ""});

				Assert.Throws<OperationCanceledException>(
					() => queue.Dequeue(
						DefaultQueues,
						CreateTimingOutCancellationToken()));
			});
		}

		[Fact, CleanDatabase]
		private void Dequeue_ShouldFetchJobs_FromMultipleQueues_WithUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldFetchJobs_FromMultipleQueues(true);
		}

		[Fact, CleanDatabase]
		private void Dequeue_ShouldFetchJobs_FromMultipleQueues_WithoutUseNativeDatabaseTransactions()
		{
			Dequeue_ShouldFetchJobs_FromMultipleQueues(false);
		}

		private void Dequeue_ShouldFetchJobs_FromMultipleQueues(bool useNativeDatabaseTransactions)
		{
			string arrangeSql = @"
WITH i AS (
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values (@invocationData, @arguments, now() at time zone 'utc')
returning ""id"")
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"")
select i.""id"", @queue from i;
";

			var queueNames = new[] {"default", "critical"};

			UseConnection((connection, storage) =>
			{
				connection.Execute(
					arrangeSql,
					new[]
					{
						new {queue = queueNames.First(), invocationData = "", arguments = ""},
						new {queue = queueNames.Last(), invocationData = "", arguments = ""}
					});

				var queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

				var queueFirst = (PostgreSqlFetchedJob) queue.Dequeue(
					queueNames,
					CreateTimingOutCancellationToken());

				Assert.NotNull(queueFirst.JobId);
				Assert.Contains(queueFirst.Queue, queueNames);

				var queueLast = (PostgreSqlFetchedJob) queue.Dequeue(
					queueNames,
					CreateTimingOutCancellationToken());

				Assert.NotNull(queueLast.JobId);
				Assert.Contains(queueLast.Queue, queueNames);
			});
		}

		[Fact, CleanDatabase]
		public void Enqueue_AddsAJobToTheQueue_WithUseNativeDatabaseTransactions()
		{
			Enqueue_AddsAJobToTheQueue(true);
		}

		[Fact, CleanDatabase]
		public void Enqueue_AddsAJobToTheQueue_WithoutUseNativeDatabaseTransactions()
		{
			Enqueue_AddsAJobToTheQueue(false);
		}

		[Fact, CleanDatabase]
		public void Queues_Should_Support_Long_Queue_Names()
		{
			UseConnection((connection, storage) =>
			{
				var queue = CreateJobQueue(storage, false);

				var name = "very_long_name_that_is_over_20_characters_long_or_something";

				Assert.True(name.Length > 21);

				queue.Enqueue(name, "1");

				var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""jobqueue""").Single();
				Assert.Equal(name, record.queue.ToString());
			});
		}

		private void Enqueue_AddsAJobToTheQueue(bool useNativeDatabaseTransactions)
		{
			UseConnection((connection, storage) =>
			{
				var queue = CreateJobQueue(storage, useNativeDatabaseTransactions);

				queue.Enqueue("default", "1");

				var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""jobqueue""").Single();
				Assert.Equal("1", record.jobid.ToString());
				Assert.Equal("default", record.queue);
				Assert.Null(record.FetchedAt);
			});
		}

		private static CancellationToken CreateTimingOutCancellationToken()
		{
			var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
			return source.Token;
		}

#pragma warning disable xUnit1013 // Public method should be marked as test
		public static void Sample(string arg1, string arg2)
#pragma warning restore xUnit1013 // Public method should be marked as test
		{
		}

		private static PostgreSqlJobQueue CreateJobQueue(PostgreSqlStorage storage, bool useNativeDatabaseTransactions)
		{
			return new PostgreSqlJobQueue(storage, new PostgreSqlStorageOptions()
			{
				SchemaName = GetSchemaName(),
				UseNativeDatabaseTransactions = useNativeDatabaseTransactions
			});
		}

		private static void UseConnection(Action<IDbConnection, PostgreSqlStorage> action)
		{
			var storage = new PostgreSqlStorage(ConnectionUtils.GetConnectionString());

			storage.UseConnection(connection =>
			{
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