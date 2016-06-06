using System;
using System.Data;
using System.Globalization;
using System.Linq;
using Dapper;
using Moq;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
	public class PostgreSqlFetchedJobFacts
	{
		private const string JobId = "id";
		private const string Queue = "queue";

		private readonly Mock<IDbConnection> _connection;
		private readonly PostgreSqlStorageOptions _options;

		public PostgreSqlFetchedJobFacts()
		{
			_connection = new Mock<IDbConnection>();
			_options = new PostgreSqlStorageOptions()
			{
				SchemaName = GetSchemaName()
			};
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenConnectionIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new PostgreSqlFetchedJob(null, _options, 1, JobId, Queue));

			Assert.Equal("connection", exception.ParamName);
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenOptionsIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new PostgreSqlFetchedJob(_connection.Object, null, 1, JobId, Queue));

			Assert.Equal("options", exception.ParamName);
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenJobIdIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new PostgreSqlFetchedJob(_connection.Object, _options, 1, null, Queue));

			Assert.Equal("jobId", exception.ParamName);
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenQueueIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new PostgreSqlFetchedJob(_connection.Object, _options, 1, JobId, null));

			Assert.Equal("queue", exception.ParamName);
		}

		[Fact]
		public void Ctor_CorrectlySets_AllInstanceProperties()
		{
			var fetchedJob = new PostgreSqlFetchedJob(_connection.Object, _options, 1, JobId, Queue);

			Assert.Equal(1, fetchedJob.Id);
			Assert.Equal(JobId, fetchedJob.JobId);
			Assert.Equal(Queue, fetchedJob.Queue);
		}

		[Fact, CleanDatabase]
		public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
		{
			UseConnection(connection =>
			{
				// Arrange
				var id = CreateJobQueueRecord(connection, "1", "default");
				var processingJob = new PostgreSqlFetchedJob(connection, _options, id, "1", "default");

				// Act
				processingJob.RemoveFromQueue();

				// Assert
				var count = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""jobqueue""").Single();
				Assert.Equal(0, count);
			});
		}

		[Fact, CleanDatabase]
		public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
		{
			UseConnection(connection =>
			{
				// Arrange
				CreateJobQueueRecord(connection, "1", "default");
				CreateJobQueueRecord(connection, "1", "critical");
				CreateJobQueueRecord(connection, "2", "default");

				var fetchedJob = new PostgreSqlFetchedJob(connection, _options, 999, "1", "default");

				// Act
				fetchedJob.RemoveFromQueue();

				// Assert
				var count = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""jobqueue""").Single();
				Assert.Equal(3, count);
			});
		}

		[Fact, CleanDatabase]
		public void Requeue_SetsFetchedAtValueToNull()
		{
			UseConnection(connection =>
			{
				// Arrange
				var id = CreateJobQueueRecord(connection, "1", "default");
				var processingJob = new PostgreSqlFetchedJob(connection, _options, id, "1", "default");

				// Act
				processingJob.Requeue();

				// Assert
				var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""jobqueue""").Single();
				Assert.Null(record.FetchedAt);
			});
		}

		[Fact, CleanDatabase]
		public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
		{
			UseConnection(connection =>
			{
				// Arrange
				var id = CreateJobQueueRecord(connection, "1", "default");
				var processingJob = new PostgreSqlFetchedJob(connection, _options, id, "1", "default");

				// Act
				processingJob.Dispose();

				// Assert
				var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""jobqueue""").Single();
				Assert.Null(record.fetchedat);
			});
		}

		private static int CreateJobQueueRecord(IDbConnection connection, string jobId, string queue)
		{
			string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"", ""fetchedat"")
values (@id, @queue, now() at time zone 'utc') returning ""id""";

			return
				(int)
					connection.Query(arrangeSql, new {id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture), queue = queue})
						.Single()
						.id;
		}

		private static void UseConnection(Action<IDbConnection> action)
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				action(connection);
			}
		}

		private static string GetSchemaName()
		{
			return ConnectionUtils.GetSchemaName();
		}
	}
}