using System;
using System.Data;
using System.Globalization;
using System.Linq;
using Dapper;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
	public class PostgreSqlFetchedJobFacts
	{
		private const string JobId = "id";
		private const string Queue = "queue";

		private readonly PostgreSqlStorageOptions _options;
		private PostgreSqlStorage _storage;

		public PostgreSqlFetchedJobFacts()
		{
			_options = new PostgreSqlStorageOptions();
			_storage = new PostgreSqlStorage(ConnectionUtils.GetConnectionString());
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenConnectionIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new PostgreSqlFetchedJob(null, new PostgreSqlStorageOptions(), 1, JobId, Queue));

			Assert.Equal("storage", exception.ParamName);
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenOptionsIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new PostgreSqlFetchedJob(_storage, null, 1, JobId, Queue));

			Assert.Equal("options", exception.ParamName);
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenJobIdIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new PostgreSqlFetchedJob(_storage, _options, 1, null, Queue));

			Assert.Equal("jobId", exception.ParamName);
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenQueueIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new PostgreSqlFetchedJob(_storage, _options, 1, JobId, null));

			Assert.Equal("queue", exception.ParamName);
		}

		[Fact]
		public void Ctor_CorrectlySets_AllInstanceProperties()
		{
			var fetchedJob = new PostgreSqlFetchedJob(_storage, _options, 1, JobId, Queue);

			Assert.Equal(1, fetchedJob.Id);
			Assert.Equal(JobId, fetchedJob.JobId);
			Assert.Equal(Queue, fetchedJob.Queue);
		}

		[Fact, CleanDatabase]
		public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
		{
			// Arrange
			var id = CreateJobQueueRecord(_storage, "1", "default");
			var processingJob = new PostgreSqlFetchedJob(_storage, _options, id, "1", "default");

			// Act
			processingJob.RemoveFromQueue();

			// Assert
			var count = _storage.UseConnection(connection =>
				connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""jobqueue""").Single());
			Assert.Equal(0, count);
		}

		[Fact, CleanDatabase]
		public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
		{
			// Arrange
			CreateJobQueueRecord(_storage, "1", "default");
			CreateJobQueueRecord(_storage, "1", "critical");
			CreateJobQueueRecord(_storage, "2", "default");

			var fetchedJob = new PostgreSqlFetchedJob(_storage, _options, 999, "1", "default");

			// Act
			fetchedJob.RemoveFromQueue();

			// Assert
			var count = _storage.UseConnection(connection =>
				connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""jobqueue""").Single());
			Assert.Equal(3, count);
		}

		[Fact, CleanDatabase]
		public void Requeue_SetsFetchedAtValueToNull()
		{
			// Arrange
			var id = CreateJobQueueRecord(_storage, "1", "default");
			var processingJob = new PostgreSqlFetchedJob(_storage, _options, id, "1", "default");

			// Act
			processingJob.Requeue();

			// Assert
			var record = _storage.UseConnection(connection =>
				connection.Query(@"select * from """ + GetSchemaName() + @""".""jobqueue""").Single());
			Assert.Null(record.FetchedAt);
		}

		[Fact, CleanDatabase]
		public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
		{
			// Arrange
			var id = CreateJobQueueRecord(_storage, "1", "default");
			var processingJob = new PostgreSqlFetchedJob(_storage, _options, id, "1", "default");

			// Act
			processingJob.Dispose();

			// Assert
			var record = _storage.UseConnection(connection =>
				connection.Query(@"select * from """ + GetSchemaName() + @""".""jobqueue""").Single());
			Assert.Null(record.fetchedat);
		}

		private static long CreateJobQueueRecord(PostgreSqlStorage storage, string jobId, string queue)
		{
			string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"", ""fetchedat"")
values (@id, @queue, now() at time zone 'utc') returning ""id""";

			return
				(long)
					storage.UseConnection(connection => connection.Query(arrangeSql, new {id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture), queue = queue})
						.Single()
						.id);
		}

		private static string GetSchemaName()
		{
			return ConnectionUtils.GetSchemaName();
		}
	}
}