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

        public PostgreSqlFetchedJobFacts()
        {
            _connection = new Mock<IDbConnection>();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlFetchedJob(null, 1, JobId, Queue));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlFetchedJob(_connection.Object, 1, null, Queue));

            Assert.Equal("jobId", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlFetchedJob(_connection.Object, 1, JobId, null));

            Assert.Equal("queue", exception.ParamName);
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            var fetchedJob = new PostgreSqlFetchedJob(_connection.Object, 1, JobId, Queue);

            Assert.Equal(1, fetchedJob.Id);
            Assert.Equal(JobId, fetchedJob.JobId);
            Assert.Equal(Queue, fetchedJob.Queue);
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
        {
            UseConnection(sql =>
            {
                // Arrange
                var id = CreateJobQueueRecord(sql, "1", "default");
                var processingJob = new PostgreSqlFetchedJob(sql, id, "1", "default");

                // Act
                processingJob.RemoveFromQueue();

                // Assert
                var count = sql.Query<long>(@"select count(*) from ""hangfire"".""jobqueue""").Single();
                Assert.Equal(0, count);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
        {
            UseConnection(sql =>
            {
                // Arrange
                CreateJobQueueRecord(sql, "1", "default");
                CreateJobQueueRecord(sql, "1", "critical");
                CreateJobQueueRecord(sql, "2", "default");

                var fetchedJob = new PostgreSqlFetchedJob(sql, 999, "1", "default");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                var count = sql.Query<long>(@"select count(*) from ""hangfire"".""jobqueue""").Single();
                Assert.Equal(3, count);
            });
        }

        [Fact, CleanDatabase]
        public void Requeue_SetsFetchedAtValueToNull()
        {
            UseConnection(sql =>
            {
                // Arrange
                var id = CreateJobQueueRecord(sql, "1", "default");
                var processingJob = new PostgreSqlFetchedJob(sql, id, "1", "default");

                // Act
                processingJob.Requeue();

                // Assert
                var record = sql.Query(@"select * from ""hangfire"".""jobqueue""").Single();
                Assert.Null(record.FetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
        {
            UseConnection(sql =>
            {
                // Arrange
                var id = CreateJobQueueRecord(sql, "1", "default");
                var processingJob = new PostgreSqlFetchedJob(sql, id, "1", "default");

                // Act
                processingJob.Dispose();

                // Assert
                var record = sql.Query(@"select * from ""hangfire"".""jobqueue""").Single();
                Assert.Null(record.fetchedat);
            });
        }

        private static int CreateJobQueueRecord(IDbConnection connection, string jobId, string queue)
        {
            const string arrangeSql = @"
insert into ""hangfire"".""jobqueue"" (""jobid"", ""queue"", ""fetchedat"")
values (@id, @queue, now() at time zone 'utc') returning ""id""";

            return (int)connection.Query(arrangeSql, new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture), queue = queue }).Single().id;
        }

        private static void UseConnection(Action<IDbConnection> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
}
