using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class ExpirationManagerFacts
    {
        private readonly PostgreSqlStorage _storage;
        private readonly CancellationToken _token;

        public ExpirationManagerFacts()
        {
            _storage = new PostgreSqlStorage(ConnectionUtils.GetConnectionString());
            _token = new CancellationToken(true);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new ExpirationManager(null));
        }

        [Fact, CleanDatabase]
        public void Execute_RemovesOutdatedRecords()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, DateTime.UtcNow.AddMonths(-1));
                var manager = CreateManager();

                manager.Execute(_token);

                Assert.True(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, null);
                var manager = CreateManager();

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, DateTime.Now.AddMonths(1));
                var manager = CreateManager();

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_CounterTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                const string createSql = @"
insert into ""hangfire"".""counter"" (""key"", ""value"", ""expireat"") 
values ('key', 1, @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from ""hangfire"".""counter""").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_JobTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                const string createSql = @"
insert into ""hangfire"".""job"" (""invocationdata"", ""arguments"", ""createdat"", ""expireat"") 
values ('', '', now() at time zone 'utc', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from ""hangfire"".""job""").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_ListTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                const string createSql = @"
insert into ""hangfire"".""list"" (""key"", ""expireat"") 
values ('key', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from ""hangfire"".""list""").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_SetTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                const string createSql = @"
insert into ""hangfire"".""set"" (""key"", ""score"", ""value"", ""expireat"") 
values ('key', 0, '', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from ""hangfire"".""set""").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_HashTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                const string createSql = @"
insert into ""hangfire"".""hash"" (""key"", ""field"", ""value"", ""expireat"") 
values ('key', 'field', '', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from ""hangfire"".""hash""").Single());
            }
        }

        private static int CreateExpirationEntry(IDbConnection connection, DateTime? expireAt)
        {
            const string insertSqlNull = @"
insert into ""hangfire"".""counter""(""key"", ""value"", ""expireat"")
values ('key', 1, null) returning ""id""";

            const string insertSqlValue = @"
insert into ""hangfire"".""counter""(""key"", ""value"", ""expireat"")
values ('key', 1, now() at time zone 'utc' - interval '{0} seconds') returning ""id""";

            string insertSql = expireAt == null ? insertSqlNull : string.Format(insertSqlValue, ((long)(DateTime.UtcNow - expireAt.Value).TotalSeconds).ToString(CultureInfo.InvariantCulture));

            var id = connection.Query(insertSql).Single();
            var recordId = (int) id.id;
            return recordId;
        }

        private static bool IsEntryExpired(IDbConnection connection, int entryId)
        {
            var count = connection.Query<long>(
                    @"select count(*) from ""hangfire"".""counter"" where ""id"" = @id", new { id = entryId }).Single();
            return count == 0;
        }

        private ExpirationManager CreateManager()
        {
            return new ExpirationManager(_storage);
        }
    }
}
