using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class ExpirationManagerFacts
    {
        private readonly CancellationToken _token;
        private readonly PostgreSqlStorageOptions _options;

        public ExpirationManagerFacts()
        {
            var cts = new CancellationTokenSource();
            _token = cts.Token;
            _options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                EnableTransactionScopeEnlistment = true
            };
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new ExpirationManager(null, _options));
        }

        [Fact, CleanDatabase]
        public void Execute_RemovesOutdatedRecords()
        {
            using (var connection = CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, DateTime.UtcNow.AddMonths(-1));
                var manager = CreateManager(connection);

                manager.Execute(_token);

                Assert.True(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            using (var connection = CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, null);
                var manager = CreateManager(connection);

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            using (var connection = CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, DateTime.Now.AddMonths(1));
                var manager = CreateManager(connection);

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_CounterTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                string createSql = @"
insert into """ + GetSchemaName() + @""".""counter"" (""key"", ""value"", ""expireat"") 
values ('key', 1, @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""counter""").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Aggregates_CounterTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                string createSql = $@"
insert into ""{GetSchemaName()}"".""counter"" (""key"", ""value"") 
values ('stats:succeeded', 1)";
                for (int i = 0; i < 5; i++)
                {
                    connection.Execute(createSql);
                }

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(1, connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""counter""").Single());
                Assert.Equal(5, connection.Query<long>(@"select sum(value) from """ + GetSchemaName() + @""".""counter""").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_JobTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                string createSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"", ""expireat"") 
values ('', '', now() at time zone 'utc', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""job""").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_ListTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                string createSql = @"
insert into """ + GetSchemaName() + @""".""list"" (""key"", ""expireat"") 
values ('key', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_SetTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                string createSql = @"
insert into """ + GetSchemaName() + @""".""set"" (""key"", ""score"", ""value"", ""expireat"") 
values ('key', 0, '', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""set""").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_HashTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                string createSql = @"
insert into """ + GetSchemaName() + @""".""hash"" (""key"", ""field"", ""value"", ""expireat"") 
values ('key', 'field', '', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""hash""").Single());
            }
        }

        private static long CreateExpirationEntry(NpgsqlConnection connection, DateTime? expireAt)
        {
            string insertSqlNull = @"
insert into """ + GetSchemaName() + @""".""counter""(""key"", ""value"", ""expireat"")
values ('key', 1, null) returning ""id""";

            string insertSqlValue = @"
insert into """ + GetSchemaName() + @""".""counter""(""key"", ""value"", ""expireat"")
values ('key', 1, now() at time zone 'utc' - interval '{0} seconds') returning ""id""";

            string insertSql = expireAt == null
                ? insertSqlNull
                : string.Format(insertSqlValue,
                    ((long)(DateTime.UtcNow - expireAt.Value).TotalSeconds).ToString(CultureInfo.InvariantCulture));

            var id = connection.Query(insertSql).Single();
            var recordId = (long)id.id;
            return recordId;
        }

        private static bool IsEntryExpired(NpgsqlConnection connection, long entryId)
        {
            var count = connection.Query<long>(
                @"select count(*) from """ + GetSchemaName() + @""".""counter"" where ""id"" = @id", new { id = entryId }).Single();
            return count == 0;
        }

        private NpgsqlConnection CreateConnection()
        {
            return ConnectionUtils.CreateConnection();
        }

        private static string GetSchemaName()
        {
            return ConnectionUtils.GetSchemaName();
        }


        private ExpirationManager CreateManager(NpgsqlConnection connection)
        {
            var storage = new PostgreSqlStorage(connection, _options);
            return new ExpirationManager(storage, _options, TimeSpan.Zero);
        }
    }
}