using Dapper;
using Npgsql;
using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class ExpirationManagerFacts : IClassFixture<PostgreSqlStorageFixture>
    {
        private readonly CancellationToken _token;
        private readonly PostgreSqlStorageFixture _fixture;

        public ExpirationManagerFacts(PostgreSqlStorageFixture fixture)
        {
            var cts = new CancellationTokenSource();
            _token = cts.Token;
            _fixture = fixture;
            _fixture.SetupOptions(o => o.DeleteExpiredBatchSize = 2);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new ExpirationManager(null));
        }

        [Fact, CleanDatabase]
        public void Execute_RemovesOutdatedRecords()
        {
            UseConnection((connection, manager) =>
            {
                long CreateEntry(string key) =>
                    CreateExpirationEntry(connection, DateTime.UtcNow.AddMonths(-1), key);

                var entryIds = Enumerable.Range(1, 3).Select(i => CreateEntry($"key{i}")).ToList();

                manager.Execute(_token);

                entryIds.ForEach(entryId => Assert.True(IsEntryExpired(connection, entryId)));
            });
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            UseConnection((connection, manager) =>
            {
                var entryId = CreateExpirationEntry(connection, null);

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            });
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            UseConnection((connection, manager) =>
            {
                var entryId = CreateExpirationEntry(connection, DateTime.Now.AddMonths(1));

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            });
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_CounterTable()
        {
            UseConnection((connection, manager) =>
            {
                // Arrange
                string createSql = @"
insert into """ + GetSchemaName() + @""".""counter"" (""key"", ""value"", ""expireat"") 
values ('key', 1, @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""counter""").Single());
            });
        }

        [Fact, CleanDatabase]
        public void Execute_Aggregates_CounterTable()
        {
            UseConnection((connection, manager) =>
            {
                // Arrange
                string createSql = $@"
insert into ""{GetSchemaName()}"".""counter"" (""key"", ""value"") 
values ('stats:succeeded', 1)";
                for (int i = 0; i < 5; i++)
                {
                    connection.Execute(createSql);
                }

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(1, connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""counter""").Single());
                Assert.Equal(5, connection.Query<long>(@"select sum(value) from """ + GetSchemaName() + @""".""counter""").Single());
            });
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_JobTable()
        {
            UseConnection((connection, manager) =>
            {
                // Arrange
                string createSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"", ""expireat"") 
values ('', '', now() at time zone 'utc', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""job""").Single());
            });
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_ListTable()
        {
            UseConnection((connection, manager) =>
            {
                // Arrange
                string createSql = @"
insert into """ + GetSchemaName() + @""".""list"" (""key"", ""expireat"") 
values ('key', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""").Single());
            });
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_SetTable()
        {
            UseConnection((connection, manager) =>
            {
                // Arrange
                string createSql = @"
insert into """ + GetSchemaName() + @""".""set"" (""key"", ""score"", ""value"", ""expireat"") 
values ('key', 0, '', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""set""").Single());
            });
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_HashTable()
        {
            UseConnection((connection, manager) =>
            {
                // Arrange
                string createSql = @"
insert into """ + GetSchemaName() + @""".""hash"" (""key"", ""field"", ""value"", ""expireat"") 
values ('key', 'field', '', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""hash""").Single());
            });
        }

        private static long CreateExpirationEntry(NpgsqlConnection connection, DateTime? expireAt, string key = "key")
        {
            string insertSqlNull = @"
insert into """ + GetSchemaName() + $@""".""counter""(""key"", ""value"", ""expireat"")
values ('{key}', 1, null) returning ""id""";

            string insertSqlValue = @"
insert into """ + GetSchemaName() + $@""".""counter""(""key"", ""value"", ""expireat"")
values ('{key}', 1, now() at time zone 'utc' - interval '{{0}} seconds') returning ""id""";

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

        private void UseConnection(Action<NpgsqlConnection, ExpirationManager> action)
        {
            var storage = _fixture.SafeInit();
            var manager = new ExpirationManager(storage, TimeSpan.Zero);
            action(storage.CreateAndOpenConnection(), manager);
        }

        private static string GetSchemaName()
        {
            return ConnectionUtils.GetSchemaName();
        }
    }
}