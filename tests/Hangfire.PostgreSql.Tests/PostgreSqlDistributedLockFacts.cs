using Dapper;
using Moq;
using Npgsql;
using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlDistributedLockFacts
    {
        private readonly TimeSpan _timeout = TimeSpan.FromSeconds(5);

        [Fact]
        public void Acquire_ThrowsAnException_WhenResourceIsNullOrEmpty()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions();

            var exception = Assert.Throws<ArgumentNullException>(
                () => PostgreSqlDistributedLock.Acquire(new Mock<IDbConnection>().Object, "", _timeout, options));

            Assert.Equal("resource", exception.ParamName);
        }

        [Fact]
        public void Acquire_ThrowsAnException_WhenConnectionIsNull()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions();

            var exception = Assert.Throws<ArgumentNullException>(
                () => PostgreSqlDistributedLock.Acquire(null, "hello", _timeout, options));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Acquire_ThrowsAnException_WhenOptionsIsNull()
        {
            var connection = new Mock<IDbConnection>();
            connection.SetupGet(c => c.State).Returns(ConnectionState.Open);
            var exception = Assert.Throws<ArgumentNullException>(
                () => PostgreSqlDistributedLock.Acquire(new Mock<IDbConnection>().Object, "hi", _timeout, null));

            Assert.Equal("options", exception.ParamName);
        }


        [Fact, CleanDatabase]
        public void Acquire_AcquiresExclusiveApplicationLock_WithUseNativeDatabaseTransactions_OnSession()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = true
            };

            UseConnection(connection =>
            {
                // ReSharper disable once UnusedVariable
                PostgreSqlDistributedLock.Acquire(connection, "hello", _timeout, options);

                var lockCount = connection.Query<long>(
                    @"select count(*) from """ + GetSchemaName() + @""".""lock"" where ""resource"" = @resource",
                    new { resource = "hello" }).Single();

                Assert.Equal(1, lockCount);
                //Assert.Equal("Exclusive", lockMode);
            });
        }

        [Fact, CleanDatabase]
        public void Acquire_AcquiresExclusiveApplicationLock_WithUseNativeDatabaseTransactions_OnSession_WhenDeadlockIsOccured()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = true,
                DistributedLockTimeout = TimeSpan.FromSeconds(10)
            };

            UseConnection(connection =>
            {
                // Arrange
                var timeout = TimeSpan.FromSeconds(15);
                var resourceName = "hello";
                var dateTimeNow = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                connection.Execute($@"INSERT INTO ""{GetSchemaName()}"".""lock"" VALUES ('{resourceName}', 0, '{dateTimeNow}')");

                // Act && Assert (not throwing means it worked)
                PostgreSqlDistributedLock.Acquire(connection, resourceName, timeout, options);
            });
        }

        [Fact, CleanDatabase]
        public void Acquire_AcquiresExclusiveApplicationLock_WithoutUseNativeDatabaseTransactions_OnSession()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = false
            };

            UseConnection(connection =>
            {
                // ReSharper disable UnusedVariable

                // Acquire locks on two different resources to make sure they don't conflict.
                PostgreSqlDistributedLock.Acquire(connection, "hello", _timeout, options);
                PostgreSqlDistributedLock.Acquire(connection, "hello2", _timeout, options);

                // ReSharper restore UnusedVariable

                var lockCount = connection.Query<long>(
                    @"select count(*) from """ + GetSchemaName() + @""".""lock"" where ""resource"" = @resource",
                    new { resource = "hello" }).Single();

                Assert.Equal(1, lockCount);
                //Assert.Equal("Exclusive", lockMode);
            });
        }


        [Fact, CleanDatabase]
        public void Acquire_ThrowsAnException_IfLockCanNotBeGranted_WithUseNativeDatabaseTransactions()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = true
            };

            var releaseLock = new ManualResetEventSlim(false);
            var lockAcquired = new ManualResetEventSlim(false);

            var thread = new Thread(
                () => UseConnection(connection1 =>
                {
                    PostgreSqlDistributedLock.Acquire(connection1, "exclusive", _timeout, options);
                    lockAcquired.Set();
                    releaseLock.Wait();
                    PostgreSqlDistributedLock.Release(connection1, "exclusive", options);
                }));
            thread.Start();

            lockAcquired.Wait();

            UseConnection(connection2 =>
                Assert.Throws<PostgreSqlDistributedLockException>(
                    () => PostgreSqlDistributedLock.Acquire(connection2, "exclusive", _timeout, options)));

            releaseLock.Set();
            thread.Join();
        }

        [Fact, CleanDatabase]
        public void Acquire_ThrowsAnException_IfLockCanNotBeGranted_WithoutUseNativeDatabaseTransactions()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = false
            };

            var releaseLock = new ManualResetEventSlim(false);
            var lockAcquired = new ManualResetEventSlim(false);

            var thread = new Thread(
                () => UseConnection(connection1 =>
                {
                    PostgreSqlDistributedLock.Acquire(connection1, "exclusive", _timeout, options);
                    lockAcquired.Set();
                    releaseLock.Wait();
                    PostgreSqlDistributedLock.Release(connection1, "exclusive", options);
                }));
            thread.Start();

            lockAcquired.Wait();

            UseConnection(connection2 =>
                Assert.Throws<PostgreSqlDistributedLockException>(
                    () => PostgreSqlDistributedLock.Acquire(connection2, "exclusive", _timeout, options)));

            releaseLock.Set();
            thread.Join();
        }


        [Fact, CleanDatabase]
        public void Dispose_ReleasesExclusiveApplicationLock_WithUseNativeDatabaseTransactions()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = true
            };

            UseConnection(connection =>
            {
                PostgreSqlDistributedLock.Acquire(connection, "hello", _timeout, options);
                PostgreSqlDistributedLock.Release(connection, "hello", options);

                var lockCount = connection.Query<long>(
                    @"select count(*) from """ + GetSchemaName() + @""".""lock"" where ""resource"" = @resource",
                    new { resource = "hello" }).Single();

                Assert.Equal(0, lockCount);
            });
        }

        [Fact, CleanDatabase]
        public void Dispose_ReleasesExclusiveApplicationLock_WithoutUseNativeDatabaseTransactions()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = false
            };

            UseConnection(connection =>
            {
                PostgreSqlDistributedLock.Acquire(connection, "hello", _timeout, options);
                PostgreSqlDistributedLock.Release(connection, "hello", options);

                var lockCount = connection.Query<long>(
                    @"select count(*) from """ + GetSchemaName() + @""".""lock"" where ""resource"" = @resource",
                    new { resource = "hello" }).Single();

                Assert.Equal(0, lockCount);
            });
        }


        private void UseConnection(Action<NpgsqlConnection> action)
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
