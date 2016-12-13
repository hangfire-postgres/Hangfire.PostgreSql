using System;
using System.Data;
using System.Linq;
using System.Threading;
using Dapper;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlDistributedLockFacts
    {
        private readonly TimeSpan _timeout = TimeSpan.FromSeconds(5);

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNullOrEmpty()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions();

            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlDistributedLock("", _timeout, new Mock<IDbConnection>().Object, options));

            Assert.Equal("resource", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions();

            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlDistributedLock("hello", _timeout, null, options));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlDistributedLock("hi", _timeout, new Mock<IDbConnection>().Object, null));

            Assert.Equal("options", exception.ParamName);
        }


        [Fact, CleanDatabase]
        public void Ctor_AcquiresExclusiveApplicationLock_WithUseNativeDatabaseTransactions_OnSession()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = true
            };

            UseConnection(connection =>
            {
                // ReSharper disable once UnusedVariable
                var distributedLock = new PostgreSqlDistributedLock("hello", _timeout, connection, options);

                var lockCount = connection.Query<long>(
                    @"select count(*) from """ + GetSchemaName() + @""".""lock"" where ""resource"" = @resource",
                    new { resource = "hello" }).Single();

                Assert.Equal(lockCount, 1);
                //Assert.Equal("Exclusive", lockMode);
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_AcquiresExclusiveApplicationLock_WithUseNativeDatabaseTransactions_OnSession_WhenDeadlockIsOccured()
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
                connection.Execute($@"INSERT INTO ""{GetSchemaName()}"".""lock"" VALUES ('{resourceName}', 0, '{DateTime.UtcNow}')");

                // Act
                var distributedLock = new PostgreSqlDistributedLock(resourceName, timeout, connection, options);

                // Assert
                Assert.True(distributedLock != null);
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_AcquiresExclusiveApplicationLock_WithoutUseNativeDatabaseTransactions_OnSession()
        {
            PostgreSqlStorageOptions options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                UseNativeDatabaseTransactions = false
            };

            UseConnection(connection =>
            {
                // ReSharper disable once UnusedVariable
                var distributedLock = new PostgreSqlDistributedLock("hello", _timeout, connection, options);

                var lockCount = connection.Query<long>(
                    @"select count(*) from """ + GetSchemaName() + @""".""lock"" where ""resource"" = @resource",
                    new { resource = "hello" }).Single();

                Assert.Equal(lockCount, 1);
                //Assert.Equal("Exclusive", lockMode);
            });
        }


        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfLockCanNotBeGranted_WithUseNativeDatabaseTransactions()
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
                    using (new PostgreSqlDistributedLock("exclusive", _timeout, connection1, options))
                    {
                        lockAcquired.Set();
                        releaseLock.Wait();
                    }
                }));
            thread.Start();

            lockAcquired.Wait();

            UseConnection(connection2 =>
                Assert.Throws<PostgreSqlDistributedLockException>(
                    () => new PostgreSqlDistributedLock("exclusive", _timeout, connection2, options)));

            releaseLock.Set();
            thread.Join();
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfLockCanNotBeGranted_WithoutUseNativeDatabaseTransactions()
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
                    using (new PostgreSqlDistributedLock("exclusive", _timeout, connection1, options))
                    {
                        lockAcquired.Set();
                        releaseLock.Wait();
                    }
                }));
            thread.Start();

            lockAcquired.Wait();

            UseConnection(connection2 =>
                Assert.Throws<PostgreSqlDistributedLockException>(
                    () => new PostgreSqlDistributedLock("exclusive", _timeout, connection2, options)));

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
                var distributedLock = new PostgreSqlDistributedLock("hello", _timeout, connection, options);
                distributedLock.Dispose();

                var lockCount = connection.Query<long>(
                    @"select count(*) from """ + GetSchemaName() + @""".""lock"" where ""resource"" = @resource",
                    new { resource = "hello" }).Single();

                Assert.Equal(lockCount, 0);
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
                var distributedLock = new PostgreSqlDistributedLock("hello", _timeout, connection, options);
                distributedLock.Dispose();

                var lockCount = connection.Query<long>(
                    @"select count(*) from """ + GetSchemaName() + @""".""lock"" where ""resource"" = @resource",
                    new { resource = "hello" }).Single();

                Assert.Equal(lockCount, 0);
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