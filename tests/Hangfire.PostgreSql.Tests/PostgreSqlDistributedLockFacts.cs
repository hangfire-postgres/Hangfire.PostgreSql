using System;
using System.Data;
using System.Data.SqlClient;
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
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlDistributedLock("", _timeout, new Mock<IDbConnection>().Object));

            Assert.Equal("resource", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlDistributedLock("hello", _timeout, null));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_AcquiresExclusiveApplicationLock_OnSession()
        {
            UseConnection(sql =>
            {
                // ReSharper disable once UnusedVariable
                var distributedLock = new PostgreSqlDistributedLock("hello", _timeout, sql);

                var lockCount = sql.Query<long>(
                    @"select count(*) from ""hangfire"".""lock"" where ""resource"" = @resource", new { resource = "hello"}).Single();

                Assert.Equal(lockCount, 1);
                //Assert.Equal("Exclusive", lockMode);
            });
        }



        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfLockCanNotBeGranted()
        {
            var releaseLock = new ManualResetEventSlim(false);
            var lockAcquired = new ManualResetEventSlim(false);

            var thread = new Thread(
                () => UseConnection(connection1 =>
                {
                    using (new PostgreSqlDistributedLock("exclusive", _timeout, connection1))
                    {
                        lockAcquired.Set();
                        releaseLock.Wait();
                    }
                }));
            thread.Start();

            lockAcquired.Wait();

            UseConnection(connection2 => 
                Assert.Throws<PostgreSqlDistributedLockException>(
                    () => new PostgreSqlDistributedLock("exclusive", _timeout, connection2)));

            releaseLock.Set();
            thread.Join();
        }

        [Fact, CleanDatabase]
        public void Dispose_ReleasesExclusiveApplicationLock()
        {
            UseConnection(sql =>
            {
                var distributedLock = new PostgreSqlDistributedLock("hello", _timeout, sql);
                distributedLock.Dispose();

                var lockCount = sql.Query<long>(
                    @"select count(*) from ""hangfire"".""lock"" where ""resource"" = @resource", new { resource = "hello" }).Single();

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
    }
}
