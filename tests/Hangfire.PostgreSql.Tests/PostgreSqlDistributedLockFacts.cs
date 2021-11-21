using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Tests.Utils;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
  public class PostgreSqlDistributedLockFacts : IDisposable
  {
    private readonly TimeSpan _timeout = TimeSpan.FromSeconds(5);
    private NpgsqlConnection _connection;

    public void Dispose()
    {
      _connection?.Dispose();
    }

    [Fact]
    public void Acquire_ThrowsAnException_WhenResourceIsNullOrEmpty()
    {
      PostgreSqlStorageOptions options = new();

      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
        () => PostgreSqlDistributedLock.Acquire(new Mock<IDbConnection>().Object, "", _timeout, options));

      Assert.Equal("resource", exception.ParamName);
    }

    [Fact]
    public void Acquire_ThrowsAnException_WhenConnectionIsNull()
    {
      PostgreSqlStorageOptions options = new();

      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => PostgreSqlDistributedLock.Acquire(null, "hello", _timeout, options));

      Assert.Equal("connection", exception.ParamName);
    }

    [Fact]
    public void Acquire_ThrowsAnException_WhenOptionsIsNull()
    {
      Mock<IDbConnection> connection = new Mock<IDbConnection>();
      connection.SetupGet(c => c.State).Returns(ConnectionState.Open);
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
        () => PostgreSqlDistributedLock.Acquire(new Mock<IDbConnection>().Object, "hi", _timeout, null));

      Assert.Equal("options", exception.ParamName);
    }


    [Fact]
    [CleanDatabase]
    public void Acquire_AcquiresExclusiveApplicationLock_WithUseNativeDatabaseTransactions_OnSession()
    {
      PostgreSqlStorageOptions options = new() {
        SchemaName = GetSchemaName(),
        UseNativeDatabaseTransactions = true,
      };

      UseConnection(connection => {
        // ReSharper disable once UnusedVariable
        PostgreSqlDistributedLock.Acquire(connection, "hello", _timeout, options);

        long lockCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""lock"" where ""resource"" = @Resource",
          new { Resource = "hello" });

        Assert.Equal(1, lockCount);
        //Assert.Equal("Exclusive", lockMode);
      });
    }

    [Fact]
    [CleanDatabase]
    public void Acquire_AcquiresExclusiveApplicationLock_WithUseNativeDatabaseTransactions_OnSession_WhenDeadlockIsOccured()
    {
      PostgreSqlStorageOptions options = new() {
        SchemaName = GetSchemaName(),
        UseNativeDatabaseTransactions = true,
        DistributedLockTimeout = TimeSpan.FromSeconds(10),
      };

      UseConnection(connection => {
        // Arrange
        TimeSpan timeout = TimeSpan.FromSeconds(15);
        string resourceName = "hello";
        connection.Execute($@"INSERT INTO ""{GetSchemaName()}"".""lock"" VALUES (@ResourceName, 0, @Now)", new { ResourceName = resourceName, Now = DateTime.UtcNow });

        // Act && Assert (not throwing means it worked)
        PostgreSqlDistributedLock.Acquire(connection, resourceName, timeout, options);
      });
    }

    [Fact]
    [CleanDatabase]
    public void Acquire_AcquiresExclusiveApplicationLock_WithoutUseNativeDatabaseTransactions_OnSession()
    {
      PostgreSqlStorageOptions options = new() {
        SchemaName = GetSchemaName(),
        UseNativeDatabaseTransactions = false,
      };

      UseConnection(connection => {
        // ReSharper disable UnusedVariable

        // Acquire locks on two different resources to make sure they don't conflict.
        PostgreSqlDistributedLock.Acquire(connection, "hello", _timeout, options);
        PostgreSqlDistributedLock.Acquire(connection, "hello2", _timeout, options);

        // ReSharper restore UnusedVariable

        long lockCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM """ + GetSchemaName() + @""".""lock"" WHERE ""resource"" = @Resource",
          new { Resource = "hello" });

        Assert.Equal(1, lockCount);
      });
    }


    [Fact]
    [CleanDatabase]
    public void Acquire_ThrowsAnException_IfLockCanNotBeGranted_WithUseNativeDatabaseTransactions()
    {
      PostgreSqlStorageOptions options = new() {
        SchemaName = GetSchemaName(),
        UseNativeDatabaseTransactions = true,
      };

      ManualResetEventSlim releaseLock = new(false);
      ManualResetEventSlim lockAcquired = new(false);

      Thread thread = new(() => UseConnection(connection1 => {
        PostgreSqlDistributedLock.Acquire(connection1, "exclusive", _timeout, options);
        lockAcquired.Set();
        releaseLock.Wait();
        PostgreSqlDistributedLock.Release(connection1, "exclusive", options);
      }));
      thread.Start();

      lockAcquired.Wait();

      UseConnection(connection2 =>
        Assert.Throws<PostgreSqlDistributedLockException>(() => PostgreSqlDistributedLock.Acquire(connection2, "exclusive", _timeout, options)));

      releaseLock.Set();
      thread.Join();
    }

    [Fact]
    [CleanDatabase]
    public void Acquire_ThrowsAnException_IfLockCanNotBeGranted_WithoutUseNativeDatabaseTransactions()
    {
      PostgreSqlStorageOptions options = new() {
        SchemaName = GetSchemaName(),
        UseNativeDatabaseTransactions = false,
      };

      ManualResetEventSlim releaseLock = new(false);
      ManualResetEventSlim lockAcquired = new(false);

      Thread thread = new(() => UseConnection(connection1 => {
        PostgreSqlDistributedLock.Acquire(connection1, "exclusive", _timeout, options);
        lockAcquired.Set();
        releaseLock.Wait();
        PostgreSqlDistributedLock.Release(connection1, "exclusive", options);
      }));
      thread.Start();

      lockAcquired.Wait();

      UseConnection(connection2 =>
        Assert.Throws<PostgreSqlDistributedLockException>(() => PostgreSqlDistributedLock.Acquire(connection2, "exclusive", _timeout, options)));

      releaseLock.Set();
      thread.Join();
    }


    [Fact]
    [CleanDatabase]
    public void Dispose_ReleasesExclusiveApplicationLock_WithUseNativeDatabaseTransactions()
    {
      PostgreSqlStorageOptions options = new() {
        SchemaName = GetSchemaName(),
        UseNativeDatabaseTransactions = true,
      };

      UseConnection(connection => {
        PostgreSqlDistributedLock.Acquire(connection, "hello", _timeout, options);
        PostgreSqlDistributedLock.Release(connection, "hello", options);

        long lockCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""lock"" WHERE ""resource"" = @Resource",
          new { Resource = "hello" });

        Assert.Equal(0, lockCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void Dispose_ReleasesExclusiveApplicationLock_WithoutUseNativeDatabaseTransactions()
    {
      PostgreSqlStorageOptions options = new() {
        SchemaName = GetSchemaName(),
        UseNativeDatabaseTransactions = false,
      };

      UseConnection(connection => {
        PostgreSqlDistributedLock.Acquire(connection, "hello", _timeout, options);
        PostgreSqlDistributedLock.Release(connection, "hello", options);

        long lockCount = connection.Query<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""lock"" WHERE ""resource"" = @Resource",
          new { Resource = "hello" }).Single();

        Assert.Equal(0, lockCount);
      });
    }

    private void UseConnection(Action<NpgsqlConnection> action)
    {
      _connection = _connection ?? ConnectionUtils.CreateConnection();
      action(_connection);
    }

    private static string GetSchemaName()
    {
      return ConnectionUtils.GetSchemaName();
    }
  }
}
