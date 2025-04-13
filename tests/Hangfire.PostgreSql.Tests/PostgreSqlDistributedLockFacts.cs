using System.Data;
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
      PostgreSqlStorageContext context = new(options, null!);

      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
        () => PostgreSqlDistributedLock.Acquire(new Mock<IDbConnection>().Object, "", _timeout, context));

      Assert.Equal("resource", exception.ParamName);
    }

    [Fact]
    public void Acquire_ThrowsAnException_WhenConnectionIsNull()
    {
      PostgreSqlStorageOptions options = new();
      PostgreSqlStorageContext context = new(options, null!);

      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => PostgreSqlDistributedLock.Acquire(null, "hello", _timeout, context));

      Assert.Equal("connection", exception.ParamName);
    }

    [Fact]
    public void Acquire_ThrowsAnException_WhenContextIsNull()
    {
      Mock<IDbConnection> connection = new();
      connection.SetupGet(c => c.State).Returns(ConnectionState.Open);
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
        () => PostgreSqlDistributedLock.Acquire(new Mock<IDbConnection>().Object, "hi", _timeout, null));

      Assert.Equal("context", exception.ParamName);
    }


    [Fact]
    [CleanDatabase]
    public void Acquire_AcquiresExclusiveApplicationLock_WithUseNativeDatabaseTransactions_OnSession()
    {
      PostgreSqlStorageOptions options = new() {
        SchemaName = GetSchemaName(),
        UseNativeDatabaseTransactions = true,
      };
      PostgreSqlStorageContext context = new(options, null!);

      UseConnection(connection => {
        // ReSharper disable once UnusedVariable
        PostgreSqlDistributedLock.Acquire(connection, "hello", _timeout, context);

        long lockCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""lock"" WHERE ""resource"" = @Resource",
          new { Resource = "hello" });

        Assert.Equal(1, lockCount);
        //Assert.Equal("Exclusive", lockMode);
      });
    }

    [Fact]
    [CleanDatabase]
    public void Acquire_AcquiresExclusiveApplicationLock_WithUseNativeDatabaseTransactions_OnSession_WhenDeadlockOccurs()
    {
      PostgreSqlStorageOptions options = new() {
        SchemaName = GetSchemaName(),
        UseNativeDatabaseTransactions = true,
        DistributedLockTimeout = TimeSpan.FromSeconds(10),
      };
      PostgreSqlStorageContext context = new(options, null!);

      UseConnection(connection => {
        // Arrange
        TimeSpan timeout = TimeSpan.FromSeconds(15);
        string resourceName = "hello";
        connection.Execute($@"INSERT INTO ""{GetSchemaName()}"".""lock"" VALUES (@ResourceName, 0, @Now)", new { ResourceName = resourceName, Now = DateTime.UtcNow });

        // Act && Assert (not throwing means it worked)
        PostgreSqlDistributedLock.Acquire(connection, resourceName, timeout, context);
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
      PostgreSqlStorageContext context = new(options, null!);

      UseConnection(connection => {
        // Acquire locks on two different resources to make sure they don't conflict.
        PostgreSqlDistributedLock.Acquire(connection, "hello", _timeout, context);
        PostgreSqlDistributedLock.Acquire(connection, "hello2", _timeout, context);

        long lockCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""lock"" WHERE ""resource"" = @Resource",
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
      PostgreSqlStorageContext context = new(options, null!);

      ManualResetEventSlim releaseLock = new(false);
      ManualResetEventSlim lockAcquired = new(false);

      Thread thread = new(() => UseConnection(connection1 => {
        PostgreSqlDistributedLock.Acquire(connection1, "exclusive", _timeout, context);
        lockAcquired.Set();
        releaseLock.Wait();
        PostgreSqlDistributedLock.Release(connection1, "exclusive", context);
      }));
      thread.Start();

      lockAcquired.Wait();

      UseConnection(connection2 =>
        Assert.Throws<PostgreSqlDistributedLockException>(() => PostgreSqlDistributedLock.Acquire(connection2, "exclusive", _timeout, context)));

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
      PostgreSqlStorageContext context = new(options, null!);

      ManualResetEventSlim releaseLock = new(false);
      ManualResetEventSlim lockAcquired = new(false);

      Thread thread = new(() => UseConnection(connection1 => {
        PostgreSqlDistributedLock.Acquire(connection1, "exclusive", _timeout, context);
        lockAcquired.Set();
        releaseLock.Wait();
        PostgreSqlDistributedLock.Release(connection1, "exclusive", context);
      }));
      thread.Start();

      lockAcquired.Wait();

      UseConnection(connection2 =>
        Assert.Throws<PostgreSqlDistributedLockException>(() => PostgreSqlDistributedLock.Acquire(connection2, "exclusive", _timeout, context)));

      releaseLock.Set();
      thread.Join();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [CleanDatabase]
    public void Acquire_ExpiredLockExists_LocksAnyway(bool useNativeDatabaseTransactions)
    {
      const string resource = "hello";

      PostgreSqlStorageOptions options = new() {
        SchemaName = GetSchemaName(),
        UseNativeDatabaseTransactions = useNativeDatabaseTransactions,
      };
      PostgreSqlStorageContext context = new(options, null!);

      UseConnection(connection => {
        DateTime acquired = DateTime.UtcNow - options.DistributedLockTimeout - TimeSpan.FromMinutes(1);
        connection.Execute($@"INSERT INTO ""{GetSchemaName()}"".""lock"" (""resource"", ""acquired"") VALUES (@Resource, @Acquired)", new { Resource = resource, Acquired = acquired });

        PostgreSqlDistributedLock.Acquire(connection, resource, _timeout, context);

        long lockCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""lock"" WHERE ""resource"" = @Resource",
          new { Resource = resource });

        Assert.Equal(1, lockCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void Dispose_ReleasesExclusiveApplicationLock_WithUseNativeDatabaseTransactions()
    {
      PostgreSqlStorageOptions options = new() {
        SchemaName = GetSchemaName(),
        UseNativeDatabaseTransactions = true,
      };
      PostgreSqlStorageContext context = new(options, null!);

      UseConnection(connection => {
        PostgreSqlDistributedLock.Acquire(connection, "hello", _timeout, context);
        PostgreSqlDistributedLock.Release(connection, "hello", context);

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
      PostgreSqlStorageContext context = new(options, null!);

      UseConnection(connection => {
        PostgreSqlDistributedLock.Acquire(connection, "hello", _timeout, context);
        PostgreSqlDistributedLock.Release(connection, "hello", context);

        long lockCount = connection.Query<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""lock"" WHERE ""resource"" = @Resource",
          new { Resource = "hello" }).Single();

        Assert.Equal(0, lockCount);
      });
    }

    private void UseConnection(Action<NpgsqlConnection> action)
    {
      _connection ??= ConnectionUtils.CreateConnection();
      action(_connection);
    }

    private static string GetSchemaName()
    {
      return ConnectionUtils.GetSchemaName();
    }
  }
}
