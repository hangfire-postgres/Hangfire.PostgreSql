using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Tests.Extensions;
using Hangfire.PostgreSql.Tests.Utils;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
  public class ExpirationManagerFacts : IClassFixture<PostgreSqlStorageFixture>
  {
    private readonly PostgreSqlStorageFixture _fixture;
    private readonly CancellationToken _token;

    public ExpirationManagerFacts(PostgreSqlStorageFixture fixture)
    {
      CancellationTokenSource cts = new();
      _token = cts.Token;
      _fixture = fixture;
      _fixture.SetupOptions(o => o.DeleteExpiredBatchSize = 2);
      DbQueryHelper.IsUpperCase = true;
      PostgreSql.Utils.DbQueryHelper.IsUpperCase = DbQueryHelper.IsUpperCase;
    }

    [Fact]
    public void Ctor_ThrowsAnException_WhenStorageIsNull()
    {
      Assert.Throws<ArgumentNullException>(() => new ExpirationManager(null));
    }

    [Fact]
    [CleanDatabase]
    public void Execute_RemovesOutdatedRecords()
    {
      UseConnection((connection, manager) => {
        long CreateEntry(string key)
        {
          return CreateExpirationEntry(connection, DateTime.UtcNow.AddMonths(-1), key);
        }

        List<long> entryIds = Enumerable.Range(1, 3).Select(i => CreateEntry($"key{i}")).ToList();

        manager.Execute(_token);

        entryIds.ForEach(entryId => Assert.True(IsEntryExpired(connection, entryId)));
      });
    }

    [Fact]
    [CleanDatabase]
    public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
    {
      UseConnection((connection, manager) => {
        long entryId = CreateExpirationEntry(connection, null);

        manager.Execute(_token);

        Assert.False(IsEntryExpired(connection, entryId));
      });
    }

    [Fact]
    [CleanDatabase]
    public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
    {
      UseConnection((connection, manager) => {
        long entryId = CreateExpirationEntry(connection, DateTime.Now.AddMonths(1));

        manager.Execute(_token);

        Assert.False(IsEntryExpired(connection, entryId));
      });
    }

    [Fact]
    [CleanDatabase]
    public void Execute_Processes_CounterTable()
    {
      UseConnection((connection, manager) => {
        // Arrange
        string createSql = $@"
          INSERT INTO ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}""
          (""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"", ""{"expireat".GetProperDbObjectName()}"") 
          VALUES ('key', 1, @ExpireAt)
        ";
        connection.Execute(createSql, new { ExpireAt = DateTime.UtcNow.AddMonths(-1) });

        // Act
        manager.Execute(_token);

        // Assert
        Assert.Equal(0, connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}"""));
      });
    }

    [Fact]
    [CleanDatabase]
    public void Execute_Aggregates_CounterTable()
    {
      UseConnection((connection, manager) => {
        // Arrange
        string createSql = $@"
          INSERT INTO ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}"" (""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"") 
          VALUES ('stats:succeeded', 1)
        ";
        for (int i = 0; i < 5; i++)
        {
          connection.Execute(createSql);
        }

        // Act
        manager.Execute(_token);

        // Assert
        Assert.Equal(5, connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}"""));
        Assert.Equal(5, connection.QuerySingle<long>($@"SELECT SUM(""{"value".GetProperDbObjectName()}"") FROM ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}"""));
      });
    }

    [Fact]
    [CleanDatabase]
    public void Execute_Processes_JobTable()
    {
      UseConnection((connection, manager) => {
        // Arrange
        string createSql = $@"
          INSERT INTO ""{GetSchemaName()}"".""{"job".GetProperDbObjectName()}"" 
          (""{"invocationdata".GetProperDbObjectName()}"", ""{"arguments".GetProperDbObjectName()}"", ""{"createdat".GetProperDbObjectName()}"", ""{"expireat".GetProperDbObjectName()}"") 
          VALUES ('', '', NOW() AT TIME ZONE 'UTC', @ExpireAt)
        ";
        connection.Execute(createSql, new { ExpireAt = DateTime.UtcNow.AddMonths(-1) });

        // Act
        manager.Execute(_token);

        // Assert
        Assert.Equal(0, connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"job".GetProperDbObjectName()}"""));
      });
    }

    [Fact]
    [CleanDatabase]
    public void Execute_Processes_ListTable()
    {
      UseConnection((connection, manager) => {
        // Arrange
        string createSql = $@"INSERT INTO ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}"" (""{"key".GetProperDbObjectName()}"", ""{"expireat".GetProperDbObjectName()}"") 
                           VALUES ('key', @ExpireAt)";
        connection.Execute(createSql, new { ExpireAt = DateTime.UtcNow.AddMonths(-1) });

        // Act
        manager.Execute(_token);

        // Assert
        Assert.Equal(0, connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}"""));
      });
    }

    [Fact]
    [CleanDatabase]
    public void Execute_Processes_SetTable()
    {
      UseConnection((connection, manager) => {
        // Arrange
        string createSql = $@"INSERT INTO ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}"" 
        (""{"key".GetProperDbObjectName()}"", ""{"score".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"", ""{"expireat".GetProperDbObjectName()}"") VALUES ('key', 0, '', @ExpireAt)";
        connection.Execute(createSql, new { ExpireAt = DateTime.UtcNow.AddMonths(-1) });

        // Act
        manager.Execute(_token);

        // Assert
        Assert.Equal(0, connection.Query<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""").Single());
      });
    }

    [Fact]
    [CleanDatabase]
    public void Execute_Processes_HashTable()
    {
      UseConnection((connection, manager) => {
        // Arrange
        string createSql = $@"
          INSERT INTO ""{GetSchemaName()}"".""{"hash".GetProperDbObjectName()}"" 
          (""{"key".GetProperDbObjectName()}"", ""{"field".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"", ""{"expireat".GetProperDbObjectName()}"") 
          VALUES ('key', 'field', '', @ExpireAt)";
        connection.Execute(createSql, new { ExpireAt = DateTime.UtcNow.AddMonths(-1) });

        // Act
        manager.Execute(_token);

        // Assert
        Assert.Equal(0, connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"hash".GetProperDbObjectName()}"""));
      });
    }

    private static long CreateExpirationEntry(NpgsqlConnection connection, DateTime? expireAt, string key = "key")
    {
      string insertSqlNull = $@"
        INSERT INTO ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}""(""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"", ""{"expireat".GetProperDbObjectName()}"")
        VALUES (@Key, 1, null) RETURNING ""{"id".GetProperDbObjectName()}""
      ";

      string insertSqlValue = $@"
        INSERT INTO ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}""(""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"", ""{"expireat".GetProperDbObjectName()}"")
        VALUES (@Key, 1, NOW() AT TIME ZONE 'UTC' - interval '{{0}} seconds') RETURNING ""{"id".GetProperDbObjectName()}""
      ";

      string insertSql = expireAt == null
        ? insertSqlNull
        : string.Format(CultureInfo.InvariantCulture, insertSqlValue,
          ((long)(DateTime.UtcNow - expireAt.Value).TotalSeconds).ToString(CultureInfo.InvariantCulture));

      return connection.QuerySingle<long>(insertSql, new { Key = key });
    }

    private static bool IsEntryExpired(NpgsqlConnection connection, long entryId)
    {
      return connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}"" WHERE ""{"id".GetProperDbObjectName()}"" = @Id", new { Id = entryId }) == 0;
    }

    private void UseConnection(Action<NpgsqlConnection, ExpirationManager> action)
    {
      PostgreSqlStorage storage = _fixture.SafeInit();
      ExpirationManager manager = new ExpirationManager(storage, TimeSpan.Zero);
      action(storage.CreateAndOpenConnection(), manager);
    }

    private static string GetSchemaName()
    {
      return ConnectionUtils.GetSchemaName();
    }
  }
}
