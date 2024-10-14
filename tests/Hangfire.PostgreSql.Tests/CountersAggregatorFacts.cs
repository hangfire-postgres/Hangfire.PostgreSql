using System;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Tests.Utils;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests;

public class CountersAggregatorFacts : IClassFixture<PostgreSqlStorageFixture>
{
  private static readonly string _schemaName = ConnectionUtils.GetSchemaName();

  private readonly CancellationToken _token;
  private readonly PostgreSqlStorageFixture _fixture;

  public CountersAggregatorFacts(PostgreSqlStorageFixture fixture)
  {
    CancellationTokenSource cts = new();
    _token = cts.Token;
    _fixture = fixture;
    _fixture.SetupOptions(o => o.CountersAggregateInterval = TimeSpan.FromMinutes(5));
  }

  [Fact]
  [CleanDatabase]
  public void Execute_AggregatesCounters()
  {
    UseConnection((connection, manager) => {
      CreateEntry(1);
      CreateEntry(5);
      CreateEntry(15);
      CreateEntry(5, "key2");
      CreateEntry(10, "key2");

      manager.Execute(_token);

      Assert.Equal(21, GetAggregatedCounters(connection));
      Assert.Equal(15, GetAggregatedCounters(connection, "key2"));
      Assert.Null(GetRegularCounters(connection));
      Assert.Null(GetRegularCounters(connection, "key2"));
      return;

      void CreateEntry(long value, string key = "key")
      {
        CreateCounterEntry(connection, value, key);
      }
    });
  }

  private void UseConnection(Action<NpgsqlConnection, CountersAggregator> action)
  {
    PostgreSqlStorage storage = _fixture.SafeInit();
    CountersAggregator aggregator = new(storage, TimeSpan.Zero);
    action(storage.CreateAndOpenConnection(), aggregator);
  }

  private static void CreateCounterEntry(NpgsqlConnection connection, long? value, string key = "key")
  {
    value ??= 1;
    string insertSql =
      $"""
       INSERT INTO "{_schemaName}"."counter"("key", "value", "expireat")
       VALUES (@Key, @Value, null)
       """;

    connection.Execute(insertSql, new { Key = key, Value = value });
  }

  private static long GetAggregatedCounters(NpgsqlConnection connection, string key = "key")
  {
    return connection.QuerySingle<long>(
      $"""
       SELECT "value"
       FROM {_schemaName}."aggregatedcounter"
       WHERE "key" = @Key 
       """, new { Key = key });
  }

  private static long? GetRegularCounters(NpgsqlConnection connection, string key = "key")
  {
    return connection.QuerySingle<long?>(
      $"""
         SELECT SUM("value")
         FROM {_schemaName}."counter"
         WHERE "key" = @Key
         """, new { Key = key });
  }
}
