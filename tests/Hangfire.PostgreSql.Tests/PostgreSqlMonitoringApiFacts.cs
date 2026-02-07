using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
  public class PostgreSqlMonitoringApiFacts : IClassFixture<PostgreSqlStorageFixture>
  {
    private readonly PostgreSqlStorageFixture _fixture;

    public PostgreSqlMonitoringApiFacts(PostgreSqlStorageFixture fixture)
    {
      _fixture = fixture;
    }

    [Fact]
    [CleanDatabase]
    public void GetJobs_MixedCasing_ReturnsJob()
    {
      string arrangeSql = $@"
        INSERT INTO ""{ConnectionUtils.GetSchemaName()}"".""job""(""invocationdata"", ""arguments"", ""createdat"")
        VALUES (@InvocationData::jsonb, @Arguments::jsonb, NOW()) RETURNING ""id""";

      Job job = Job.FromExpression(() => SampleMethod("Hello"));
      InvocationData invocationData = InvocationData.SerializeJob(job);

      UseConnection(connection => {
        long jobId = connection.QuerySingle<long>(arrangeSql,
          new {
            InvocationData = JsonParameter.GetParameterValue(SerializationHelper.Serialize(invocationData)),
            Arguments = JsonParameter.GetParameterValue(invocationData.Arguments, JsonParameter.ValueType.Array),
          });

        Mock<IState> state = new();
        state.Setup(x => x.Name).Returns(SucceededState.StateName);
        state.Setup(x => x.SerializeData())
          .Returns(new Dictionary<string, string> {
            { "SUCCEEDEDAT", "2018-05-03T13:28:18.3939693Z" },
            { "PerformanceDuration", "53" },
            { "latency", "6730" },
          });

        Commit(connection, x => x.SetJobState(jobId.ToString(CultureInfo.InvariantCulture), state.Object));

        IMonitoringApi monitoringApi = _fixture.Storage.GetMonitoringApi();
        JobList<SucceededJobDto> jobs = monitoringApi.SucceededJobs(0, 10);

        Assert.NotNull(jobs);
      });
    }

    [Fact]
    [CleanDatabase]
    public void HourlySucceededJobs_ReturnsAggregatedStats()
    {
      DateTime now = DateTime.UtcNow;
      string schemaName = ConnectionUtils.GetSchemaName();
      string key = $"stats:succeeded:{now.ToString("yyyy-MM-dd-HH", CultureInfo.InvariantCulture)}";
      string arrangeSql =
        $"""
        BEGIN;
        INSERT INTO "{schemaName}"."counter"("key", "value") 
        VALUES (@Key, 5);
        INSERT INTO "{schemaName}"."aggregatedcounter"("key", "value") 
        VALUES (@Key, 7);
        COMMIT;
        """;
      UseConnection(connection => {
        connection.Execute(arrangeSql, new { Key = key });

        IMonitoringApi monitoringApi = _fixture.Storage.GetMonitoringApi();
        IDictionary<DateTime, long> stats = monitoringApi.HourlySucceededJobs();
        Assert.Equal(24, stats.Count);

        long actualCounter = Assert.Single(stats.Where(x => x.Key.Hour == now.Hour).Select(x => x.Value));
        Assert.Equal(12, actualCounter);
      });
    }

    private void UseConnection(Action<NpgsqlConnection> action)
    {
      PostgreSqlStorage storage = _fixture.SafeInit();
      action(storage.CreateAndOpenConnection());
    }

    private void Commit(
      NpgsqlConnection connection,
      Action<PostgreSqlWriteOnlyTransaction> action)
    {
      PostgreSqlStorage storage = _fixture.SafeInit();
      using PostgreSqlWriteOnlyTransaction transaction = new(storage, () => connection);
      action(transaction);
      transaction.Commit();
    }

    [Fact]
    [CleanDatabase]
    public void FetchedJobs_WithDuplicateJobQueueEntries_DoesNotThrow()
    {
      string schemaName = ConnectionUtils.GetSchemaName();

      string createJobSql = $@"
        INSERT INTO ""{schemaName}"".""job"" (""invocationdata"", ""arguments"", ""createdat"")
        VALUES (@InvocationData, @Arguments, NOW()) RETURNING ""id""";

      string createJobQueueSql = $@"
        INSERT INTO ""{schemaName}"".""jobqueue"" (""jobid"", ""queue"", ""fetchedat"")
        VALUES (@JobId, @Queue, @FetchedAt)";

      UseConnection(connection => {
        Job job = Job.FromExpression(() => SampleMethod("test"));
        InvocationData invocationData = InvocationData.SerializeJob(job);

        long jobId = connection.QuerySingle<long>(createJobSql,
          new {
            InvocationData = new JsonParameter(SerializationHelper.Serialize(invocationData)),
            Arguments = new JsonParameter(invocationData.Arguments, JsonParameter.ValueType.Array),
          });

        DateTime fetchedAt = DateTime.UtcNow;
        connection.Execute(createJobQueueSql, new { JobId = jobId, Queue = "default", FetchedAt = fetchedAt });
        connection.Execute(createJobQueueSql, new { JobId = jobId, Queue = "default", FetchedAt = fetchedAt.AddSeconds(1) });

        PostgreSqlStorage storage = _fixture.SafeInit();
        PostgreSqlStorageOptions options = new() { SchemaName = schemaName };

        PostgreSqlJobQueueProvider provider = new(storage, options);
        PersistentJobQueueProviderCollection providers = new(provider);
        storage.QueueProviders = providers;

        IMonitoringApi monitoringApi = storage.GetMonitoringApi();
        JobList<FetchedJobDto> fetchedJobs = monitoringApi.FetchedJobs("default", 0, 10);

        Assert.NotNull(fetchedJobs);
        Assert.Single(fetchedJobs);
      });
    }

#pragma warning disable xUnit1013 // Public method should be marked as test
    public static void SampleMethod(string arg)
#pragma warning restore xUnit1013 // Public method should be marked as test
    { }
  }
}
