using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Factories;
using Hangfire.PostgreSql.Tests.Entities;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.Server;
using Hangfire.Storage;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
  public class PostgreSqlConnectionFacts : IClassFixture<PostgreSqlStorageFixture>
  {
    private readonly PostgreSqlStorageFixture _fixture;

    public PostgreSqlConnectionFacts(PostgreSqlStorageFixture fixture)
    {
      _fixture = fixture;
      _fixture.SetupOptions(o => o.TransactionSynchronisationTimeout = TimeSpan.FromSeconds(2));
    }

    [Fact]
    public void Ctor_ThrowsAnException_WhenStorageIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new PostgreSqlConnection(null));

      Assert.Equal("storage", exception.ParamName);
    }

    [Fact]
    [CleanDatabase]
    public void Ctor_ThrowsAnException_WhenOptionsIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
        () => new PostgreSqlConnection(new PostgreSqlStorage(ConnectionUtils.GetDefaultConnectionFactory(), null)));

      Assert.Equal("options", exception.ParamName);
    }


    [Fact]
    [CleanDatabase]
    public void FetchNextJob_DelegatesItsExecution_ToTheQueue()
    {
      UseConnection(connection => {
        CancellationToken token = new();
        string[] queues = { "default" };

        connection.FetchNextJob(queues, token);

        _fixture.PersistentJobQueueMock.Verify(x => x.Dequeue(queues, token));
      });
    }

    [Fact]
    [CleanDatabase]
    public void FetchNextJob_Throws_IfMultipleProvidersResolved()
    {
      UseConnection(connection => {
        CancellationToken token = new CancellationToken();
        Mock<IPersistentJobQueueProvider> anotherProvider = new Mock<IPersistentJobQueueProvider>();
        _fixture.PersistentJobQueueProviderCollection.Add(anotherProvider.Object, new[] { "critical" });

        try
        {
          Assert.Throws<InvalidOperationException>(() => connection.FetchNextJob(new[] { "critical", "default" }, token));
        }
        finally
        {
          _fixture.PersistentJobQueueProviderCollection.Remove("critical");
        }
      });
    }

    [Fact]
    [CleanDatabase]
    public void CreateWriteTransaction_ReturnsNonNullInstance()
    {
      UseConnection(connection => {
        IWriteOnlyTransaction transaction = connection.CreateWriteTransaction();
        Assert.NotNull(transaction);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AcquireLock_ReturnsNonNullInstance()
    {
      UseConnection(connection => {
        IDisposable distributedLock = connection.AcquireDistributedLock("1", TimeSpan.FromSeconds(1));
        Assert.NotNull(distributedLock);
      });
    }

    [Fact]
    [CleanDatabase]
    public void CreateExpiredJob_ThrowsAnException_WhenJobIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.CreateExpiredJob(null,
          new Dictionary<string, string>(),
          DateTime.UtcNow,
          TimeSpan.Zero));

        Assert.Equal("job", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void CreateExpiredJob_ThrowsAnException_WhenParametersCollectionIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.CreateExpiredJob(
          Job.FromExpression(() => SampleMethod("hello")),
          null,
          DateTime.UtcNow,
          TimeSpan.Zero));

        Assert.Equal("parameters", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void CreateExpiredJob_CreatesAJobInTheStorage_AndSetsItsParameters()
    {
      UseConnections((connection, jobStorageConnection) => {
        DateTime createdAt = new DateTime(2012, 12, 12, 0, 0, 0, DateTimeKind.Utc);
        string jobId = jobStorageConnection.CreateExpiredJob(Job.FromExpression(() => SampleMethod("Hello")),
          new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
          createdAt,
          TimeSpan.FromDays(1));

        Assert.NotNull(jobId);
        Assert.NotEmpty(jobId);

        TestJob testJob = Helper.GetTestJob(connection, GetSchemaName(), "-1");
        Assert.Equal(jobId, testJob.Id.ToString(CultureInfo.InvariantCulture));
        Assert.Equal(createdAt, testJob.CreatedAt);
        Assert.Null(testJob.StateId);
        Assert.Null(testJob.StateName);

        InvocationData invocationData = SerializationHelper.Deserialize<InvocationData>(testJob.InvocationData);
        invocationData.Arguments = testJob.Arguments;

        Job job = invocationData.DeserializeJob();
        Assert.Equal(typeof(PostgreSqlConnectionFacts), job.Type);
        Assert.Equal("SampleMethod", job.Method.Name);
        Assert.Equal("Hello", job.Args[0]);

        Assert.True(createdAt.AddDays(1).AddMinutes(-1) < testJob.ExpireAt);
        Assert.True(testJob.ExpireAt < createdAt.AddDays(1).AddMinutes(1));

        Dictionary<string, string> parameters = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""jobparameter"" WHERE ""jobid"" = @Id",
            new { Id = Convert.ToInt64(jobId, CultureInfo.InvariantCulture) })
          .ToDictionary(x => (string)x.name, x => (string)x.value);

        Assert.Equal("Value1", parameters["Key1"]);
        Assert.Equal("Value2", parameters["Key2"]);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
    {
      UseConnection(connection => Assert.Throws<ArgumentNullException>(() => connection.GetJobData(null)));
    }

    [Fact]
    [CleanDatabase]
    public void GetJobData_ReturnsNull_WhenThereIsNoSuchJob()
    {
      UseConnection(connection => {
        JobData result = connection.GetJobData("1");
        Assert.Null(result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetJobData_ReturnsResult_WhenJobExists()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""job"" (""invocationdata"", ""arguments"", ""statename"", ""createdat"")
        VALUES (@InvocationData, @Arguments, @StateName, NOW()) RETURNING ""id""
      ";

      UseConnections((connection, jobStorageConnection) => {
        Job job = Job.FromExpression(() => SampleMethod("wrong"));

        long jobId = connection.QuerySingle<long>(arrangeSql,
          new {
            InvocationData = new JsonParameter(SerializationHelper.Serialize(InvocationData.SerializeJob(job))),
            StateName = "Succeeded",
            Arguments = new JsonParameter("[\"\\\"Arguments\\\"\"]", JsonParameter.ValueType.Array),
          });

        JobData result = jobStorageConnection.GetJobData(jobId.ToString(CultureInfo.InvariantCulture));

        Assert.NotNull(result);
        Assert.NotNull(result.Job);
        Assert.Equal("Succeeded", result.State);
        Assert.Equal("Arguments", result.Job.Args[0]);
        Assert.Null(result.LoadException);
        Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
        Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
    {
      UseConnection(connection => Assert.Throws<ArgumentNullException>(() => connection.GetStateData(null)));
    }

    [Fact]
    [CleanDatabase]
    public void GetStateData_ReturnsNull_IfThereIsNoSuchState()
    {
      UseConnection(connection => {
        StateData result = connection.GetStateData("1");
        Assert.Null(result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetStateData_ReturnsCorrectData()
    {
      string createJobSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""job"" (""invocationdata"", ""arguments"", ""statename"", ""createdat"")
        VALUES ('{{}}', '[]', '', NOW()) RETURNING ""id"";
      ";

      string createStateSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""state"" (""jobid"", ""name"", ""createdat"")
        VALUES(@JobId, 'old-state', NOW());

        INSERT INTO ""{GetSchemaName()}"".""state"" (""jobid"", ""name"", ""reason"", ""data"", ""createdat"")
        VALUES(@JobId, @Name, @Reason, @Data, NOW())
        RETURNING ""id"";
      ";

      string updateJobStateSql = $@"
        UPDATE ""{GetSchemaName()}"".""job""
        SET ""stateid"" = @StateId
        WHERE ""id"" = @JobId;
      ";

      UseConnections((connection, jobStorageConnection) => {
        Dictionary<string, string> data = new() {
          { "Key", "Value" },
        };

        long jobId = connection.QuerySingle<long>(createJobSql);

        long stateId = connection.QuerySingle<long>(createStateSql,
          new { JobId = jobId, Name = "Name", Reason = "Reason", Data = new JsonParameter(SerializationHelper.Serialize(data)) });

        connection.Execute(updateJobStateSql, new { JobId = jobId, StateId = stateId });

        StateData result = jobStorageConnection.GetStateData(jobId.ToString(CultureInfo.InvariantCulture));
        Assert.NotNull(result);

        Assert.Equal("Name", result.Name);
        Assert.Equal("Reason", result.Reason);
        Assert.Equal("Value", result.Data["Key"]);
      });
    }

    [Fact]
    [CleanDatabase]
    public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.SetJobParameter(null, "name", "value"));

        Assert.Equal("id", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void SetParameter_ThrowsAnException_WhenNameIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.SetJobParameter("1", null, "value"));

        Assert.Equal("name", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""job"" (""invocationdata"", ""arguments"", ""createdat"")
        VALUES ('{{}}', '[]', NOW()) RETURNING ""id""
      ";

      UseConnections((connection, jobStorageConnection) => {
        string jobId = connection.QuerySingle<long>(arrangeSql).ToString(CultureInfo.InvariantCulture);

        jobStorageConnection.SetJobParameter(jobId, "Name", "Value");

        string parameterValue = connection.QuerySingle<string>($@"SELECT ""value"" FROM ""{GetSchemaName()}"".""jobparameter"" WHERE ""jobid"" = @Id AND ""name"" = @Name",
          new { Id = Convert.ToInt64(jobId, CultureInfo.InvariantCulture), Name = "Name" });

        Assert.Equal("Value", parameterValue);
      });
    }

    [Fact]
    [CleanDatabase]
    public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""job"" (""invocationdata"", ""arguments"", ""createdat"")
        VALUES ('{{}}', '[]', NOW()) RETURNING ""id""
      ";

      UseConnections((connection, jobStorageConnection) => {
        string jobId = connection.QuerySingle<long>(arrangeSql).ToString(CultureInfo.InvariantCulture);

        jobStorageConnection.SetJobParameter(jobId, "Name", "Value");
        jobStorageConnection.SetJobParameter(jobId, "Name", "AnotherValue");

        string parameterValue = connection.QuerySingle<string>($@"SELECT ""value"" FROM ""{GetSchemaName()}"".""jobparameter"" WHERE ""jobid"" = @Id AND ""name"" = @Name",
          new { Id = Convert.ToInt64(jobId, CultureInfo.InvariantCulture), Name = "Name" });

        Assert.Equal("AnotherValue", parameterValue);
      });
    }

    [Fact]
    [CleanDatabase]
    public void SetParameter_CanAcceptNulls_AsValues()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""job"" (""invocationdata"", ""arguments"", ""createdat"")
        VALUES ('{{}}', '[]', NOW()) RETURNING ""id""
      ";

      UseConnections((connection, jobStorageConnection) => {
        string jobId = connection.QuerySingle<long>(arrangeSql).ToString(CultureInfo.InvariantCulture);

        jobStorageConnection.SetJobParameter(jobId, "Name", null);

        string parameterValue = connection.QuerySingle<string>($@"SELECT ""value"" FROM ""{GetSchemaName()}"".""jobparameter"" WHERE ""jobid"" = @Id AND ""name"" = @Name",
          new { Id = Convert.ToInt64(jobId, CultureInfo.InvariantCulture), Name = "Name" });

        Assert.Null(parameterValue);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetParameter_ThrowsAnException_WhenJobIdIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetJobParameter(null, "hello"));

        Assert.Equal("id", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetParameter_ThrowsAnException_WhenNameIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetJobParameter("1", null));

        Assert.Equal("name", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetParameter_ReturnsNull_WhenParameterDoesNotExists()
    {
      UseConnection(connection => {
        string value = connection.GetJobParameter("1", "hello");
        Assert.Null(value);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetParameter_ReturnsParameterValue_WhenJobExists()
    {
      string arrangeSql = $@"
        WITH ""insertedjob"" AS (
          INSERT INTO ""{GetSchemaName()}"".""job"" (""invocationdata"", ""arguments"", ""createdat"")
          VALUES ('{{}}', '[]', NOW()) RETURNING ""id""
        )
        INSERT INTO ""{GetSchemaName()}"".""jobparameter"" (""jobid"", ""name"", ""value"")
        SELECT ""insertedjob"".""id"", @Name, @Value
        FROM ""insertedjob""
        RETURNING ""jobid"";
      ";
      UseConnections((connection, jobStorageConnection) => {
        long id = connection.QuerySingle<long>(arrangeSql,
          new { Name = "name", Value = "value" });

        string value = jobStorageConnection.GetJobParameter(Convert.ToString(id, CultureInfo.InvariantCulture), "name");

        Assert.Equal("value", value);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetFirstByLowestScoreFromSet(null, 0, 1));

        Assert.Equal("key", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetFirstByLowestScoreFromSet_ThrowsAnException_ToScoreIsLowerThanFromScore()
    {
      UseConnection(connection => Assert.Throws<ArgumentException>(() => connection.GetFirstByLowestScoreFromSet("key", 0, -1)));
    }

    [Fact]
    [CleanDatabase]
    public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTheKeyDoesNotExist()
    {
      UseConnection(connection => {
        string result = connection.GetFirstByLowestScoreFromSet("key", 0, 1);

        Assert.Null(result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""set"" (""key"", ""score"", ""value"")
        VALUES 
        ('key', 1.0, '1.0'),
        ('key', -1.0, '-1.0'),
        ('key', -5.0, '-5.0'),
        ('another-key', -2.0, '-2.0')
      ";

      UseConnections((connection, jobStorageConnection) => {
        connection.Execute(arrangeSql);

        string result = jobStorageConnection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0);

        Assert.Equal("-1.0", result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetFirstByLowestScoreFromSet_List_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetFirstByLowestScoreFromSet(null, 0, 1, 1));

        Assert.Equal("key", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetFirstByLowestScoreFromSet_List_ThrowsAnException_WhenToScoreIsLowerThanFromScore()
    {
      UseConnection(connection => {
        ArgumentException exception = Assert.Throws<ArgumentException>(() => connection.GetFirstByLowestScoreFromSet("key", 0, -1, 1));

        Assert.Contains("The 'toScore' value must be higher or equal to the 'fromScore' value.", exception.Message);
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(-1)]
    [InlineData(0)]
    public void GetFirstByLowestScoreFromSet_List_ThrowsAnException_WhenCountIsLessThanOne(int count)
    {
      UseConnection(connection => {
        ArgumentException exception = Assert.Throws<ArgumentException>(() => connection.GetFirstByLowestScoreFromSet("key", 0, 1, count));

        Assert.Contains("The 'count' value must be greater than zero (0).", exception.Message);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetFirstByLowestScoreFromSet_List_ReturnsEmpty_WhenTheKeyDoesNotExist()
    {
      UseConnection(connection => {
        List<string> result = connection.GetFirstByLowestScoreFromSet("key", 0, 1, 1);

        Assert.NotNull(result);
        Assert.Empty(result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetFirstByLowestScoreFromSet_List_ReturnsEmpty_WhenNoValuesExistForKey()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""set"" (""key"", ""score"", ""value"")
        VALUES 
        ('another-key', -2.0, '-2.0')
      ";

      UseConnections((connection, jobStorageConnection) => {
        connection.Execute(arrangeSql);

        List<string> result = jobStorageConnection.GetFirstByLowestScoreFromSet("key", 0, 1, 1);

        Assert.NotNull(result);
        Assert.Empty(result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetFirstByLowestScoreFromSet_List_ReturnsAllLowestValuesMatchingInputs()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""set"" (""key"", ""score"", ""value"")
        VALUES 
        ('key', 1.0, '1.0'),
        ('key', -1.0, '-1.0'),
        ('key', -5.0, '-5.0'),
        ('another-key', -2.0, '-2.0')
      ";

      UseConnections((connection, jobStorageConnection) => {
        connection.Execute(arrangeSql);

        List<string> result = jobStorageConnection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0, 10);

        Assert.Equal(2, result.Count);
        Assert.Equal("-1.0", result[0]);
        Assert.Equal("1.0", result[1]);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetFirstByLowestScoreFromSet_List_ReturnsSubsetOfLowestValuesMatchingInputs()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""set"" (""key"", ""score"", ""value"")
        VALUES 
        ('key', 1.0, '1.0'),
        ('key', 1.5, '1.5'),
        ('key', 2.0, '2.0'),
        ('key', 2.5, '2.5'),
        ('key', -1.0, '-1.0'),
        ('key', -5.0, '-5.0'),
        ('another-key', -2.0, '-2.0')
      ";

      int count = 3;
      UseConnections((connection, jobStorageConnection) => {
        connection.Execute(arrangeSql);

        List<string> result = jobStorageConnection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0, count);

        Assert.Equal(count, result.Count);
        Assert.Equal("-1.0", result[0]);
        Assert.Equal("1.0", result[1]);
        Assert.Equal("1.5", result[2]);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.AnnounceServer(null, new ServerContext()));

        Assert.Equal("serverId", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.AnnounceServer("server", null));

        Assert.Equal("context", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AnnounceServer_CreatesOrUpdatesARecord()
    {
      UseConnections((connection, jobStorageConnection) => {
        ServerContext context1 = new ServerContext {
          WorkerCount = 4,
          Queues = new[] { "critical", "default" },
        };
        jobStorageConnection.AnnounceServer("server", context1);

        dynamic server = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""server""").Single();
        Assert.Equal("server", server.id);

        ServerContext serverData = JsonSerializer.Deserialize<ServerContext>(server.data);
        Assert.Equal(4, serverData.WorkerCount);
        Assert.Equal(context1.Queues, serverData.Queues);
        Assert.NotNull(server.lastheartbeat);

        ServerContext context2 = new ServerContext {
          Queues = new[] { "default" },
          WorkerCount = 1000,
        };
        jobStorageConnection.AnnounceServer("server", context2);
        dynamic sameServer = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""server""").Single();
        Assert.Equal("server", sameServer.id);
        Assert.Contains("1000", sameServer.data);
      });
    }

    [Fact]
    [CleanDatabase]
    public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
    {
      UseConnection(connection => Assert.Throws<ArgumentNullException>(() => connection.RemoveServer(null)));
    }

    [Fact]
    [CleanDatabase]
    public void RemoveServer_RemovesAServerRecord()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""server"" (""id"", ""data"", ""lastheartbeat"")
        VALUES ('Server1', '{{}}', NOW()),
        ('Server2', '{{}}', NOW())
      ";

      UseConnections((connection, jobStorageConnection) => {
        connection.Execute(arrangeSql);

        jobStorageConnection.RemoveServer("Server1");

        dynamic server = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""server""").Single();
        Assert.NotEqual("Server1", server.Id, StringComparer.OrdinalIgnoreCase);
      });
    }

    [Fact]
    [CleanDatabase]
    public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
    {
      UseConnection(connection => Assert.Throws<ArgumentNullException>(() => connection.Heartbeat(null)));
    }

    [Fact]
    [CleanDatabase]
    public void Heartbeat_ThrowsBackgroundServerGoneException_WhenServerDisappeared()
    {
      string disappearedServerId = Guid.NewGuid().ToString();

      UseConnection(connection => Assert.Throws<BackgroundServerGoneException>(() => connection.Heartbeat(disappearedServerId)));
    }

    [Fact]
    [CleanDatabase]
    public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""server"" (""id"", ""data"", ""lastheartbeat"")
        VALUES ('server1', '{{}}', '2012-12-12 12:12:12'), ('server2', '{{}}', '2012-12-12 12:12:12')
      ";

      UseConnections((connection, jobStorageConnection) => {
        connection.Execute(arrangeSql);

        jobStorageConnection.Heartbeat("server1");

        Dictionary<string, DateTime> servers = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""server""")
          .ToDictionary(x => (string)x.id, x => (DateTime)x.lastheartbeat);

        Assert.NotEqual(2012, servers["server1"].Year);
        Assert.Equal(2012, servers["server2"].Year);
      });
    }

    [Fact]
    [CleanDatabase]
    public void RemoveTimedOutServers_ThrowsAnException_WhenTimeOutIsNegative()
    {
      UseConnection(connection => Assert.Throws<ArgumentException>(() => connection.RemoveTimedOutServers(TimeSpan.FromMinutes(-5))));
    }

    [Fact]
    [CleanDatabase]
    public void RemoveTimedOutServers_DoItsWorkPerfectly()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""server"" (""id"", ""data"", ""lastheartbeat"")
        VALUES (@Id, '{{}}', @Heartbeat)
      ";

      UseConnections((connection, jobStorageConnection) => {
        connection.Execute(arrangeSql,
          new[] {
            new { Id = "server1", Heartbeat = DateTime.UtcNow.AddDays(-1) },
            new { Id = "server2", Heartbeat = DateTime.UtcNow.AddHours(-12) },
          });

        jobStorageConnection.RemoveTimedOutServers(TimeSpan.FromHours(15));

        dynamic liveServer = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""server""").Single();
        Assert.Equal("server2", liveServer.id);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection =>
        Assert.Throws<ArgumentNullException>(() => connection.GetAllItemsFromSet(null)));
    }

    [Fact]
    [CleanDatabase]
    public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenKeyDoesNotExist()
    {
      UseConnection(connection => {
        HashSet<string> result = connection.GetAllItemsFromSet("some-set");

        Assert.NotNull(result);
        Assert.Empty(result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetAllItemsFromSet_ReturnsAllItems()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""set"" (""key"", ""score"", ""value"")
        VALUES (@Key, 0.0, @Value)
      ";

      UseConnections((connection, jobStorageConnection) => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "some-set", Value = "1" },
          new { Key = "some-set", Value = "2" },
          new { Key = "another-set", Value = "3" },
        });

        // Act
        HashSet<string> result = jobStorageConnection.GetAllItemsFromSet("some-set");

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Contains("1", result);
        Assert.Contains("2", result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.SetRangeInHash(null, new Dictionary<string, string>()));

        Assert.Equal("key", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.SetRangeInHash("some-hash", null));

        Assert.Equal("keyValuePairs", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void SetRangeInHash_MergesAllRecords()
    {
      UseConnections((connection, jobStorageConnection) => {
        jobStorageConnection.SetRangeInHash("some-hash", new Dictionary<string, string> {
          { "Key1", "Value1" },
          { "Key2", "Value2" },
        });

        Dictionary<string, string> result = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""hash"" WHERE ""key"" = @Key",
            new { Key = "some-hash" })
          .ToDictionary(x => (string)x.field, x => (string)x.value);

        Assert.Equal("Value1", result["Key1"]);
        Assert.Equal("Value2", result["Key2"]);
      });
    }

    [Fact]
    [CleanDatabase]
    public void SetRangeInHash_DoesNotThrowSerializationException()
    {
      Parallel.For(1, 100, _ => {
        UseDisposableConnection(connection => {
          connection.SetRangeInHash("some-hash", new Dictionary<string, string> {
            { "Key1", "Value1" },
            { "Key2", "Value2" },
          });
        });
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection =>
        Assert.Throws<ArgumentNullException>(() => connection.GetAllEntriesFromHash(null)));
    }

    [Fact]
    [CleanDatabase]
    public void GetAllEntriesFromHash_ReturnsNull_IfHashDoesNotExist()
    {
      UseConnection(connection => {
        Dictionary<string, string> result = connection.GetAllEntriesFromHash("some-hash");
        Assert.Null(result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""hash"" (""key"", ""field"", ""value"")
        VALUES (@Key, @Field, @Value)
      ";

      UseConnections((connection, jobStorageConnection) => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "some-hash", Field = "Key1", Value = "Value1" },
          new { Key = "some-hash", Field = "Key2", Value = "Value2" },
          new { Key = "another-hash", Field = "Key3", Value = "Value3" },
        });

        // Act
        Dictionary<string, string> result = jobStorageConnection.GetAllEntriesFromHash("some-hash");

        // Assert
        Assert.NotNull(result);
        Assert.Equal(2, result.Count);
        Assert.Equal("Value1", result["Key1"]);
        Assert.Equal("Value2", result["Key2"]);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetSetCount_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetSetCount(null)); });
    }

    [Fact]
    [CleanDatabase]
    public void GetSetCount_ReturnsZero_WhenSetDoesNotExist()
    {
      UseConnection(connection => {
        long result = connection.GetSetCount("my-set");
        Assert.Equal(0, result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetSetCount_ReturnsNumberOfElements_InASet()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".set (key, value, score) VALUES (@Key, @Value, 0.0)";

      UseConnections((connection, jobStorageConnection) => {
        connection.Execute(arrangeSql, new[] {
          new { Key = "set-1", Value = "value-1" },
          new { Key = "set-2", Value = "value-1" },
          new { Key = "set-1", Value = "value-2" },
        });

        long result = jobStorageConnection.GetSetCount("set-1");

        Assert.Equal(2, result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetAllItemsFromList_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetAllItemsFromList(null)); });
    }

    [Fact]
    [CleanDatabase]
    public void GetAllItemsFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
    {
      UseConnection(connection => {
        List<string> result = connection.GetAllItemsFromList("my-list");
        Assert.Empty(result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetAllItemsFromList_ReturnsAllItems_FromAGivenList()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".list (key, value) VALUES (@Key, @Value)";

      UseConnections((connection, jobStorageConnection) => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "list-1", Value = "1" },
          new { Key = "list-2", Value = "2" },
          new { Key = "list-1", Value = "3" },
        });

        // Act
        List<string> result = jobStorageConnection.GetAllItemsFromList("list-1");

        // Assert
        Assert.Equal(new[] { "3", "1" }, result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetCounter_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetCounter(null)); });
    }

    [Fact]
    [CleanDatabase]
    public void GetCounter_ReturnsZero_WhenKeyDoesNotExist()
    {
      UseConnection(connection => {
        long result = connection.GetCounter("my-counter");
        Assert.Equal(0, result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetCounter_ReturnsSumOfValues_InCounterTable()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".counter (key, value) VALUES (@Key, @Value)";

      UseConnections((connection, jobStorageConnection) => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "counter-1", Value = 1 },
          new { Key = "counter-2", Value = 1 },
          new { Key = "counter-1", Value = 1 },
        });

        // Act
        long result = jobStorageConnection.GetCounter("counter-1");

        // Assert
        Assert.Equal(2, result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetListCount_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetListCount(null)); });
    }

    [Fact]
    [CleanDatabase]
    public void GetListCount_ReturnsZero_WhenListDoesNotExist()
    {
      UseConnection(connection => {
        long result = connection.GetListCount("my-list");
        Assert.Equal(0, result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetListCount_ReturnsTheNumberOfListElements()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".""list""(""key"") VALUES (@Key)";

      UseConnections((connection, jobStorageConnection) => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "list-1" },
          new { Key = "list-1" },
          new { Key = "list-2" },
        });

        // Act
        long result = jobStorageConnection.GetListCount("list-1");

        // Assert
        Assert.Equal(2, result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetListTtl_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetListTtl(null)); });
    }

    [Fact]
    [CleanDatabase]
    public void GetListTtl_ReturnsNegativeValue_WhenListDoesNotExist()
    {
      UseConnection(connection => {
        TimeSpan result = connection.GetListTtl("my-list");
        Assert.True(result < TimeSpan.Zero);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetListTtl_ReturnsExpirationTimeForList()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".list (key, expireat) VALUES (@Key, @ExpireAt)";

      UseConnections((connection, jobStorageConnection) => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "list-1", ExpireAt = (DateTime?)DateTime.UtcNow.AddHours(1) },
          new { Key = "list-2", ExpireAt = (DateTime?)null },
        });

        // Act
        TimeSpan result = jobStorageConnection.GetListTtl("list-1");

        // Assert
        Assert.True(TimeSpan.FromMinutes(59) < result);
        Assert.True(result < TimeSpan.FromMinutes(61));
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetRangeFromList_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetRangeFromList(null, 0, 1));

        Assert.Equal("key", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetRangeFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
    {
      UseConnection(connection => {
        List<string> result = connection.GetRangeFromList("my-list", 0, 1);
        Assert.Empty(result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetRangeFromList_ReturnsAllEntries_WithinGivenBounds()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".list (key, value) VALUES (@Key, @Value)";

      UseConnections((connection, jobStorageConnection) => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "list-1", Value = "1" },
          new { Key = "list-2", Value = "2" },
          new { Key = "list-1", Value = "3" },
          new { Key = "list-1", Value = "4" },
          new { Key = "list-1", Value = "5" },
        });

        // Act
        List<string> result = jobStorageConnection.GetRangeFromList("list-1", 1, 2);

        // Assert
        Assert.Equal(new[] { "4", "3" }, result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetHashCount_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetHashCount(null)); });
    }

    [Fact]
    [CleanDatabase]
    public void GetHashCount_ReturnsZero_WhenKeyDoesNotExist()
    {
      UseConnection(connection => {
        long result = connection.GetHashCount("my-hash");
        Assert.Equal(0, result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetHashCount_ReturnsNumber_OfHashFields()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".hash (key, field) VALUES (@Key, @Field)";

      UseConnections((connection, jobStorageConnection) => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "hash-1", Field = "field-1" },
          new { Key = "hash-1", Field = "field-2" },
          new { Key = "hash-2", Field = "field-1" },
        });

        // Act
        long result = jobStorageConnection.GetHashCount("hash-1");

        // Assert
        Assert.Equal(2, result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetHashTtl_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetHashTtl(null)); });
    }

    [Fact]
    [CleanDatabase]
    public void GetHashTtl_ReturnsNegativeValue_WhenHashDoesNotExist()
    {
      UseConnection(connection => {
        TimeSpan result = connection.GetHashTtl("my-hash");
        Assert.True(result < TimeSpan.Zero);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetHashTtl_ReturnsExpirationTimeForHash()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".hash (key, field, expireat) VALUES (@Key, @Field, @ExpireAt)";

      UseConnections((connection, jobStorageConnection) => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "hash-1", Field = "field", ExpireAt = (DateTime?)DateTime.UtcNow.AddHours(1) },
          new { Key = "hash-2", Field = "field", ExpireAt = (DateTime?)null },
        });

        // Act
        TimeSpan result = jobStorageConnection.GetHashTtl("hash-1");

        // Assert
        Assert.True(TimeSpan.FromMinutes(59) < result);
        Assert.True(result < TimeSpan.FromMinutes(61));
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetRangeFromSet_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetRangeFromSet(null, 0, 1)); });
    }

    [Fact]
    [CleanDatabase]
    public void GetRangeFromSet_ReturnsPagedElements()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".set (key, value, score) VALUES (@Key, @Value, 0.0)";

      UseConnections((connection, jobStorageConnection) => {
        connection.Execute(arrangeSql, new[] {
          new { Key = "set-1", Value = "1" },
          new { Key = "set-1", Value = "2" },
          new { Key = "set-1", Value = "3" },
          new { Key = "set-1", Value = "4" },
          new { Key = "set-2", Value = "4" },
          new { Key = "set-1", Value = "5" },
        });

        List<string> result = jobStorageConnection.GetRangeFromSet("set-1", 2, 3);

        Assert.Equal(new[] { "3", "4" }, result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetSetTtl_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => connection.GetSetTtl(null)); });
    }

    [Fact]
    [CleanDatabase]
    public void GetSetTtl_ReturnsNegativeValue_WhenSetDoesNotExist()
    {
      UseConnection(connection => {
        TimeSpan result = connection.GetSetTtl("my-set");
        Assert.True(result < TimeSpan.Zero);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetSetTtl_ReturnsExpirationTime_OfAGivenSet()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".set (key, value, expireat, score) VALUES (@Key, @Value, @ExpireAt, 0.0)";

      UseConnections((connection, jobStorageConnection) => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "set-1", Value = "1", ExpireAt = (DateTime?)DateTime.UtcNow.AddMinutes(60) },
          new { Key = "set-2", Value = "2", ExpireAt = (DateTime?)null },
        });

        // Act
        TimeSpan result = jobStorageConnection.GetSetTtl("set-1");

        // Assert
        Assert.True(TimeSpan.FromMinutes(59) < result);
        Assert.True(result < TimeSpan.FromMinutes(61));
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetValueFromHash_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetValueFromHash(null, "name"));

        Assert.Equal("key", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetValueFromHash_ThrowsAnException_WhenNameIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetValueFromHash("key", null));

        Assert.Equal("name", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetValueFromHash_ReturnsNull_WhenHashDoesNotExist()
    {
      UseConnection(connection => {
        string result = connection.GetValueFromHash("my-hash", "name");
        Assert.Null(result);
      });
    }

    [Fact]
    [CleanDatabase]
    public void GetValueFromHash_ReturnsValue_OfAGivenField()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".hash (key, field, value) VALUES (@Key, @Field, @Value)";

      UseConnections((connection, jobStorageConnection) => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "hash-1", Field = "field-1", Value = "1" },
          new { Key = "hash-1", Field = "field-2", Value = "2" },
          new { Key = "hash-2", Field = "field-1", Value = "3" },
        });

        // Act
        string result = jobStorageConnection.GetValueFromHash("hash-1", "field-1");

        // Assert
        Assert.Equal("1", result);
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(false)]
    [InlineData(true)]
    public void CreateExpiredJob_EnlistsInTransaction(bool completeTransactionScope)
    {
      TransactionScope CreateTransactionScope()
      {
        TransactionOptions transactionOptions = new TransactionOptions() {
          IsolationLevel = IsolationLevel.ReadCommitted,
          Timeout = TransactionManager.MaximumTimeout,
        };

        return new TransactionScope(TransactionScopeOption.Required, transactionOptions);
      }

      string jobId = null;
      DateTime createdAt = new DateTime(2012, 12, 12, 0, 0, 0, DateTimeKind.Utc);
      using (TransactionScope scope = CreateTransactionScope())
      {
        UseConnections((_, connection) => {
          jobId = connection.CreateExpiredJob(Job.FromExpression(() => SampleMethod("Hello")),
            new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
            createdAt,
            TimeSpan.FromDays(1));

          Assert.NotNull(jobId);
          Assert.NotEmpty(jobId);
        });

        if (completeTransactionScope)
        {
          scope.Complete();
        }
      }

      UseConnections((connection, _) => {
        if (completeTransactionScope)
        {
          dynamic sqlJob = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""job""").Single();
          Assert.Equal(jobId, sqlJob.id.ToString());
          Assert.Equal(createdAt, sqlJob.createdat);
          Assert.Null((long?)sqlJob.stateid);
          Assert.Null((string)sqlJob.statename);
        }
        else
        {
          TestJob job = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""job""").SingleOrDefault();
          Assert.Null(job);
        }
      });
    }

    private void UseConnections(Action<NpgsqlConnection, PostgreSqlConnection> action)
    {
      PostgreSqlStorage storage = _fixture.SafeInit();
      action(storage.CreateAndOpenConnection(), storage.GetStorageConnection());
    }

    private void UseConnection(Action<PostgreSqlConnection> action)
    {
      PostgreSqlStorage storage = _fixture.SafeInit();
      action(storage.GetStorageConnection());
    }

    private static void UseDisposableConnection(Action<PostgreSqlConnection> action)
    {
      using (NpgsqlConnection sqlConnection = ConnectionUtils.CreateConnection())
      {
        PostgreSqlStorageOptions options = new()
        {
          EnableTransactionScopeEnlistment = true,
          SchemaName = GetSchemaName(),
          TransactionSynchronisationTimeout = TimeSpan.FromSeconds(1),
        };
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(sqlConnection, options), options);
        using (PostgreSqlConnection connection = storage.GetStorageConnection())
        {
          action(connection);
        }
      }
    }

    private static string GetSchemaName()
    {
      return ConnectionUtils.GetSchemaName();
    }

#pragma warning disable xUnit1013 // Public method should be marked as test
    public static void SampleMethod(string arg)
#pragma warning restore xUnit1013 // Public method should be marked as test
    { }
  }
}
