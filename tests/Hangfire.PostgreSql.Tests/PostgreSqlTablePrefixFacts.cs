using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Factories;
using Hangfire.PostgreSql.Tests.Entities;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
  public class PostgreSqlTablePrefixFacts : IClassFixture<PostgreSqlStorageFixture>
  {
    private readonly PostgreSqlStorageFixture _fixture;

    public PostgreSqlTablePrefixFacts(PostgreSqlStorageFixture fixture)
    {
      _fixture = fixture;
    }

    [Theory]
    [CleanDatabase]
    [InlineData(false, "")]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_ExpireJob_WorksWithAndWithoutPrefix(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);

        // Arrange
        string tableName = usePrefix ? prefix + "job" : "job";
        string arrangeSql = $@"
          INSERT INTO ""{schemaName}"".""{tableName}"" (""invocationdata"", ""arguments"", ""createdat"")
          VALUES ('{{}}', '[]', NOW()) RETURNING ""id""
        ";

        DateTime utcNow = DateTime.UtcNow;
        string jobId = connection.QuerySingle<long>(arrangeSql).ToString(CultureInfo.InvariantCulture);

        // Act
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.ExpireJob(jobId, TimeSpan.FromDays(1));
          transaction.Commit();
        }

        // Assert
        TestJob job = Helper.GetTestJob(connection, schemaName, jobId, usePrefix, prefix);
        Assert.NotNull(job.ExpireAt);
        Assert.True(utcNow.AddMinutes(-1) < job.ExpireAt && job.ExpireAt <= utcNow.AddDays(1).AddSeconds(5));

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(false, "")]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_PersistJob_WorksWithAndWithoutPrefix(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);

        // Arrange
        string tableName = usePrefix ? prefix + "job" : "job";
        string arrangeSql = $@"
          INSERT INTO ""{schemaName}"".""{tableName}"" (""invocationdata"", ""arguments"", ""createdat"", ""expireat"")
          VALUES ('{{}}', '[]', NOW(), NOW()) RETURNING ""id""
        ";

        string jobId = connection.QuerySingle<long>(arrangeSql).ToString(CultureInfo.InvariantCulture);

        // Act
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.PersistJob(jobId);
          transaction.Commit();
        }

        // Assert
        TestJob job = Helper.GetTestJob(connection, schemaName, jobId, usePrefix, prefix);
        Assert.Null(job.ExpireAt);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(false, "")]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_SetJobState_WorksWithAndWithoutPrefix(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);

        // Arrange
        string tableName = usePrefix ? prefix + "job" : "job";
        string stateTableName = usePrefix ? prefix + "state" : "state";
        string arrangeSql = $@"
          INSERT INTO ""{schemaName}"".""{tableName}"" (""invocationdata"", ""arguments"", ""createdat"")
          VALUES ('{{}}', '[]', NOW()) RETURNING ""id""
        ";

        string jobId = connection.QuerySingle<long>(arrangeSql).ToString(CultureInfo.InvariantCulture);

        Mock<IState> state = new();
        state.Setup(x => x.Name).Returns("TestState");
        state.Setup(x => x.Reason).Returns("TestReason");
        state.Setup(x => x.SerializeData()).Returns(new Dictionary<string, string> { { "Key", "Value" } });

        // Act
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.SetJobState(jobId, state.Object);
          transaction.Commit();
        }

        // Assert
        TestJob job = Helper.GetTestJob(connection, schemaName, jobId, usePrefix, prefix);
        Assert.Equal("TestState", job.StateName);
        Assert.NotNull(job.StateId);

        dynamic jobState = connection.QuerySingle($@"SELECT * FROM ""{schemaName}"".""{stateTableName}"" WHERE ""jobid"" = @JobId",
          new { JobId = long.Parse(jobId) });
        Assert.Equal("TestState", jobState.name);
        Assert.Equal("TestReason", jobState.reason);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(false, "")]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_AddToSet_WorksWithAndWithoutPrefix(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);

        // Arrange
        string key = "test-set";
        string value = "test-value";
        double score = 42.5;

        // Act
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.AddToSet(key, value, score);
          transaction.Commit();
        }

        // Assert
        string setTableName = usePrefix ? prefix + "set" : "set";
        dynamic result = connection.QuerySingle(
          $@"SELECT * FROM ""{schemaName}"".""{setTableName}"" WHERE ""key"" = @Key AND ""value"" = @Value",
          new { Key = key, Value = value });

        Assert.Equal(key, result.key);
        Assert.Equal(value, result.value);
        Assert.Equal(score, (double)result.score);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(false, "")]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_AddToQueue_WorksWithAndWithoutPrefix(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);

        // Arrange
        string tableName = usePrefix ? prefix + "job" : "job";
        string queueTableName = usePrefix ? prefix + "jobqueue" : "jobqueue";
        string arrangeSql = $@"
          INSERT INTO ""{schemaName}"".""{tableName}"" (""invocationdata"", ""arguments"", ""createdat"")
          VALUES ('{{}}', '[]', NOW()) RETURNING ""id""
        ";

        string jobId = connection.QuerySingle<long>(arrangeSql).ToString(CultureInfo.InvariantCulture);
        string queue = "default";

        // Act
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.AddToQueue(queue, jobId);
          transaction.Commit();
        }

        // Assert
        dynamic result = connection.QuerySingle(
          $@"SELECT * FROM ""{schemaName}"".""{queueTableName}"" WHERE ""jobid"" = @JobId",
          new { JobId = long.Parse(jobId) });

        Assert.Equal(queue, result.queue);
        Assert.Equal(long.Parse(jobId), (long)result.jobid);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(false, "")]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_IncrementCounter_WorksWithAndWithoutPrefix(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);

        // Arrange
        string key = "test-counter";

        // Act
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.IncrementCounter(key);
          transaction.Commit();
        }

        // Assert
        string counterTableName = usePrefix ? prefix + "counter" : "counter";
        int count = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{counterTableName}"" WHERE ""key"" = @Key",
          new { Key = key });

        Assert.Equal(1, count);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(false, "")]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_SetJobParameter_WorksWithAndWithoutPrefix(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        // Arrange
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);

        string tableName = usePrefix ? prefix + "job" : "job";
        string hashTableName = usePrefix ? prefix + "hash" : "hash";
        string arrangeSql = $@"
          INSERT INTO ""{schemaName}"".""{tableName}"" (""invocationdata"", ""arguments"", ""createdat"")
          VALUES ('{{}}', '[]', NOW()) RETURNING ""id""
        ";

        string jobId = connection.QuerySingle<long>(arrangeSql).ToString(CultureInfo.InvariantCulture);
        string parameterName = "test-parameter";
        string parameterValue = "test-value";

        // Act
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.SetRangeInHash($"job:{jobId}:parameters", new[] { new KeyValuePair<string, string>(parameterName, parameterValue) });
          transaction.Commit();
        }

        // Assert
        string result = connection.QuerySingle<string>(
          $@"SELECT ""value"" FROM ""{schemaName}"".""{hashTableName}"" WHERE ""key"" = @Key AND ""field"" = @Field",
          new { Key = $"job:{jobId}:parameters", Field = parameterName });

        Assert.Equal(parameterValue, result);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(false, "")]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_ComplexOperation_WorksWithAndWithoutPrefix(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);

        // Arrange
        string tableName = usePrefix ? prefix + "job" : "job";
        string hashTableName = usePrefix ? prefix + "hash" : "hash";
        string setTableName = usePrefix ? prefix + "set" : "set";
        string counterTableName = usePrefix ? prefix + "counter" : "counter";

        string arrangeSql = $@"
          INSERT INTO ""{schemaName}"".""{tableName}"" (""invocationdata"", ""arguments"", ""createdat"")
          VALUES ('{{}}', '[]', NOW()) RETURNING ""id""
        ";

        string jobId = connection.QuerySingle<long>(arrangeSql).ToString(CultureInfo.InvariantCulture);

        // Act - Perform multiple operations in a single transaction
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.SetRangeInHash($"job:{jobId}:metadata", new[] {
            new KeyValuePair<string, string>("key1", "value1"),
            new KeyValuePair<string, string>("key2", "value2")
          });
          transaction.AddToSet("test-set", "value1", 1.0);
          transaction.AddToSet("test-set", "value2", 2.0);
          transaction.IncrementCounter("test-counter");
          transaction.IncrementCounter("test-counter");
          transaction.ExpireJob(jobId, TimeSpan.FromHours(1));
          transaction.Commit();
        }

        // Assert
        int hashCount = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{hashTableName}"" WHERE ""key"" = @Key",
          new { Key = $"job:{jobId}:metadata" });
        Assert.Equal(2, hashCount);

        int setCount = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{setTableName}"" WHERE ""key"" = @Key",
          new { Key = "test-set" });
        Assert.Equal(2, setCount);

        int counterCount = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{counterTableName}"" WHERE ""key"" = @Key",
          new { Key = "test-counter" });
        Assert.Equal(2, counterCount);

        TestJob job = Helper.GetTestJob(connection, schemaName, jobId, usePrefix, prefix);
        Assert.NotNull(job.ExpireAt);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(false, "")]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_MultipleJobsWithStates_WorksWithAndWithoutPrefix(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);

        // Arrange
        string tableName = usePrefix ? prefix + "job" : "job";
        string stateTableName = usePrefix ? prefix + "state" : "state";
        string jobQueueTableName = usePrefix ? prefix + "jobqueue" : "jobqueue";

        List<string> jobIds = new();
        for (int i = 0; i < 3; i++)
        {
          string sql = $@"
            INSERT INTO ""{schemaName}"".""{tableName}"" (""invocationdata"", ""arguments"", ""createdat"")
            VALUES ('{{}}', '[]', NOW()) RETURNING ""id""
          ";
          jobIds.Add(connection.QuerySingle<long>(sql).ToString(CultureInfo.InvariantCulture));
        }

        // Act - Create multiple jobs with different states and queue them
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          Mock<IState> enqueuedState = new();
          enqueuedState.Setup(x => x.Name).Returns("Enqueued");
          enqueuedState.Setup(x => x.Reason).Returns((string)null);
          enqueuedState.Setup(x => x.SerializeData()).Returns(new Dictionary<string, string> { { "Queue", "default" } });

          Mock<IState> processingState = new();
          processingState.Setup(x => x.Name).Returns("Processing");
          processingState.Setup(x => x.Reason).Returns((string)null);
          processingState.Setup(x => x.SerializeData()).Returns(new Dictionary<string, string> { { "WorkerId", "worker1" } });

          Mock<IState> failedState = new();
          failedState.Setup(x => x.Name).Returns("Failed");
          failedState.Setup(x => x.Reason).Returns("Exception occurred");
          failedState.Setup(x => x.SerializeData()).Returns(new Dictionary<string, string> { { "ExceptionMessage", "Test error" } });

          transaction.SetJobState(jobIds[0], enqueuedState.Object);
          transaction.AddToQueue("default", jobIds[0]);
          
          transaction.SetJobState(jobIds[1], processingState.Object);
          
          transaction.SetJobState(jobIds[2], failedState.Object);
          
          transaction.Commit();
        }

        // Assert
        int enqueuedCount = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{stateTableName}"" WHERE ""name"" = @Name",
          new { Name = "Enqueued" });
        Assert.Equal(1, enqueuedCount);

        int processingCount = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{stateTableName}"" WHERE ""name"" = @Name",
          new { Name = "Processing" });
        Assert.Equal(1, processingCount);

        int failedCount = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{stateTableName}"" WHERE ""name"" = @Name",
          new { Name = "Failed" });
        Assert.Equal(1, failedCount);

        int queuedJobsCount = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{jobQueueTableName}""");
        Assert.Equal(1, queuedJobsCount);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_WithNonExistentJobId_DoesNotThrow(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);

        // Act - Should not throw even if job doesn't exist
        Exception ex = Record.Exception(() => {
          using IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction();
          transaction.ExpireJob("999999", TimeSpan.FromDays(1));
          transaction.Commit();
        });

        // Assert
        Assert.Null(ex);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_AddToSet_DuplicateValueUpdatesScore(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);

        // Arrange
        string key = "test-set";
        string value = "test-value";
        string setTableName = usePrefix ? prefix + "set" : "set";

        // Act - Add same value twice with different scores
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.AddToSet(key, value, 1.0);
          transaction.Commit();
        }

        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.AddToSet(key, value, 2.0);
          transaction.Commit();
        }

        // Assert - Should have only one record with updated score
        int count = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{setTableName}"" WHERE ""key"" = @Key AND ""value"" = @Value",
          new { Key = key, Value = value });
        Assert.Equal(1, count);

        double score = connection.QuerySingle<double>(
          $@"SELECT ""score"" FROM ""{schemaName}"".""{setTableName}"" WHERE ""key"" = @Key AND ""value"" = @Value",
          new { Key = key, Value = value });
        Assert.Equal(2.0, score);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_RemoveFromSet_WorksWithPrefix(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);

        // Arrange
        string key = "test-set";
        string value1 = "value1";
        string value2 = "value2";
        string setTableName = usePrefix ? prefix + "set" : "set";

        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.AddToSet(key, value1, 1.0);
          transaction.AddToSet(key, value2, 2.0);
          transaction.Commit();
        }

        // Act
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.RemoveFromSet(key, value1);
          transaction.Commit();
        }

        // Assert
        int count = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{setTableName}"" WHERE ""key"" = @Key",
          new { Key = key });
        Assert.Equal(1, count);

        string remainingValue = connection.QuerySingle<string>(
          $@"SELECT ""value"" FROM ""{schemaName}"".""{setTableName}"" WHERE ""key"" = @Key",
          new { Key = key });
        Assert.Equal(value2, remainingValue);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_InsertToList_WorksWithPrefix(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);

        // Arrange
        string key = "test-list";
        string listTableName = usePrefix ? prefix + "list" : "list";

        // Act
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.InsertToList(key, "value1");
          transaction.InsertToList(key, "value2");
          transaction.InsertToList(key, "value3");
          transaction.Commit();
        }

        // Assert
        int count = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{listTableName}"" WHERE ""key"" = @Key",
          new { Key = key });
        Assert.Equal(3, count);

        List<string> values = connection.Query<string>(
          $@"SELECT ""value"" FROM ""{schemaName}"".""{listTableName}"" WHERE ""key"" = @Key ORDER BY ""id"" DESC",
          new { Key = key }).ToList();
        
        Assert.Equal(new[] { "value3", "value2", "value1" }, values);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_WithEmptyKey_DoesNotThrow(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);
        string setTableName = usePrefix ? prefix + "set" : "set";

        // Act - Empty key should be allowed
        Exception ex = Record.Exception(() => {
          using IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction();
          transaction.AddToSet("", "value", 1.0);
          transaction.Commit();
        });

        // Assert
        Assert.Null(ex);

        int count = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{setTableName}"" WHERE ""key"" = @Key",
          new { Key = "" });
        Assert.Equal(1, count);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_MultipleUpdatesToSameHash_WorksCorrectly(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);
        string hashTableName = usePrefix ? prefix + "hash" : "hash";
        string key = "test-hash";

        // Act - Update same field multiple times in same transaction
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.SetRangeInHash(key, new[] {
            new KeyValuePair<string, string>("field1", "value1")
          });
          transaction.SetRangeInHash(key, new[] {
            new KeyValuePair<string, string>("field1", "value2")
          });
          transaction.SetRangeInHash(key, new[] {
            new KeyValuePair<string, string>("field1", "value3")
          });
          transaction.Commit();
        }

        // Assert - Should have the last value
        string result = connection.QuerySingle<string>(
          $@"SELECT ""value"" FROM ""{schemaName}"".""{hashTableName}"" WHERE ""key"" = @Key AND ""field"" = @Field",
          new { Key = key, Field = "field1" });
        
        Assert.Equal("value3", result);

        int count = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{hashTableName}"" WHERE ""key"" = @Key",
          new { Key = key });
        Assert.Equal(1, count);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_IncrementCounter_WithTimeToLive_WorksCorrectly(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);
        string counterTableName = usePrefix ? prefix + "counter" : "counter";
        string key = "test-counter-ttl";
        DateTime utcNow = DateTime.UtcNow;

        // Act
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.IncrementCounter(key, TimeSpan.FromHours(2));
          transaction.Commit();
        }

        // Assert
        int count = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{counterTableName}"" WHERE ""key"" = @Key",
          new { Key = key });
        Assert.Equal(1, count);

        DateTime? expireAt = connection.QuerySingle<DateTime?>(
          $@"SELECT ""expireat"" FROM ""{schemaName}"".""{counterTableName}"" WHERE ""key"" = @Key",
          new { Key = key });
        
        Assert.NotNull(expireAt);
        Assert.True(expireAt.Value > utcNow.AddHours(1).AddMinutes(59) && expireAt.Value <= utcNow.AddHours(2).AddMinutes(1));

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_DecrementCounter_WorksCorrectly(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);
        string counterTableName = usePrefix ? prefix + "counter" : "counter";
        string key = "test-decrement-counter";

        // Arrange - Add some counters first
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.IncrementCounter(key);
          transaction.IncrementCounter(key);
          transaction.IncrementCounter(key);
          transaction.Commit();
        }

        // Act - Decrement
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.DecrementCounter(key);
          transaction.Commit();
        }

        // Assert
        int count = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{counterTableName}"" WHERE ""key"" = @Key AND ""value"" > 0",
          new { Key = key });
        Assert.Equal(3, count);

        int negativeCount = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{counterTableName}"" WHERE ""key"" = @Key AND ""value"" < 0",
          new { Key = key });
        Assert.Equal(1, negativeCount);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_RemoveFromList_WorksCorrectly(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);
        string listTableName = usePrefix ? prefix + "list" : "list";
        string key = "test-list";

        // Arrange
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.InsertToList(key, "value1");
          transaction.InsertToList(key, "value2");
          transaction.InsertToList(key, "value3");
          transaction.InsertToList(key, "value2"); // duplicate
          transaction.Commit();
        }

        // Act - Remove one instance of value2
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.RemoveFromList(key, "value2");
          transaction.Commit();
        }

        // Assert
        int totalCount = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{listTableName}"" WHERE ""key"" = @Key",
          new { Key = key });
        Assert.Equal(3, totalCount);

        int value2Count = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{listTableName}"" WHERE ""key"" = @Key AND ""value"" = @Value",
          new { Key = key, Value = "value2" });
        Assert.Equal(1, value2Count);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_TrimList_WorksCorrectly(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);
        string listTableName = usePrefix ? prefix + "list" : "list";
        string key = "test-list";

        // Arrange - Add 10 items
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          for (int i = 0; i < 10; i++)
          {
            transaction.InsertToList(key, $"value{i}");
          }
          transaction.Commit();
        }

        // Act - Keep only first 5
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.TrimList(key, 0, 4);
          transaction.Commit();
        }

        // Assert
        int count = connection.QuerySingle<int>(
          $@"SELECT COUNT(*) FROM ""{schemaName}"".""{listTableName}"" WHERE ""key"" = @Key",
          new { Key = key });
        Assert.Equal(5, count);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(false, "")]
    [InlineData(true, "hf_")]
    public void WriteOnlyTransaction_AddJobState_WithLongData_WorksCorrectly(bool usePrefix, string prefix)
    {
      string schemaName = "hangfire_test_" + Guid.NewGuid().ToString();
      PostgreSqlStorageOptions options = new() {
        SchemaName = schemaName,
        EnableTransactionScopeEnlistment = true,
        UseTablePrefix = usePrefix,
        TablePrefixName = usePrefix ? prefix : string.Empty,
        PrepareSchemaIfNecessary = true,
      };

      UseConnection(connection => {
        PostgreSqlStorage storage = new(new ExistingNpgsqlConnectionFactory(connection, options), options);
        string tableName = usePrefix ? prefix + "job" : "job";
        string stateTableName = usePrefix ? prefix + "state" : "state";

        // Arrange
        string arrangeSql = $@"
          INSERT INTO ""{schemaName}"".""{tableName}"" (""invocationdata"", ""arguments"", ""createdat"")
          VALUES ('{{}}', '[]', NOW()) RETURNING ""id""
        ";
        string jobId = connection.QuerySingle<long>(arrangeSql).ToString(CultureInfo.InvariantCulture);

        // Create large data
        Dictionary<string, string> largeData = new();
        for (int i = 0; i < 100; i++)
        {
          largeData[$"Key{i}"] = $"Value{i}_" + new string('X', 100);
        }

        Mock<IState> state = new();
        state.Setup(x => x.Name).Returns("ProcessingWithLargeData");
        state.Setup(x => x.Reason).Returns("Testing with large data payload");
        state.Setup(x => x.SerializeData()).Returns(largeData);

        // Act
        using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
        {
          transaction.AddJobState(jobId, state.Object);
          transaction.Commit();
        }

        // Assert
        dynamic jobState = connection.QuerySingle(
          $@"SELECT * FROM ""{schemaName}"".""{stateTableName}"" WHERE ""jobid"" = @JobId",
          new { JobId = long.Parse(jobId) });
        
        Assert.Equal("ProcessingWithLargeData", jobState.name);
        Assert.NotNull(jobState.data);
        
        string dataJson = jobState.data;
        Dictionary<string, string> deserializedData = JsonSerializer.Deserialize<Dictionary<string, string>>(dataJson);
        Assert.Equal(100, deserializedData.Count);

        // connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
      });
    }

    private static void UseConnection(Action<NpgsqlConnection> action)
    {
      using NpgsqlConnection connection = ConnectionUtils.CreateConnection();
      action(connection);
    }
  }
}