using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Tests.Extensions;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Npgsql;
using Xunit;
using IsolationLevel = System.Transactions.IsolationLevel;

namespace Hangfire.PostgreSql.Tests
{
  public class PostgreSqlWriteOnlyTransactionFacts : IClassFixture<PostgreSqlStorageFixture>
  {
    private readonly PostgreSqlStorageFixture _fixture;

    public PostgreSqlWriteOnlyTransactionFacts(PostgreSqlStorageFixture fixture)
    {
      _fixture = fixture;
    }

    [Fact]
    public void Ctor_ThrowsAnException_IfStorageIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new PostgreSqlWriteOnlyTransaction(null, () => null));

      Assert.Equal("storage", exception.ParamName);
    }

    [Fact]
    public void Ctor_ThrowsAnException_IfDedicatedConnectionFuncIsNull()
    {
      PostgreSqlStorageOptions options = new PostgreSqlStorageOptions { EnableTransactionScopeEnlistment = true };
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        new PostgreSqlWriteOnlyTransaction(new PostgreSqlStorage(ConnectionUtils.CreateConnection(), options), null));

      Assert.Equal("dedicatedConnectionFunc", exception.ParamName);
    }

    [Fact]
    [CleanDatabase]
    public void ExpireJob_SetsJobExpirationData()
    {

      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""{"job".GetProperDbObjectName()}"" 
        (""{"invocationdata".GetProperDbObjectName()}"", ""{"arguments".GetProperDbObjectName()}"", ""{"createdat".GetProperDbObjectName()}"")
        VALUES ('', '', @When) RETURNING ""{"id".GetProperDbObjectName()}""
      ";

      UseConnection(connection => {
        DateTime utcNow = DateTime.UtcNow;
        string jobId = connection.QuerySingle<long>(arrangeSql, new { When = utcNow }).ToString(CultureInfo.InvariantCulture);
        string anotherJobId = connection.QuerySingle<long>(arrangeSql, new { When = utcNow }).ToString(CultureInfo.InvariantCulture);

        Commit(connection, x => x.ExpireJob(jobId, TimeSpan.FromDays(1)));

        TestJob job = Helper.GetTestJob(connection, GetSchemaName(), jobId, DbQueryHelper.IsUpperCase);
        Assert.True(utcNow.AddMinutes(-1) < job.ExpireAt && job.ExpireAt <= utcNow.AddDays(1).AddSeconds(5));

        TestJob anotherJob = Helper.GetTestJob(connection, GetSchemaName(), anotherJobId, DbQueryHelper.IsUpperCase);
        Assert.Null(anotherJob.ExpireAt);
      });
    }

    [Fact]
    [CleanDatabase]
    public void PersistJob_ClearsTheJobExpirationData()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""{"job".GetProperDbObjectName()}"" 
        (""{"invocationdata".GetProperDbObjectName()}"", ""{"arguments".GetProperDbObjectName()}"", ""{"createdat".GetProperDbObjectName()}"", ""{"expireat".GetProperDbObjectName()}"")
        VALUES ('', '', NOW() AT TIME ZONE 'UTC', NOW() AT TIME ZONE 'UTC') RETURNING ""{"id".GetProperDbObjectName()}""
      ";

      UseConnection(connection => {
        string jobId = connection.QuerySingle<long>(arrangeSql).ToString(CultureInfo.InvariantCulture);
        string anotherJobId = connection.QuerySingle<long>(arrangeSql).ToString(CultureInfo.InvariantCulture);

        Commit(connection, x => x.PersistJob(jobId));

        TestJob job = Helper.GetTestJob(connection, GetSchemaName(), jobId, DbQueryHelper.IsUpperCase);
        Assert.Null(job.ExpireAt);

        TestJob anotherJob = Helper.GetTestJob(connection, GetSchemaName(), anotherJobId, DbQueryHelper.IsUpperCase);
        Assert.NotNull(anotherJob.ExpireAt);
      });
    }

    [Fact]
    [CleanDatabase]
    public void SetJobState_AppendsAStateAndSetItToTheJob()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""{"job".GetProperDbObjectName()}"" 
        (""{"invocationdata".GetProperDbObjectName()}"", ""{"arguments".GetProperDbObjectName()}"", ""{"createdat".GetProperDbObjectName()}"")
        VALUES ('', '', NOW() AT TIME ZONE 'UTC') RETURNING ""{"id".GetProperDbObjectName()}""";

      UseConnection(connection => {
        var query = connection.Query(arrangeSql).Single();
        var anotherQuery = connection.Query(arrangeSql).Single();
        var jobId = "";
        var anotherJobId = "";
        if (DbQueryHelper.IsUpperCase)
        {
          jobId = query.ID.ToString();
          anotherJobId = anotherQuery.ID.ToString();
        }
        else
        {
          jobId = query.id.ToString();
          anotherJobId = anotherQuery.id.ToString();
        }

        Mock<IState> state = new Mock<IState>();
        state.Setup(x => x.Name).Returns("State");
        state.Setup(x => x.Reason).Returns("Reason");
        state.Setup(x => x.SerializeData())
          .Returns(new Dictionary<string, string> { { "Name", "Value" } });

        Commit(connection, x => x.SetJobState(jobId, state.Object));

        TestJob job = Helper.GetTestJob(connection, GetSchemaName(), jobId, DbQueryHelper.IsUpperCase);

        Assert.Equal("State", job.StateName);
        Assert.NotNull(job.StateId);

        TestJob anotherJob = Helper.GetTestJob(connection, GetSchemaName(), anotherJobId, DbQueryHelper.IsUpperCase);
        Assert.Null(anotherJob.StateName);
        Assert.Null(anotherJob.StateId);

        dynamic jobState = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"state".GetProperDbObjectName()}""").Single();
        Assert.Equal((string)jobId, DbQueryHelper.IsUpperCase ? jobState.JOBID.ToString() : jobState.jobid.ToString());
        Assert.Equal("State", DbQueryHelper.IsUpperCase ? jobState.NAME : jobState.name);
        Assert.Equal("Reason", DbQueryHelper.IsUpperCase ? jobState.REASON : jobState.reason);
        Assert.NotNull(DbQueryHelper.IsUpperCase ? jobState.CREATEDAT : jobState.createdat);
        Assert.Equal("{\"Name\":\"Value\"}", DbQueryHelper.IsUpperCase ? jobState.DATA : jobState.data);
      });
    }

    [Theory]
    [CleanDatabase]
    [InlineData(false)]
    [InlineData(true)]
    public void SetJobState_EnlistsInAmbientTransaction(bool completeTransactionScope)
    {
      TransactionScope CreateTransactionScope(IsolationLevel isolationLevel = IsolationLevel.RepeatableRead)
      {
        TransactionOptions transactionOptions = new TransactionOptions() {
          IsolationLevel = isolationLevel,
          Timeout = TransactionManager.MaximumTimeout,
        };

        return new TransactionScope(TransactionScopeOption.Required, transactionOptions);
      }

      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""{"job".GetProperDbObjectName()}"" 
        (""{"invocationdata".GetProperDbObjectName()}"", ""{"arguments".GetProperDbObjectName()}"", ""{"createdat".GetProperDbObjectName()}"")
        VALUES ('', '', NOW() AT TIME ZONE 'UTC') RETURNING ""{"id".GetProperDbObjectName()}""";


      string jobId = null;
      string anotherJobId = null;
      UseConnection(connection => {
        var query = connection.Query(arrangeSql).Single();
        var anotherQuery = connection.Query(arrangeSql).Single();
        if (DbQueryHelper.IsUpperCase)
        {
          jobId = query.ID.ToString();
          anotherJobId = anotherQuery.ID.ToString();
        }
        else
        {
          jobId = query.id.ToString();
          anotherJobId = anotherQuery.id.ToString();
        }
      });

      using (TransactionScope scope = CreateTransactionScope())
      {
        UseConnection(connection => {
          Mock<IState> state = new Mock<IState>();
          state.Setup(x => x.Name).Returns("State");
          state.Setup(x => x.Reason).Returns("Reason");
          state.Setup(x => x.SerializeData())
            .Returns(new Dictionary<string, string> { { "Name", "Value" } });

          Commit(connection, x => x.SetJobState(jobId, state.Object));
        });
        if (completeTransactionScope)
        {
          scope.Complete();
        }
      }

      UseConnection(connection => {
        TestJob job = Helper.GetTestJob(connection, GetSchemaName(), jobId, DbQueryHelper.IsUpperCase);
        if (completeTransactionScope)
        {
          Assert.Equal("State", job.StateName);
          Assert.NotNull(job.StateId);

          dynamic jobState = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"state".GetProperDbObjectName()}""").Single();         
          Assert.Equal(jobId, DbQueryHelper.IsUpperCase ? jobState.JOBID.ToString() : jobState.jobid.ToString());
          Assert.Equal("State", DbQueryHelper.IsUpperCase ? jobState.NAME : jobState.name);
          Assert.Equal("Reason", DbQueryHelper.IsUpperCase ? jobState.REASON : jobState.reason);
          Assert.NotNull(DbQueryHelper.IsUpperCase ? jobState.CREATEDAT : jobState.createdat);
          Assert.Equal("{\"Name\":\"Value\"}", DbQueryHelper.IsUpperCase ? jobState.DATA : jobState.data);
        }
        else
        {
          Assert.Null(job.StateName);
          Assert.Null(job.StateId);

          Assert.Null(connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"state".GetProperDbObjectName()}""").SingleOrDefault());
        }

        TestJob anotherJob = Helper.GetTestJob(connection, GetSchemaName(), anotherJobId, DbQueryHelper.IsUpperCase);
        Assert.Null(anotherJob.StateName);
        Assert.Null(anotherJob.StateId);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AddJobState_JustAddsANewRecordInATable()
    {
      string arrangeSql = $@"
        INSERT INTO ""{GetSchemaName()}"".""{"job".GetProperDbObjectName()}"" 
        (""{"invocationdata".GetProperDbObjectName()}"", ""{"arguments".GetProperDbObjectName()}"", ""{"createdat".GetProperDbObjectName()}"")
        VALUES ('', '', NOW() AT TIME ZONE 'UTC')
        RETURNING ""{"id".GetProperDbObjectName()}""
      ";

      UseConnection(connection => {
        dynamic query = connection.Query(arrangeSql).Single();

        string jobId = "";
        if (DbQueryHelper.IsUpperCase)
        {
          jobId = query.ID.ToString(CultureInfo.InvariantCulture);
        }
        else
        {
          jobId = query.id.ToString(CultureInfo.InvariantCulture);
        }

        Mock<IState> state = new Mock<IState>();
        state.Setup(x => x.Name).Returns("State");
        state.Setup(x => x.Reason).Returns("Reason");
        state.Setup(x => x.SerializeData())
          .Returns(new Dictionary<string, string> { { "Name", "Value" } });

        Commit(connection, x => x.AddJobState(jobId, state.Object));

        TestJob job = Helper.GetTestJob(connection, GetSchemaName(), jobId, DbQueryHelper.IsUpperCase);
        Assert.Null(job.StateName);
        Assert.Null(job.StateId);

        dynamic jobState = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"state".GetProperDbObjectName()}""").Single();

        Assert.Equal((string)jobId, DbQueryHelper.IsUpperCase ? jobState.JOBID.ToString(CultureInfo.InvariantCulture) : jobState.jobid.ToString(CultureInfo.InvariantCulture));
        Assert.Equal("State", DbQueryHelper.IsUpperCase ? jobState.NAME : jobState.name);
        Assert.Equal("Reason", DbQueryHelper.IsUpperCase ? jobState.REASON : jobState.reason);
        Assert.NotNull(DbQueryHelper.IsUpperCase ? jobState.CREATEDAT : jobState.createdat);
        Assert.Equal("{\"Name\":\"Value\"}", DbQueryHelper.IsUpperCase ? jobState.DATA : jobState.data);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AddToQueue_CallsEnqueue_OnTargetPersistentQueue()
    {
      UseConnection(connection => {
        Mock<IPersistentJobQueue> correctJobQueue = new Mock<IPersistentJobQueue>();
        Mock<IPersistentJobQueueProvider> correctProvider = new Mock<IPersistentJobQueueProvider>();
        correctProvider.Setup(x => x.GetJobQueue())
          .Returns(correctJobQueue.Object);

        _fixture.PersistentJobQueueProviderCollection.Add(correctProvider.Object, new[] { "default" });

        try
        {
          Commit(connection, x => x.AddToQueue("default", "1"));

          correctJobQueue.Verify(x => x.Enqueue(connection, "default", "1"));
        }
        finally
        {
          _fixture.PersistentJobQueueProviderCollection.Remove("default");
        }
      });
    }


    [Fact]
    [CleanDatabase]
    public void IncrementCounter_AddsRecordToCounterTable_WithPositiveValue()
    {
      UseConnection(connection => {
        Commit(connection, x => x.IncrementCounter("my-key"));

        dynamic record = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}""").Single();

        Assert.Equal("my-key", DbQueryHelper.IsUpperCase ? record.KEY : record.key);
        Assert.Equal(1, DbQueryHelper.IsUpperCase ? record.VALUE : record.value);
        Assert.Equal((DateTime?)null, DbQueryHelper.IsUpperCase ? record.EXPIREAT : record.expireat);
      });
    }

    [Fact]
    [CleanDatabase]
    public void IncrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
    {
      UseConnection(connection => {
        Commit(connection, x => x.IncrementCounter("my-key", TimeSpan.FromDays(1)));

        dynamic record = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}""").Single();

        Assert.Equal("my-key", DbQueryHelper.IsUpperCase ? record.KEY : record.key);
        Assert.Equal(1, DbQueryHelper.IsUpperCase ? record.VALUE : record.value);
        Assert.NotNull(DbQueryHelper.IsUpperCase ? record.EXPIREAT : record.expireat);

        DateTime expireAt = DbQueryHelper.IsUpperCase ? (DateTime)record.EXPIREAT : (DateTime)record.expireat;

        Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
        Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
      });
    }

    [Fact]
    [CleanDatabase]
    public void IncrementCounter_WithExistingKey_AddsAnotherRecord()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.IncrementCounter("my-key");
          x.IncrementCounter("my-key");
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}""");

        Assert.Equal(2, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void DecrementCounter_AddsRecordToCounterTable_WithNegativeValue()
    {
      UseConnection(connection => {
        Commit(connection, x => x.DecrementCounter("my-key"));

        dynamic record = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}""").Single();

        Assert.Equal("my-key", DbQueryHelper.IsUpperCase ? record.KEY : record.key);
        Assert.Equal(-1, DbQueryHelper.IsUpperCase ? record.VALUE : record.value);
        Assert.Equal((DateTime?)null, DbQueryHelper.IsUpperCase ? record.EXPIREAT : record.expireat);
      });
    }

    [Fact]
    [CleanDatabase]
    public void DecrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
    {
      UseConnection(connection => {
        Commit(connection, x => x.DecrementCounter("my-key", TimeSpan.FromDays(1)));

        dynamic record = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}""").Single();

        Assert.Equal("my-key", DbQueryHelper.IsUpperCase ? record.KEY : record.key);
        Assert.Equal(-1, DbQueryHelper.IsUpperCase ? record.VALUE : record.value);
        Assert.NotNull(DbQueryHelper.IsUpperCase ? record.EXPIREAT : record.expireat);

        DateTime expireAt = DbQueryHelper.IsUpperCase ? (DateTime)record.EXPIREAT : (DateTime)record.expireat;

        Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
        Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
      });
    }

    [Fact]
    [CleanDatabase]
    public void DecrementCounter_WithExistingKey_AddsAnotherRecord()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.DecrementCounter("my-key");
          x.DecrementCounter("my-key");
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"counter".GetProperDbObjectName()}""");

        Assert.Equal(2, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AddToSet_AddsARecord_IfThereIsNo_SuchKeyAndValue()
    {
      UseConnection(connection => {
        Commit(connection, x => x.AddToSet("my-key", "my-value"));

        dynamic record = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""").Single();

        Assert.Equal("my-key", DbQueryHelper.IsUpperCase ? record.KEY : record.key);
        Assert.Equal("my-value", DbQueryHelper.IsUpperCase ? record.VALUE : record.value);
        Assert.Equal(0.0, DbQueryHelper.IsUpperCase ? record.SCORE : record.score, 2);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AddToSet_AddsARecord_WhenKeyIsExists_ButValuesAreDifferent()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.AddToSet("my-key", "my-value");
          x.AddToSet("my-key", "another-value");
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""");

        Assert.Equal(2, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AddToSet_DoesNotAddARecord_WhenBothKeyAndValueAreExist()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.AddToSet("my-key", "my-value");
          x.AddToSet("my-key", "my-value");
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""");

        Assert.Equal(1, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AddToSet_WithScore_AddsARecordWithScore_WhenBothKeyAndValueAreNotExist()
    {
      UseConnection(connection => {
        Commit(connection, x => x.AddToSet("my-key", "my-value", 3.2));

        dynamic record = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""").Single();

        Assert.Equal("my-key", DbQueryHelper.IsUpperCase ? record.KEY : record.key);
        Assert.Equal("my-value", DbQueryHelper.IsUpperCase ? record.VALUE : record.value);
        Assert.Equal(3.2, DbQueryHelper.IsUpperCase ? record.SCORE : record.score, 3);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AddToSet_WithScore_UpdatesAScore_WhenBothKeyAndValueAreExist()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.AddToSet("my-key", "my-value");
          x.AddToSet("my-key", "my-value", 3.2);
        });

        dynamic record = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""").Single();

        Assert.Equal(3.2, DbQueryHelper.IsUpperCase ? record.SCORE : record.score, 3);
      });
    }

    [SkippableFact]
    [CleanDatabase]
    public void AddToSet_DoesNotFailWithConcurrencyError_WhenRunningMultipleThreads()
    {
      if (Environment.ProcessorCount < 2)
      {
        throw new SkipException("You need to have more than 1 CPU to run the test");
      }

      void CommitTags(PostgreSqlWriteOnlyTransaction transaction, IEnumerable<string> tags, string jobId)
      {
        //Imitating concurrency issue scenario from Hangfire.Tags library.
        //Details: https://github.com/frankhommers/Hangfire.PostgreSql/issues/191

        foreach (string tag in tags)
        {
          long score = DateTime.Now.Ticks;

          transaction.AddToSet("tags", tag, score);
          transaction.AddToSet($"tags:{jobId}", tag, score);
          transaction.AddToSet($"tags:{tag}", jobId, score);
        }
      }

      const int loopIterations = 1_000;
      const int jobGroups = 10;
      const int totalTagsCount = 2;

      Parallel.For(1, 1 + loopIterations, i => {
        UseDisposableConnection(sql => {
          CommitDisposable(sql, x => {
            int jobTypeIndex = i % jobGroups;
            CommitTags(x, new[] { "my-shared-tag", $"job-type-{jobTypeIndex}" }, i.ToString(CultureInfo.InvariantCulture));
          });
        });
      });

      UseConnection(connection => {
        int jobsCountUnderMySharedTag = connection.Query<int>($@"
          SELECT COUNT(*) 
          FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""
          WHERE ""{"key".GetProperDbObjectName()}"" LIKE 'tags:my-shared-tag'").Single();
        Assert.Equal(loopIterations, jobsCountUnderMySharedTag);


        int[] jobsCountsUnderJobTypeTags = connection.Query<int>($@"
          SELECT COUNT(*)
          FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""
          where ""{"key".GetProperDbObjectName()}"" like 'tags:job-type-%'
          group by ""{"key".GetProperDbObjectName()}"";").ToArray();

        Assert.All(jobsCountsUnderJobTypeTags, count => Assert.Equal(loopIterations / jobGroups, count));

        int jobLinkTagsCount = connection.Query<int>($@"
          SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""
          where ""{"value".GetProperDbObjectName()}"" ~ '^\d+$'
        ").Single();

        Assert.Equal(loopIterations * totalTagsCount, jobLinkTagsCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void RemoveFromSet_RemovesARecord_WithGivenKeyAndValue()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.AddToSet("my-key", "my-value");
          x.RemoveFromSet("my-key", "my-value");
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""");

        Assert.Equal(0, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void RemoveFromSet_DoesNotRemoveRecord_WithSameKey_AndDifferentValue()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.AddToSet("my-key", "my-value");
          x.RemoveFromSet("my-key", "different-value");
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""");

        Assert.Equal(1, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void RemoveFromSet_DoesNotRemoveRecord_WithSameValue_AndDifferentKey()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.AddToSet("my-key", "my-value");
          x.RemoveFromSet("different-key", "my-value");
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""");

        Assert.Equal(1, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void InsertToList_AddsARecord_WithGivenValues()
    {
      UseConnection(connection => {
        Commit(connection, x => x.InsertToList("my-key", "my-value"));

        dynamic record = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""").Single();

        Assert.Equal("my-key", DbQueryHelper.IsUpperCase ? record.KEY : record.key);
        Assert.Equal("my-value", DbQueryHelper.IsUpperCase ? record.VALUE : record.value);
      });
    }

    [Fact]
    [CleanDatabase]
    public void InsertToList_AddsAnotherRecord_WhenBothKeyAndValueAreExist()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.InsertToList("my-key", "my-value");
          x.InsertToList("my-key", "my-value");
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""");
        Assert.Equal(2, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void RemoveFromList_RemovesAllRecords_WithGivenKeyAndValue()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.InsertToList("my-key", "my-value");
          x.InsertToList("my-key", "my-value");
          x.RemoveFromList("my-key", "my-value");
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""");

        Assert.Equal(0, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void RemoveFromList_DoesNotRemoveRecords_WithSameKey_ButDifferentValue()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.InsertToList("my-key", "my-value");
          x.RemoveFromList("my-key", "different-value");
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""");

        Assert.Equal(1, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void RemoveFromList_DoesNotRemoveRecords_WithSameValue_ButDifferentKey()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.InsertToList("my-key", "my-value");
          x.RemoveFromList("different-key", "my-value");
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""");

        Assert.Equal(1, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void TrimList_TrimsAList_ToASpecifiedRange()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.InsertToList("my-key", "0");
          x.InsertToList("my-key", "1");
          x.InsertToList("my-key", "2");
          x.InsertToList("my-key", "3");
          x.TrimList("my-key", 1, 2);
        });

        dynamic[] records = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""").ToArray();
        if (DbQueryHelper.IsUpperCase)
        {
          Assert.Equal(2, records.Length);
          Assert.Equal("1", records[0].VALUE);
          Assert.Equal("2", records[1].VALUE);
        }
        else
        {
          Assert.Equal(2, records.Length);
          Assert.Equal("1", records[0].value);
          Assert.Equal("2", records[1].value);
        }
      });
    }

    [Fact]
    [CleanDatabase]
    public void TrimList_RemovesRecordsToEnd_IfKeepAndingAt_GreaterThanMaxElementIndex()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.InsertToList("my-key", "0");
          x.InsertToList("my-key", "1");
          x.InsertToList("my-key", "2");
          x.TrimList("my-key", 1, 100);
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""");

        Assert.Equal(2, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void TrimList_RemovesAllRecords_WhenStartingFromValue_GreaterThanMaxElementIndex()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.InsertToList("my-key", "0");
          x.TrimList("my-key", 1, 100);
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""");

        Assert.Equal(0, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void TrimList_RemovesAllRecords_IfStartFromGreaterThanEndingAt()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.InsertToList("my-key", "0");
          x.TrimList("my-key", 1, 0);
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""");

        Assert.Equal(0, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void TrimList_RemovesRecords_OnlyOfAGivenKey()
    {
      UseConnection(connection => {
        Commit(connection, x => {
          x.InsertToList("my-key", "0");
          x.TrimList("another-key", 1, 0);
        });

        long recordCount = connection.QuerySingle<long>($@"SELECT COUNT(*) FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""");

        Assert.Equal(1, recordCount);
      });
    }

    [Fact]
    [CleanDatabase]
    public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
          () => Commit(connection, x => x.SetRangeInHash(null, new Dictionary<string, string>())));

        Assert.Equal("key", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => Commit(connection, x => x.SetRangeInHash("some-hash", null)));

        Assert.Equal("keyValuePairs", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void SetRangeInHash_MergesAllRecords()
    {
      UseConnection(connection => {
        Commit(connection, x => x.SetRangeInHash("some-hash", new Dictionary<string, string> {
          { "Key1", "Value1" },
          { "Key2", "Value2" },
        }));

        Dictionary<string, string> result = null;
        if (DbQueryHelper.IsUpperCase)
        {
          result = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"hash".GetProperDbObjectName()}"" 
                                                                WHERE ""{"key".GetProperDbObjectName()}"" = @Key", new { Key = "some-hash" })
                             .ToDictionary(x => (string)x.FIELD, x => (string)x.VALUE);
        }
        else
        {
          result = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"hash".GetProperDbObjectName()}"" 
                                                                WHERE ""{"key".GetProperDbObjectName()}"" = @Key", new { Key = "some-hash" })
                              .ToDictionary(x => (string)x.field, x => (string)x.value);
        }


        Assert.Equal("Value1", result["Key1"]);
        Assert.Equal("Value2", result["Key2"]);
      });
    }

    [Fact]
    [CleanDatabase]
    public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => Commit(connection, x => x.RemoveHash(null))); });
    }

    [Fact]
    [CleanDatabase]
    public void RemoveHash_RemovesAllHashRecords()
    {
      UseConnection(connection => {
        // Arrange
        Commit(connection, x => x.SetRangeInHash("some-hash", new Dictionary<string, string> {
          { "Key1", "Value1" },
          { "Key2", "Value2" },
        }));

        // Act
        Commit(connection, x => x.RemoveHash("some-hash"));

        // Assert
        long count = connection.QuerySingle<long>($@"SELECT COUNT(*) 
                                                     FROM ""{GetSchemaName()}"".""{"hash".GetProperDbObjectName()}""");
        Assert.Equal(0, count);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AddRangeToSet_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => Commit(connection, x => x.AddRangeToSet(null, new List<string>())));

        Assert.Equal("key", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AddRangeToSet_ThrowsAnException_WhenItemsValueIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => Commit(connection, x => x.AddRangeToSet("my-set", null)));

        Assert.Equal("items", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AddRangeToSet_AddsAllItems_ToAGivenSet()
    {
      UseConnection(connection => {
        List<string> items = new List<string> { "1", "2", "3" };

        Commit(connection, x => x.AddRangeToSet("my-set", items));

        IEnumerable<string> records = connection.Query<string>($@"SELECT ""{"value".GetProperDbObjectName()}"" 
                                                                  FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}"" 
                                                                  WHERE ""{"key".GetProperDbObjectName()}"" = 'my-set'");
        Assert.Equal(items, records);
      });
    }

    [Fact]
    [CleanDatabase]
    public void RemoveSet_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => { Assert.Throws<ArgumentNullException>(() => Commit(connection, x => x.RemoveSet(null))); });
    }

    [Fact]
    [CleanDatabase]
    public void RemoveSet_RemovesASet_WithAGivenKey()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}"" 
                             (""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"", ""{"score".GetProperDbObjectName()}"") VALUES (@Key, @Value, 0.0)";

      UseConnection(connection => {
        connection.Execute(arrangeSql, new[] {
          new { Key = "set-1", Value = "1" },
          new { Key = "set-2", Value = "1" },
        });

        Commit(connection, x => x.RemoveSet("set-1"));

        dynamic record = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""").Single();
        Assert.Equal("set-2", DbQueryHelper.IsUpperCase ? record.KEY : record.key);
      });
    }

    [Fact]
    [CleanDatabase]
    public void ExpireHash_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
          Commit(connection, x => x.ExpireHash(null, TimeSpan.FromMinutes(5)))
            );
        Assert.Equal("key", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void ExpireHash_SetsExpirationTimeOnAHash_WithGivenKey()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".""{"hash".GetProperDbObjectName()}"" 
                          (""{"key".GetProperDbObjectName()}"", ""{"field".GetProperDbObjectName()}"") VALUES (@Key, @Field)";

      UseConnection(connection => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "hash-1", Field = "field" },
          new { Key = "hash-2", Field = "field" },
        });

        // Act
        Commit(connection, x => x.ExpireHash("hash-1", TimeSpan.FromMinutes(60)));

        // Assert
        Dictionary<string, DateTime?> records = null;
        if (!DbQueryHelper.IsUpperCase)
        {
          records = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"hash".GetProperDbObjectName()}""")
                              .ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
        }
        else
        {
          records = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"hash".GetProperDbObjectName()}""")
                              .ToDictionary(x => (string)x.KEY, x => (DateTime?)x.EXPIREAT);
        }
        Assert.True(DateTime.UtcNow.AddMinutes(59) < records["hash-1"]);
        Assert.True(records["hash-1"] < DateTime.UtcNow.AddMinutes(61));
        Assert.Null(records["hash-2"]);
      });
    }

    [Fact]
    [CleanDatabase]
    public void ExpireSet_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
          Commit(connection, x => x.ExpireSet(null, TimeSpan.FromSeconds(45)))
            );

        Assert.Equal("key", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void ExpireSet_SetsExpirationTime_OnASet_WithGivenKey()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}"" 
                          (""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"", ""{"score".GetProperDbObjectName()}"") VALUES (@Key, @Value, 0.0)";

      UseConnection(connection => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "set-1", Value = "1" },
          new { Key = "set-2", Value = "1" },
        });

        // Act
        Commit(connection, x => x.ExpireSet("set-1", TimeSpan.FromMinutes(60)));

        // Assert      
        Dictionary<string, DateTime?> records = null;
        if (!DbQueryHelper.IsUpperCase)
        {
          records = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""")
                              .ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
        }
        else
        {
          records = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""")
                              .ToDictionary(x => (string)x.KEY, x => (DateTime?)x.EXPIREAT);
        }

        Assert.True(DateTime.UtcNow.AddMinutes(59) < records["set-1"]);
        Assert.True(records["set-1"] < DateTime.UtcNow.AddMinutes(61));
        Assert.Null(records["set-2"]);
      });
    }

    [Fact]
    [CleanDatabase]
    public void ExpireList_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
          Commit(connection, x => x.ExpireList(null, TimeSpan.FromSeconds(45)))
            );

        Assert.Equal("key", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void ExpireList_SetsExpirationTime_OnAList_WithGivenKey()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}"" (""{"key".GetProperDbObjectName()}"") VALUES (@Key)";

      UseConnection(connection => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "list-1" },
          new { Key = "list-2" },
        });

        // Act
        Commit(connection, x => x.ExpireList("list-1", TimeSpan.FromMinutes(60)));

        // Assert        
        Dictionary<string, DateTime?> records = null;
        if (!DbQueryHelper.IsUpperCase)
        {
          records = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""")
                              .ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
        }
        else
        {
          records = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""")
                              .ToDictionary(x => (string)x.KEY, x => (DateTime?)x.EXPIREAT);
        }

        Assert.True(DateTime.UtcNow.AddMinutes(59) < records["list-1"]);
        Assert.True(records["list-1"] < DateTime.UtcNow.AddMinutes(61));
        Assert.Null(records["list-2"]);
      });
    }

    [Fact]
    [CleanDatabase]
    public void PersistHash_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => Commit(connection, x => x.PersistHash(null)));

        Assert.Equal("key", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void PersistHash_ClearsExpirationTime_OnAGivenHash()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".""{"hash".GetProperDbObjectName()}""
                          (""{"key".GetProperDbObjectName()}"", ""{"field".GetProperDbObjectName()}"", ""{"expireat".GetProperDbObjectName()}"") VALUES (@Key, @Field, @ExpireAt)";

      UseConnection(connection => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "hash-1", Field = "field", ExpireAt = DateTime.UtcNow.AddDays(1) },
          new { Key = "hash-2", Field = "field", ExpireAt = DateTime.UtcNow.AddDays(1) },
        });

        // Act
        Commit(connection, x => x.PersistHash("hash-1"));

        // Assert
        Dictionary<string, DateTime?> records = null;

        if (DbQueryHelper.IsUpperCase)
        {
          records = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"hash".GetProperDbObjectName()}""")
                              .ToDictionary(x => (string)x.KEY, x => (DateTime?)x.EXPIREAT);
        }
        else
        {
          records = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"hash".GetProperDbObjectName()}""")
                              .ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
        }

        Assert.Null(records["hash-1"]);
        Assert.NotNull(records["hash-2"]);
      });
    }

    [Fact]
    [CleanDatabase]
    public void PersistSet_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => Commit(connection, x => x.PersistSet(null)));

        Assert.Equal("key", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void PersistSet_ClearsExpirationTime_OnAGivenHash()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}"" 
                          (""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"", ""{"expireat".GetProperDbObjectName()}"", ""{"score".GetProperDbObjectName()}"") 
                            VALUES (@Key, @Value, @ExpireAt, 0.0)";

      UseConnection(connection => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "set-1", Value = "1", ExpireAt = DateTime.UtcNow.AddDays(1) },
          new { Key = "set-2", Value = "1", ExpireAt = DateTime.UtcNow.AddDays(1) },
        });

        // Act
        Commit(connection, x => x.PersistSet("set-1"));

        // Assert
        Dictionary<string, DateTime?> records = null;

        if (DbQueryHelper.IsUpperCase)
        {
          records = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""")
                              .ToDictionary(x => (string)x.KEY, x => (DateTime?)x.EXPIREAT);
        }
        else
        {
          records = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"set".GetProperDbObjectName()}""")
                              .ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
        }


        Assert.Null(records["set-1"]);
        Assert.NotNull(records["set-2"]);
      });
    }

    [Fact]
    [CleanDatabase]
    public void PersistList_ThrowsAnException_WhenKeyIsNull()
    {
      UseConnection(connection => {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => Commit(connection, x => x.PersistList(null)));

        Assert.Equal("key", exception.ParamName);
      });
    }

    [Fact]
    [CleanDatabase]
    public void PersistList_ClearsExpirationTime_OnAGivenHash()
    {
      string arrangeSql = $@"INSERT INTO ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}"" 
                          (""{"key".GetProperDbObjectName()}"", ""{"expireat".GetProperDbObjectName()}"") VALUES (@Key, @ExpireAt)";

      UseConnection(connection => {
        // Arrange
        connection.Execute(arrangeSql, new[] {
          new { Key = "list-1", ExpireAt = DateTime.UtcNow.AddDays(1) },
          new { Key = "list-2", ExpireAt = DateTime.UtcNow.AddDays(1) },
        });

        // Act
        Commit(connection, x => x.PersistList("list-1"));

        // Assert
        Dictionary<string, DateTime?> records = null;

        if (DbQueryHelper.IsUpperCase)
        {
          records = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""")
                              .ToDictionary(x => (string)x.KEY, x => (DateTime?)x.EXPIREAT);
        }
        else
        {
          records = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"list".GetProperDbObjectName()}""")
                              .ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
        }


        Assert.Null(records["list-1"]);
        Assert.NotNull(records["list-2"]);
      });
    }

    [Fact]
    [CleanDatabase]
    public void AddToQueue_AddsAJobToTheQueue_UsingStorageConnection_WithTransactionScopeEnlistment()
    {
      string jobId;
      PostgreSqlStorage storage =
        new PostgreSqlStorage(ConnectionUtils.GetConnectionString(), new PostgreSqlStorageOptions { EnableTransactionScopeEnlistment = true });
      using (IStorageConnection storageConnection = storage.GetConnection())
      {
        using (IWriteOnlyTransaction writeTransaction = storageConnection.CreateWriteTransaction())
        {
          // Explicitly call multiple write commands here, as AddToQueue previously opened an own connection.
          // This triggered a prepared transaction which should be avoided.
          jobId = storageConnection.CreateExpiredJob(Job.FromExpression(() => Console.Write("Hi")), new Dictionary<string, string>(), DateTime.UtcNow,
            TimeSpan.FromMinutes(1));

          writeTransaction.SetJobState(jobId, new ScheduledState(DateTime.UtcNow));
          writeTransaction.AddToQueue("default", jobId);
          writeTransaction.PersistJob(jobId);
          writeTransaction.Commit();
        }
      }

      UseConnection(connection => {
        dynamic record = connection.Query($@"SELECT * FROM ""{GetSchemaName()}"".""{"jobqueue".GetProperDbObjectName()}""").Single();
        Assert.Equal(jobId, DbQueryHelper.IsUpperCase ? record.JOBID?.ToString() : record.jobid.ToString());
        Assert.Equal("default", DbQueryHelper.IsUpperCase ? record.QUEUE : record.queue);
        Assert.Null(DbQueryHelper.IsUpperCase ? record.FETCHEDAT : record.FetchedAt);
      });
    }

    private void UseConnection(Action<NpgsqlConnection> action)
    {
      PostgreSqlStorage storage = _fixture.SafeInit();
      action(storage.CreateAndOpenConnection());
    }

    private static void UseDisposableConnection(Action<NpgsqlConnection> action)
    {
      using (NpgsqlConnection sqlConnection = ConnectionUtils.CreateConnection())
      {
        action(sqlConnection);
      }
    }

    private void Commit(NpgsqlConnection connection, Action<PostgreSqlWriteOnlyTransaction> action)
    {
      PostgreSqlStorage storage = _fixture.ForceInit(connection);
      using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
      {
        action(transaction as PostgreSqlWriteOnlyTransaction);
        transaction.Commit();
      }
    }

    private void CommitDisposable(NpgsqlConnection connection, Action<PostgreSqlWriteOnlyTransaction> action)
    {
      PostgreSqlStorage storage = new PostgreSqlStorage(connection, new PostgreSqlStorageOptions {
        EnableTransactionScopeEnlistment = true,
        SchemaName = GetSchemaName(),
      });
      using (IWriteOnlyTransaction transaction = storage.GetConnection().CreateWriteTransaction())
      {
        action(transaction as PostgreSqlWriteOnlyTransaction);
        transaction.Commit();
      }
    }

    private static string GetSchemaName()
    {
      return ConnectionUtils.GetSchemaName();
    }
  }
}
