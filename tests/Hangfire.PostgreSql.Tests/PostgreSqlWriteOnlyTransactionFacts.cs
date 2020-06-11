using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Transactions;
using Dapper;
using Hangfire.States;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlWriteOnlyTransactionFacts
    {
        private readonly PersistentJobQueueProviderCollection _queueProviders;
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlWriteOnlyTransactionFacts()
        {
            var defaultProvider = new Mock<IPersistentJobQueueProvider>();
            defaultProvider.Setup(x => x.GetJobQueue())
                .Returns(new Mock<IPersistentJobQueue>().Object);

            _queueProviders = new PersistentJobQueueProviderCollection(defaultProvider.Object);
            _options = new PostgreSqlStorageOptions()
            {
                SchemaName = GetSchemaName(),
                EnableTransactionScopeEnlistment = true
            };
        }

        [Fact]
        public void Ctor_ThrowsAnException_IfConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlWriteOnlyTransaction(null, _options, _queueProviders));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_IfOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlWriteOnlyTransaction(ConnectionUtils.CreateConnection(), null, _queueProviders));

            Assert.Equal("options", exception.ParamName);
        }


        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfProvidersCollectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlWriteOnlyTransaction(ConnectionUtils.CreateConnection(), _options, null));

            Assert.Equal("queueProviders", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void ExpireJob_SetsJobExpirationData()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job""(""invocationdata"", ""arguments"", ""createdat"")
values ('', '', now() at time zone 'utc') returning ""id""";

            UseConnection(sql =>
            {
                var jobId = sql.Query(arrangeSql).Single().id.ToString();
                var anotherJobId = sql.Query(arrangeSql).Single().id.ToString();

                Commit(sql, x => x.ExpireJob(jobId, TimeSpan.FromDays(1)));

                var job = GetTestJob(sql, jobId);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < job.expireat && job.expireat <= DateTime.UtcNow.AddDays(1));

                var anotherJob = GetTestJob(sql, anotherJobId);
                Assert.Null(anotherJob.expireat);
            });
        }

        [Fact, CleanDatabase]
        public void PersistJob_ClearsTheJobExpirationData()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"", ""expireat"")
values ('', '', now() at time zone 'utc', now() at time zone 'utc') returning ""id""";


            UseConnection(sql =>
            {
                var jobId = sql.Query(arrangeSql).Single().id.ToString();
                var anotherJobId = sql.Query(arrangeSql).Single().id.ToString();

                Commit(sql, x => x.PersistJob(jobId));

                var job = GetTestJob(sql, jobId);
                Assert.Null(job.expireat);

                var anotherJob = GetTestJob(sql, anotherJobId);
                Assert.NotNull(anotherJob.expireat);
            });
        }

        [Fact, CleanDatabase]
        public void SetJobState_AppendsAStateAndSetItToTheJob()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values ('', '', now() at time zone 'utc') returning ""id""";

            UseConnection(sql =>
            {
                var jobId = sql.Query(arrangeSql).Single().id.ToString();
                var anotherJobId = sql.Query(arrangeSql).Single().id.ToString();

                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData())
                    .Returns(new Dictionary<string, string> {{"Name", "Value"}});

                Commit(sql, x => x.SetJobState(jobId, state.Object));

                var job = GetTestJob(sql, jobId);
                Assert.Equal("State", job.statename);
                Assert.NotNull(job.stateid);

                var anotherJob = GetTestJob(sql, anotherJobId);
                Assert.Null(anotherJob.statename);
                Assert.Null(anotherJob.stateid);

                var jobState = sql.Query(@"select * from """ + GetSchemaName() + @""".""state""").Single();
                Assert.Equal((string)jobId, jobState.jobid.ToString());
                Assert.Equal("State", jobState.name);
                Assert.Equal("Reason", jobState.reason);
                Assert.NotNull(jobState.createdat);
                Assert.Equal("{\"Name\":\"Value\"}", jobState.data);
            });
        }

        [Theory, CleanDatabase]
        [InlineData(false)]
        [InlineData(true)]
        public void SetJobState_EnlistsInAmbientTransaction(bool completeTransactionScope)
        {
            TransactionScope CreateTransactionScope(System.Transactions.IsolationLevel isolationLevel = System.Transactions.IsolationLevel.RepeatableRead)
            {
                var transactionOptions = new TransactionOptions()
                {
                    IsolationLevel = isolationLevel,
                    Timeout = TransactionManager.MaximumTimeout
                };

                return new TransactionScope(TransactionScopeOption.Required, transactionOptions);
            }

            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values ('', '', now() at time zone 'utc') returning ""id""";


            string jobId = null;
            string anotherJobId = null;
            UseConnection(sql =>
            {
                jobId = sql.Query(arrangeSql).Single().id.ToString();
                anotherJobId = sql.Query(arrangeSql).Single().id.ToString();
            });

            using (var scope = CreateTransactionScope())
            {
                UseConnection(sql =>
                {
                    var state = new Mock<IState>();
                    state.Setup(x => x.Name).Returns("State");
                    state.Setup(x => x.Reason).Returns("Reason");
                    state.Setup(x => x.SerializeData())
                        .Returns(new Dictionary<string, string> { { "Name", "Value" } });

                    Commit(sql, x => x.SetJobState(jobId, state.Object));

                });
                if (completeTransactionScope)
                {
                    scope.Complete();
                }
            }

            UseConnection(sql =>
            {
                var job = GetTestJob(sql, jobId);
                if (completeTransactionScope)
                {
                    Assert.Equal("State", job.statename);
                    Assert.NotNull(job.stateid);

                    var jobState = sql.Query(@"select * from """ + GetSchemaName() + @""".""state""").Single();
                    Assert.Equal((string) jobId, jobState.jobid.ToString());
                    Assert.Equal("State", jobState.name);
                    Assert.Equal("Reason", jobState.reason);
                    Assert.NotNull(jobState.createdat);
                    Assert.Equal("{\"Name\":\"Value\"}", jobState.data);
                }
                else
                {
                    Assert.Null(job.statename);
                    Assert.Null(job.stateid);

                    Assert.Null(sql.Query(@"select * from """ + GetSchemaName() + @""".""state""").SingleOrDefault());
                }

                var anotherJob = GetTestJob(sql, anotherJobId);
                Assert.Null(anotherJob.statename);
                Assert.Null(anotherJob.stateid);
            });
        }

        [Fact, CleanDatabase]
        public void AddJobState_JustAddsANewRecordInATable()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values ('', '', now() at time zone 'utc')
returning ""id""";

            UseConnection(sql =>
            {
                var jobId = sql.Query(arrangeSql).Single().id.ToString(CultureInfo.InvariantCulture);

                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData())
                    .Returns(new Dictionary<string, string> { { "Name", "Value" } });

                Commit(sql, x => x.AddJobState(jobId, state.Object));

                var job = GetTestJob(sql, jobId);
                Assert.Null(job.StateName);
                Assert.Null(job.StateId);

                var jobState = sql.Query(@"select * from """ + GetSchemaName() + @""".""state""").Single();
                Assert.Equal((string)jobId, jobState.jobid.ToString(CultureInfo.InvariantCulture));
                Assert.Equal("State", jobState.name);
                Assert.Equal("Reason", jobState.reason);
                Assert.NotNull(jobState.createdat);
                Assert.Equal("{\"Name\":\"Value\"}", jobState.data);
            });
        }

        [Fact, CleanDatabase]
        public void AddToQueue_CallsEnqueue_OnTargetPersistentQueue()
        {
            UseConnection(sql =>
            {
                var correctJobQueue = new Mock<IPersistentJobQueue>();
                var correctProvider = new Mock<IPersistentJobQueueProvider>();
                correctProvider.Setup(x => x.GetJobQueue())
                    .Returns(correctJobQueue.Object);

                _queueProviders.Add(correctProvider.Object, new[] { "default" });

                Commit(sql, x => x.AddToQueue("default", "1"));

                correctJobQueue.Verify(x => x.Enqueue("default", "1"));
            });
        }

        private static dynamic GetTestJob(IDbConnection connection, string jobId)
        {
            return connection
                .Query(@"select * from """ + GetSchemaName() + @""".""job"" where ""id"" = @id",
                    new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) })
                .Single();
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_AddsRecordToCounterTable_WithPositiveValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.IncrementCounter("my-key"));

                var record = sql.Query(@"select * from """ + GetSchemaName() + @""".""counter""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal(1, record.value);
                Assert.Equal((DateTime?)null, record.expireat);
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.IncrementCounter("my-key", TimeSpan.FromDays(1)));

                var record = sql.Query(@"select * from """ + GetSchemaName() + @""".""counter""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal(1, record.value);
                Assert.NotNull(record.expireat);

                var expireAt = (DateTime)record.expireat;

                Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.IncrementCounter("my-key");
                    x.IncrementCounter("my-key");
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""counter""").Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_AddsRecordToCounterTable_WithNegativeValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.DecrementCounter("my-key"));

                var record = sql.Query(@"select * from """ + GetSchemaName() + @""".""counter""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal(-1, record.value);
                Assert.Equal((DateTime?)null, record.expireat);
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.DecrementCounter("my-key", TimeSpan.FromDays(1)));

                var record = sql.Query(@"select * from """ + GetSchemaName() + @""".""counter""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal(-1, record.value);
                Assert.NotNull(record.expireat);

                var expireAt = (DateTime)record.expireat;

                Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.DecrementCounter("my-key");
                    x.DecrementCounter("my-key");
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""counter""").Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsARecord_IfThereIsNo_SuchKeyAndValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.AddToSet("my-key", "my-value"));

                var record = sql.Query(@"select * from """ + GetSchemaName() + @""".""set""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal("my-value", record.value);
                Assert.Equal(0.0, record.score, 2);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsARecord_WhenKeyIsExists_ButValuesAreDifferent()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "another-value");
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""set""").Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_DoesNotAddARecord_WhenBothKeyAndValueAreExist()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value");
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""set""").Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_AddsARecordWithScore_WhenBothKeyAndValueAreNotExist()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.AddToSet("my-key", "my-value", 3.2));

                var record = sql.Query(@"select * from """ + GetSchemaName() + @""".""set""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal("my-value", record.value);
                Assert.Equal(3.2, record.score, 3);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_UpdatesAScore_WhenBothKeyAndValueAreExist()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value", 3.2);
                });

                var record = sql.Query(@"select * from """ + GetSchemaName() + @""".""set""").Single();

                Assert.Equal(3.2, record.score, 3);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_RemovesARecord_WithGivenKeyAndValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "my-value");
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""set""").Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameKey_AndDifferentValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "different-value");
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""set""").Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameValue_AndDifferentKey()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("different-key", "my-value");
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""set""").Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsARecord_WithGivenValues()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.InsertToList("my-key", "my-value"));

                var record = sql.Query(@"select * from """ + GetSchemaName() + @""".""list""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal("my-value", record.value);
            });
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsAnotherRecord_WhenBothKeyAndValueAreExist()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""").Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_RemovesAllRecords_WithGivenKeyAndValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "my-value");
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""").Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameKey_ButDifferentValue()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "different-value");
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""").Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameValue_ButDifferentKey()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("different-key", "my-value");
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""").Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_TrimsAList_ToASpecifiedRange()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.InsertToList("my-key", "3");
                    x.TrimList("my-key", 1, 2);
                });

                var records = sql.Query(@"select * from """ + GetSchemaName() + @""".""list""").ToArray();

                Assert.Equal(2, records.Length);
                Assert.Equal("1", records[0].value);
                Assert.Equal("2", records[1].value);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecordsToEnd_IfKeepAndingAt_GreaterThanMaxElementIndex()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""").Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_WhenStartingFromValue_GreaterThanMaxElementIndex()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""").Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_IfStartFromGreaterThanEndingAt()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 0);
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""").Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecords_OnlyOfAGivenKey()
        {
            UseConnection(sql =>
            {
                Commit(sql, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("another-key", 1, 0);
                });

                var recordCount = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""").Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.SetRangeInHash(null, new Dictionary<string, string>())));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(sql =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.SetRangeInHash("some-hash", null)));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnection(sql =>
            {
                Commit(sql, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    {"Key1", "Value1"},
                    {"Key2", "Value2"}
                }));

                var result = sql.Query(
                    @"select * from """ + GetSchemaName() + @""".""hash"" where ""key"" = @key",
                    new { key = "some-hash" })
                    .ToDictionary(x => (string)x.field, x => (string)x.value);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.RemoveHash(null)));
            });
        }

        [Fact, CleanDatabase]
        public void RemoveHash_RemovesAllHashRecords()
        {
            UseConnection(sql =>
            {
                // Arrange
                Commit(sql, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    {"Key1", "Value1"},
                    {"Key2", "Value2"}
                }));

                // Act
                Commit(sql, x => x.RemoveHash("some-hash"));

                // Assert
                var count = sql.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""hash""").Single();
                Assert.Equal(0, count);
            });
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.AddRangeToSet(null, new List<string>())));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_ThrowsAnException_WhenItemsValueIsNull()
        {
            UseConnection(sql =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.AddRangeToSet("my-set", null)));

                Assert.Equal("items", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_AddsAllItems_ToAGivenSet()
        {
            UseConnection(sql =>
            {
                var items = new List<string> { "1", "2", "3" };

                Commit(sql, x => x.AddRangeToSet("my-set", items));

                var records = sql.Query<string>(@"select ""value"" from """ + GetSchemaName() + @""".""set"" where ""key"" = 'my-set'");
                Assert.Equal(items, records);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.RemoveSet(null)));
            });
        }

        [Fact, CleanDatabase]
        public void RemoveSet_RemovesASet_WithAGivenKey()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() + @""".""set"" (""key"", ""value"", ""score"") values (@key, @value, 0.0)";

            UseConnection(sql =>
            {
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "set-1", value = "1" },
                    new { key = "set-2", value = "1" }
                });

                Commit(sql, x => x.RemoveSet("set-1"));

                var record = sql.Query(@"select * from """ + GetSchemaName() + @""".""set""").Single();
                Assert.Equal("set-2", record.key);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.ExpireHash(null, TimeSpan.FromMinutes(5))));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireHash_SetsExpirationTimeOnAHash_WithGivenKey()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() + @""".hash (""key"", ""field"") values (@key, @field)";

            UseConnection(sql =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "hash-1", field = "field" },
                    new { key = "hash-2", field = "field" }
                });

                // Act
                Commit(sql, x => x.ExpireHash("hash-1", TimeSpan.FromMinutes(60)));

                // Assert
                var records = sql.Query(@"select * from """ + GetSchemaName() + @""".hash").ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
                Assert.True(DateTime.UtcNow.AddMinutes(59) < records["hash-1"]);
                Assert.True(records["hash-1"] < DateTime.UtcNow.AddMinutes(61));
                Assert.Null(records["hash-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.ExpireSet(null, TimeSpan.FromSeconds(45))));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireSet_SetsExpirationTime_OnASet_WithGivenKey()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() + @""".""set"" (""key"", ""value"", ""score"") values (@key, @value, 0.0)";

            UseConnection(sql =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "set-1", value = "1" },
                    new { key = "set-2", value = "1" }
                });

                // Act
                Commit(sql, x => x.ExpireSet("set-1", TimeSpan.FromMinutes(60)));

                // Assert
                var records = sql.Query(@"select * from """ + GetSchemaName() + @""".""set""").ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
                Assert.True(DateTime.UtcNow.AddMinutes(59) < records["set-1"]);
                Assert.True(records["set-1"] < DateTime.UtcNow.AddMinutes(61));
                Assert.Null(records["set-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.ExpireList(null, TimeSpan.FromSeconds(45))));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireList_SetsExpirationTime_OnAList_WithGivenKey()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() + @""".""list"" (""key"") values (@key)";

            UseConnection(sql =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "list-1", value = "1" },
                    new { key = "list-2", value = "1" }
                });

                // Act
                Commit(sql, x => x.ExpireList("list-1", TimeSpan.FromMinutes(60)));

                // Assert
                var records = sql.Query(@"select * from """ + GetSchemaName() + @""".""list""").ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
                Assert.True(DateTime.UtcNow.AddMinutes(59) < records["list-1"]);
                Assert.True(records["list-1"] < DateTime.UtcNow.AddMinutes(61));
                Assert.Null(records["list-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void PersistHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.PersistHash(null)));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void PersistHash_ClearsExpirationTime_OnAGivenHash()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() + @""".hash (""key"", ""field"", ""expireat"") values (@key, @field, @expireAt)";

            UseConnection(sql =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "hash-1", field = "field", expireAt = DateTime.UtcNow.AddDays(1) },
                    new { key = "hash-2", field = "field", expireAt = DateTime.UtcNow.AddDays(1) }
                });

                // Act
                Commit(sql, x => x.PersistHash("hash-1"));

                // Assert
                var records = sql.Query(@"select * from """ + GetSchemaName() + @""".hash").ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
                Assert.Null(records["hash-1"]);
                Assert.NotNull(records["hash-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void PersistSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.PersistSet(null)));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void PersistSet_ClearsExpirationTime_OnAGivenHash()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() + @""".""set"" (""key"", ""value"", ""expireat"", ""score"") values (@key, @value, @expireAt, 0.0)";

            UseConnection(sql =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "set-1", value = "1", expireAt = DateTime.UtcNow.AddDays(1) },
                    new { key = "set-2", value = "1", expireAt = DateTime.UtcNow.AddDays(1) }
                });

                // Act
                Commit(sql, x => x.PersistSet("set-1"));

                // Assert
                var records = sql.Query(@"select * from """ + GetSchemaName() + @""".""set""").ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
                Assert.Null(records["set-1"]);
                Assert.NotNull(records["set-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void PersistList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, x => x.PersistList(null)));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void PersistList_ClearsExpirationTime_OnAGivenHash()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() + @""".""list"" (""key"", ""expireat"") values (@key, @expireAt)";

            UseConnection(sql =>
            {
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "list-1", expireAt = DateTime.UtcNow.AddDays(1) },
                    new { key = "list-2", expireAt = DateTime.UtcNow.AddDays(1) }
                });

                // Act
                Commit(sql, x => x.PersistList("list-1"));

                // Assert
                var records = sql.Query(@"select * from """ + GetSchemaName() + @""".""list""").ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
                Assert.Null(records["list-1"]);
                Assert.NotNull(records["list-2"]);
            });
        }

        private void UseConnection(Action<NpgsqlConnection> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }

        private void Commit(
            NpgsqlConnection connection,
            Action<PostgreSqlWriteOnlyTransaction> action)
        {
            using (var transaction = new PostgreSqlWriteOnlyTransaction(connection, _options, _queueProviders))
            {
                action(transaction);
                transaction.Commit();
            }
        }

        private static string GetSchemaName()
        {
            return ConnectionUtils.GetSchemaName();
        }
    }
}