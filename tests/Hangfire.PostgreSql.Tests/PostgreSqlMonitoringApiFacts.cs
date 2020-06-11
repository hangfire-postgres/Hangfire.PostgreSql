using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlMonitoringApiFacts
    {
        private readonly PersistentJobQueueProviderCollection _queueProviders;
        private readonly PostgreSqlStorageOptions _options;
        private readonly PostgreSqlStorage _storage;

        public PostgreSqlMonitoringApiFacts()
        {
            var defaultProvider = new Mock<IPersistentJobQueueProvider>();
            defaultProvider.Setup(x => x.GetJobQueue())
                .Returns(new Mock<IPersistentJobQueue>().Object);

            _queueProviders = new PersistentJobQueueProviderCollection(defaultProvider.Object);
            _options = new PostgreSqlStorageOptions()
            {
                PrepareSchemaIfNecessary = false,
                SchemaName = ConnectionUtils.GetSchemaName()
            };

            _storage = new PostgreSqlStorage(ConnectionUtils.GetConnectionString(), _options);
        }

        [Fact, CleanDatabase]
        public void GetJobs_MixedCasing_ReturnsJob()
        {
            string arrangeSql = @"
                insert into """ + ConnectionUtils.GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
                values (@invocationData, @arguments, now() at time zone 'utc') returning ""id""";

            var job = Job.FromExpression(() => SampleMethod("Hello"));
            var invocationData = InvocationData.SerializeJob(job);

            UseConnection(sql =>
            {
                var jobId = sql.Query(arrangeSql, 
                    new 
                    {
                        invocationData = SerializationHelper.Serialize(invocationData),
                        arguments = invocationData.Arguments,
                    }).Single().id.ToString();

                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns(SucceededState.StateName);
                state.Setup(x => x.SerializeData())
                    .Returns(new Dictionary<string, string>
                    {
                        { "SUCCEEDEDAT", "2018-05-03T13:28:18.3939693Z" },
                        { "PerformanceDuration", "53" },
                        { "latency", "6730"}
                    });

                Commit(sql, x => x.SetJobState(jobId, state.Object));

                var monitoringApi = _storage.GetMonitoringApi();
                var jobs = monitoringApi.SucceededJobs(0, 10);

                Assert.NotNull(jobs);
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

#pragma warning disable xUnit1013 // Public method should be marked as test
		public static void SampleMethod(string arg)
#pragma warning restore xUnit1013 // Public method should be marked as test
		{
        }
    }
}
