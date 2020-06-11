// This file is part of Hangfire.PostgreSql.
// Copyright © 2014 Frank Hommers <http://hmm.rs/Hangfire.PostgreSql>.
// 
// Hangfire.PostgreSql is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.PostgreSql  is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.PostgreSql. If not, see <http://www.gnu.org/licenses/>.
//
// This work is based on the work of Sergey Odinokov, author of 
// Hangfire. <http://hangfire.io/>
//   
//    Special thanks goes to him.

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Entities;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Npgsql;

namespace Hangfire.PostgreSql
{
    public class PostgreSqlMonitoringApi : IMonitoringApi
    {
        private readonly PostgreSqlStorage _storage;
        private readonly PostgreSqlStorageOptions _options;
        private readonly PersistentJobQueueProviderCollection _queueProviders;

        public PostgreSqlMonitoringApi(
            PostgreSqlStorage storage,
            PostgreSqlStorageOptions options,
            PersistentJobQueueProviderCollection queueProviders)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _queueProviders = queueProviders ?? throw new ArgumentNullException(nameof(queueProviders));
        }

        public long ScheduledCount()
        {
            return GetNumberOfJobsByStateName(ScheduledState.StateName);
        }

        public long EnqueuedCount(string queue)
        {
	        var queueApi = GetQueueApi(queue);
	        var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

	        return counters.EnqueuedCount ?? 0;
        }

        public long FetchedCount(string queue)
        {
	        var queueApi = GetQueueApi(queue);
	        var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

	        return counters.FetchedCount ?? 0;
        }

        public long FailedCount()
        {
            return GetNumberOfJobsByStateName(FailedState.StateName);
        }

        public long ProcessingCount()
        {
            return GetNumberOfJobsByStateName(ProcessingState.StateName);
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int @from, int count)
        {
            return GetJobs(
                @from, count,
                ProcessingState.StateName,
                (sqlJob, job, stateData) => new ProcessingJobDto
                {
                    Job = job,
                    ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                    StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"]),
                });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int @from, int count)
        {
            return GetJobs(
                @from, count,
                ScheduledState.StateName,
                (sqlJob, job, stateData) => new ScheduledJobDto
                {
                    Job = job,
                    EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                    ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"])
                });
        }

        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return GetTimelineStats("succeeded");
        }

        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return GetTimelineStats("failed");
        }

        public IList<ServerDto> Servers()
        {
            return _storage.UseConnection(connection =>
            {
                var servers = connection.Query<Entities.Server>(
                    @"SELECT * FROM """ + _options.SchemaName + @""".""server""")
                    .ToList();

                var result = new List<ServerDto>();

                foreach (var server in servers)
                {
                    var data = SerializationHelper.Deserialize<ServerData>(server.Data);
                    result.Add(new ServerDto
                    {
                        Name = server.Id,
                        Heartbeat = server.LastHeartbeat,
                        Queues = data.Queues,
                        StartedAt = data.StartedAt ?? DateTime.MinValue,
                        WorkersCount = data.WorkerCount
                    });
                }

                return result;
            });
        }

        public JobList<FailedJobDto> FailedJobs(int @from, int count)
        {
            return GetJobs(
                @from,
                count,
                FailedState.StateName,
                (sqlJob, job, stateData) => new FailedJobDto
                {
                    Job = job,
                    Reason = sqlJob.StateReason,
                    ExceptionDetails = stateData["ExceptionDetails"],
                    ExceptionMessage = stateData["ExceptionMessage"],
                    ExceptionType = stateData["ExceptionType"],
                    FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
                });
        }

        public JobList<SucceededJobDto> SucceededJobs(int @from, int count)
        {
            return GetJobs(
                @from,
                count,
                SucceededState.StateName,
                (sqlJob, job, stateData) => new SucceededJobDto
                {
                    Job = job,
                    Result = stateData.ContainsKey("Result") ? stateData["Result"] : null,
                    TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                        ? (long?)long.Parse(stateData["PerformanceDuration"]) + (long?)long.Parse(stateData["Latency"])
                        : null,
                    SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
                });
        }

        public JobList<DeletedJobDto> DeletedJobs(int @from, int count)
        {
            return GetJobs(
                @from,
                count,
                DeletedState.StateName,
                (sqlJob, job, stateData) => new DeletedJobDto
                {
                    Job = job,
                    DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
                });
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
	        var tuples = _queueProviders
		        .Select(x => x.GetJobQueueMonitoringApi())
		        .SelectMany(x => x.GetQueues(), (monitoring, queue) => new {Monitoring = monitoring, Queue = queue})
		        .OrderBy(x => x.Queue)
		        .ToArray();

	        var result = new List<QueueWithTopEnqueuedJobsDto>(tuples.Length);

	        foreach (var tuple in tuples)
	        {
		        var enqueuedJobIds = tuple.Monitoring.GetEnqueuedJobIds(tuple.Queue, 0, 5);
		        var counters = tuple.Monitoring.GetEnqueuedAndFetchedCount(tuple.Queue);

		        result.Add(new QueueWithTopEnqueuedJobsDto
		        {
			        Name = tuple.Queue,
			        Length = counters.EnqueuedCount ?? 0,
			        Fetched = counters.FetchedCount,
			        FirstJobs = EnqueuedJobs(enqueuedJobIds)
		        });
	        }

	        return result;
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int @from, int perPage)
        {
	        var queueApi = GetQueueApi(queue);
	        var enqueuedJobIds = queueApi.GetEnqueuedJobIds(queue, @from, perPage);

	        return EnqueuedJobs(enqueuedJobIds);
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int @from, int perPage)
        {
	        var queueApi = GetQueueApi(queue);
	        var fetchedJobIds = queueApi.GetFetchedJobIds(queue, @from, perPage);

	        return FetchedJobs(fetchedJobIds);
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return GetHourlyTimelineStats("succeeded");
        }

        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return GetHourlyTimelineStats( "failed");
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            return _storage.UseConnection(connection =>
            {


                string sql = @"
SELECT ""id"" ""Id"", ""invocationdata"" ""InvocationData"", ""arguments"" ""Arguments"", ""createdat"" ""CreatedAt"", ""expireat"" ""ExpireAt"" 
FROM """ + _options.SchemaName + @""".""job"" 
WHERE ""id"" = @id;

SELECT ""jobid"" ""JobId"", ""name"" ""Name"", ""value"" ""Value"" from """ + _options.SchemaName + @""".""jobparameter"" 
WHERE ""jobid"" = @id;

SELECT ""jobid"" ""JobId"", ""name"" ""Name"", ""reason"" ""Reason"", ""createdat"" ""CreatedAt"", ""data"" ""Data"" 
FROM """ + _options.SchemaName + @""".""state"" 
WHERE ""jobid"" = @id 
ORDER BY ""id"" DESC;
";
                using (var multi = connection.QueryMultiple(sql, new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) }))
                {
                    var job = multi.Read<SqlJob>().SingleOrDefault();
                    if (job == null) return null;

                    var parameters = multi.Read<JobParameter>().ToDictionary(x => x.Name, x => x.Value);
                    var history =
                        multi.Read<SqlState>()
                            .ToList()
                            .Select(x => new StateHistoryDto
                            {
                                StateName = x.Name,
                                CreatedAt = x.CreatedAt,
                                Reason = x.Reason,
                                Data = new SafeDictionary<string, string>(
                                    SerializationHelper.Deserialize<Dictionary<string, string>>(x.Data),
                                    StringComparer.OrdinalIgnoreCase)
                            })
                            .ToList();

                    return new JobDetailsDto
                    {
                        CreatedAt = job.CreatedAt,
                        Job = DeserializeJob(job.InvocationData, job.Arguments),
                        History = history,
                        Properties = parameters
                    };
                }
            });
        }

        public long SucceededListCount()
        {
            return GetNumberOfJobsByStateName(SucceededState.StateName);
        }

        public long DeletedListCount()
        {
            return GetNumberOfJobsByStateName(DeletedState.StateName);
        }

        public StatisticsDto GetStatistics()
        {
            return _storage.UseConnection(connection =>
            {
                var sql = $@"
SELECT ""statename"" ""State"", COUNT(""id"") ""Count"" 
FROM ""{_options.SchemaName}"".""job""
WHERE ""statename"" IS NOT NULL
GROUP BY ""statename"";

SELECT COUNT(*) 
FROM ""{_options.SchemaName}"".""server"";

SELECT SUM(""value"") 
FROM ""{_options.SchemaName}"".""counter"" 
WHERE ""key"" = 'stats:succeeded';

SELECT SUM(""value"") 
FROM ""{_options.SchemaName}"".""counter"" 
WHERE ""key"" = 'stats:deleted';

SELECT COUNT(*) 
FROM ""{_options.SchemaName}"".""set"" 
WHERE ""key"" = 'recurring-jobs';
";

                var stats = new StatisticsDto();
                using (var multi = connection.QueryMultiple(sql))
                {
                    var countByStates = multi.Read().ToDictionary(x => x.State, x => x.Count);

                    long GetCountIfExists(string name) => countByStates.ContainsKey(name) ? countByStates[name] : 0;

                    stats.Enqueued = GetCountIfExists(EnqueuedState.StateName);
                    stats.Failed = GetCountIfExists(FailedState.StateName);
                    stats.Processing = GetCountIfExists(ProcessingState.StateName);
                    stats.Scheduled = GetCountIfExists(ScheduledState.StateName);

                    stats.Servers = multi.Read<long>().Single();

                    stats.Succeeded = multi.Read<long?>().SingleOrDefault() ?? 0;
                    stats.Deleted = multi.Read<long?>().SingleOrDefault() ?? 0;

                    stats.Recurring = multi.Read<long>().Single();
                }

                stats.Queues = _queueProviders
                    .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
                    .Count();

                return stats;
            });
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
        {
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keyMaps = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd-HH}", x => x);

            return GetTimelineStats(keyMaps);
        }

        private Dictionary<DateTime, long> GetTimelineStats(string type)
        {
            var endDate = DateTime.UtcNow.Date;
            var dates = new List<DateTime>();

            for (var i = 0; i < 7; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }
            var keyMaps = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd}", x => x);

            return GetTimelineStats(keyMaps);
        }

        private Dictionary<DateTime, long> GetTimelineStats(IDictionary<string, DateTime> keyMaps)
        {
            var query = $@"
SELECT ""key"", COUNT(""value"") AS ""count"" 
FROM ""{_options.SchemaName}"".""counter""
WHERE ""key"" = ANY (@keys)
GROUP BY ""key"";
";

            var valuesMap = _storage.UseConnection(connection => connection.Query(
                query,
                new { keys = keyMaps.Keys.ToList() })
                .ToList()
                .ToDictionary(x => (string)x.key, x => (long)x.count));

            foreach (var key in keyMaps.Keys)
            {
                if (!valuesMap.ContainsKey(key)) valuesMap.Add(key, 0);
            }

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < keyMaps.Count; i++)
            {
                var value = valuesMap[keyMaps.ElementAt(i).Key];
                result.Add(keyMaps.ElementAt(i).Value, value);
            }

            return result;
        }

        private IPersistentJobQueueMonitoringApi GetQueueApi(string queueName)
        {
            var provider = _queueProviders.GetProvider(queueName);
            var monitoringApi = provider.GetJobQueueMonitoringApi();

            return monitoringApi;
        }

        private JobList<EnqueuedJobDto> EnqueuedJobs(IEnumerable<long> jobIds)
        {
            string enqueuedJobsSql = @"
SELECT j.""id"" ""Id"", j.""invocationdata"" ""InvocationData"", j.""arguments"" ""Arguments"", j.""createdat"" ""CreatedAt"", j.""expireat"" ""ExpireAt"", s.""name"" ""StateName"", s.""reason"" ""StateReason"", s.""data"" ""StateData""
FROM """ + _options.SchemaName + @""".""job"" j
LEFT JOIN """ + _options.SchemaName + @""".""state"" s ON s.""id"" = j.""stateid""
LEFT JOIN """ + _options.SchemaName + @""".""jobqueue"" jq ON jq.""jobid"" = j.""id""
WHERE j.""id"" = ANY (@jobIds)
AND jq.""fetchedat"" IS NULL;
";

            var jobs = _storage.UseConnection(connection => connection.Query<SqlJob>(
                enqueuedJobsSql,
                new { jobIds = jobIds.ToList() })
                .ToList());

            return DeserializeJobs(
                jobs,
                (sqlJob, job, stateData) => new EnqueuedJobDto
                {
                    Job = job,
                    State = sqlJob.StateName,
                    EnqueuedAt = sqlJob.StateName == EnqueuedState.StateName
                        ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
                        : null
                });
        }

        private long GetNumberOfJobsByStateName(string stateName)
        {
            string sqlQuery = @"
SELECT COUNT(""id"") 
FROM """ + _options.SchemaName + @""".""job"" 
WHERE ""statename"" = @state;
";

            var count = _storage.UseConnection(connection => connection.Query<long>(
                 sqlQuery,
                 new { state = stateName })
                 .Single());

            return count;
        }

        private static Job DeserializeJob(string invocationData, string arguments)
        {
            var data = SerializationHelper.Deserialize<InvocationData>(invocationData);
            data.Arguments = arguments;

            try
            {
                return data.DeserializeJob();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }

        private JobList<TDto> GetJobs<TDto>(int @from, int count, string stateName, Func<SqlJob, Job, Dictionary<string, string>, TDto> selector)
        {
            string jobsSql = @"
SELECT j.""id"" ""Id"", j.""invocationdata"" ""InvocationData"", j.""arguments"" ""Arguments"", j.""createdat"" ""CreatedAt"", 
    j.""expireat"" ""ExpireAt"", NULL ""FetchedAt"", j.""statename"" ""StateName"", s.""reason"" ""StateReason"", s.""data"" ""StateData""
FROM """ + _options.SchemaName + @""".""job"" j
LEFT JOIN """ + _options.SchemaName + @""".""state"" s ON j.""stateid"" = s.""id""
WHERE j.""statename"" = @stateName 
ORDER BY j.""id"" DESC
LIMIT @count OFFSET @start;
";

            var jobs = _storage.UseConnection(connection => connection.Query<SqlJob>(
                        jobsSql,
                        new { stateName = stateName, start = from, count = count })
                        .ToList());

            return DeserializeJobs(jobs, selector);
        }

        private static JobList<TDto> DeserializeJobs<TDto>(
            ICollection<SqlJob> jobs,
            Func<SqlJob, Job, SafeDictionary<string, string>, TDto> selector)
        {
            var result = new List<KeyValuePair<string, TDto>>(jobs.Count);

            foreach (var job in jobs)
            {
                var dto = default(TDto);

                if (job.InvocationData != null)
                {
                    var deserializedData = SerializationHelper.Deserialize<Dictionary<string, string>>(job.StateData);
                    var stateData = deserializedData != null
                        ? new SafeDictionary<string, string>(deserializedData, StringComparer.OrdinalIgnoreCase)
                        : null;

                    dto = selector(job, DeserializeJob(job.InvocationData, job.Arguments), stateData);
                }

                result.Add(new KeyValuePair<string, TDto>(
                    job.Id.ToString(), dto));
            }

            return new JobList<TDto>(result);
        }

        private JobList<FetchedJobDto> FetchedJobs(
            IEnumerable<long> jobIds)
        {
            string fetchedJobsSql = @"
SELECT j.""id"" ""Id"", j.""invocationdata"" ""InvocationData"", j.""arguments"" ""Arguments"", j.""createdat"" ""CreatedAt"", 
    j.""expireat"" ""ExpireAt"", jq.""fetchedat"" ""FetchedAt"", j.""statename"" ""StateName"", s.""reason"" ""StateReason"", s.""data"" ""StateData""
FROM """ + _options.SchemaName + @""".""job"" j
LEFT JOIN """ + _options.SchemaName + @""".""state"" s ON j.""stateid"" = s.""id""
LEFT JOIN """ + _options.SchemaName + @""".""jobqueue"" jq ON jq.""jobid"" = j.""id""
WHERE j.""id"" = ANY (@jobIds)
AND ""jq"".""fetchedat"" IS NOT NULL;
";

            var jobs = _storage.UseConnection(connection => connection.Query<SqlJob>(
                fetchedJobsSql,
                new { jobIds = jobIds.ToList() })
                .ToList());

            var result = new List<KeyValuePair<string, FetchedJobDto>>(jobs.Count);

            foreach (var job in jobs)
            {
                result.Add(new KeyValuePair<string, FetchedJobDto>(
                    job.Id.ToString(),
                    new FetchedJobDto
                    {
                        Job = DeserializeJob(job.InvocationData, job.Arguments),
                        State = job.StateName,
                        FetchedAt = job.FetchedAt
                    }));
            }

            return new JobList<FetchedJobDto>(result);
        }

        /// <summary>
        /// Overloaded dictionary that doesn't throw if given an invalid key
        /// Fixes issues such as https://github.com/frankhommers/Hangfire.PostgreSql/issues/79
        /// </summary>
        private class SafeDictionary<TKey, TValue> : Dictionary<TKey, TValue>
        {
            public SafeDictionary(IDictionary<TKey, TValue> dictionary, IEqualityComparer<TKey> comparer)
                : base(dictionary, comparer)
            {
            }

            public new TValue this[TKey i]
            {
                get { return ContainsKey(i) ? base[i] : default(TValue); }
                set { base[i] = value; }
            }
        }
    }
}
