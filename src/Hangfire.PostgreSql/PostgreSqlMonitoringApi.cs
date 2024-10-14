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

namespace Hangfire.PostgreSql
{
  public class PostgreSqlMonitoringApi : IMonitoringApi
  {
    private readonly PersistentJobQueueProviderCollection _queueProviders;
    private readonly PostgreSqlStorage _storage;

    public PostgreSqlMonitoringApi(
      PostgreSqlStorage storage,
      PersistentJobQueueProviderCollection queueProviders)
    {
      _storage = storage ?? throw new ArgumentNullException(nameof(storage));
      _queueProviders = queueProviders ?? throw new ArgumentNullException(nameof(queueProviders));
    }

    public long ScheduledCount()
    {
      return GetNumberOfJobsByStateName(ScheduledState.StateName);
    }

    public long EnqueuedCount(string queue)
    {
      IPersistentJobQueueMonitoringApi queueApi = GetQueueApi(queue);
      EnqueuedAndFetchedCountDto counters = queueApi.GetEnqueuedAndFetchedCount(queue);

      return counters.EnqueuedCount;
    }

    public long FetchedCount(string queue)
    {
      IPersistentJobQueueMonitoringApi queueApi = GetQueueApi(queue);
      EnqueuedAndFetchedCountDto counters = queueApi.GetEnqueuedAndFetchedCount(queue);

      return counters.FetchedCount;
    }

    public long FailedCount()
    {
      return GetNumberOfJobsByStateName(FailedState.StateName);
    }

    public long ProcessingCount()
    {
      return GetNumberOfJobsByStateName(ProcessingState.StateName);
    }

    public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
    {
      return GetJobs(from, count,
        ProcessingState.StateName,
        (_, job, stateData) => new ProcessingJobDto {
          Job = job,
          ServerId = stateData.TryGetValue("ServerId", out string serverId) ? serverId : stateData["ServerName"],
          StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"]),
        });
    }

    public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
    {
      return GetJobs(from, count,
        ScheduledState.StateName,
        (_, job, stateData) => new ScheduledJobDto {
          Job = job,
          EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
          ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"]),
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
      return UseConnection(connection => {
        List<(Entities.Server Server, ServerData Data)> servers = connection.Query<Entities.Server>($@"SELECT * FROM ""{_storage.Options.SchemaName}"".""server""")
          .AsEnumerable()
          .Select(server => (server, SerializationHelper.Deserialize<ServerData>(server.Data)))
          .ToList();

        List<ServerDto> result = servers.Select(item => new ServerDto {
          Name = item.Server.Id,
          Heartbeat = item.Server.LastHeartbeat,
          Queues = item.Data.Queues,
          StartedAt = item.Data.StartedAt ?? DateTime.MinValue,
          WorkersCount = item.Data.WorkerCount,
        }).ToList();

        return result;
      });
    }

    public JobList<FailedJobDto> FailedJobs(int from, int count)
    {
      return GetJobs(from,
        count,
        FailedState.StateName,
        (sqlJob, job, stateData) => new FailedJobDto {
          Job = job,
          Reason = sqlJob.StateReason,
          ExceptionDetails = stateData["ExceptionDetails"],
          ExceptionMessage = stateData["ExceptionMessage"],
          ExceptionType = stateData["ExceptionType"],
          FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"]),
        });
    }

    public JobList<SucceededJobDto> SucceededJobs(int from, int count)
    {
      return GetJobs(from,
        count,
        SucceededState.StateName,
        (_, job, stateData) => new SucceededJobDto {
          Job = job,
          Result = stateData.TryGetValue("Result", out string result) ? result : null,
          TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.TryGetValue("Latency", out string latency)
            ? long.Parse(stateData["PerformanceDuration"]) + (long?)long.Parse(latency)
            : null,
          SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"]),
        });
    }

    public JobList<DeletedJobDto> DeletedJobs(int from, int count)
    {
      return GetJobs(from,
        count,
        DeletedState.StateName,
        (_, job, stateData) => new DeletedJobDto {
          Job = job,
          DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"]),
        });
    }

    public IList<QueueWithTopEnqueuedJobsDto> Queues()
    {
      var tuples = _queueProviders
        .Select(x => x.GetJobQueueMonitoringApi())
        .SelectMany(x => x.GetQueues(), (monitoring, queue) => new { Monitoring = monitoring, Queue = queue })
        .OrderBy(x => x.Queue)
        .ToArray();

      List<QueueWithTopEnqueuedJobsDto> result = new(tuples.Length);

      foreach (var tuple in tuples)
      {
        IEnumerable<long> enqueuedJobIds = tuple.Monitoring.GetEnqueuedJobIds(tuple.Queue, 0, 5);
        EnqueuedAndFetchedCountDto counters = tuple.Monitoring.GetEnqueuedAndFetchedCount(tuple.Queue);

        result.Add(new QueueWithTopEnqueuedJobsDto {
          Name = tuple.Queue,
          Length = counters.EnqueuedCount,
          Fetched = counters.FetchedCount,
          FirstJobs = EnqueuedJobs(enqueuedJobIds),
        });
      }

      return result;
    }

    public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
    {
      IPersistentJobQueueMonitoringApi queueApi = GetQueueApi(queue);
      IEnumerable<long> enqueuedJobIds = queueApi.GetEnqueuedJobIds(queue, from, perPage);

      return EnqueuedJobs(enqueuedJobIds);
    }

    public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
    {
      IPersistentJobQueueMonitoringApi queueApi = GetQueueApi(queue);
      IEnumerable<long> fetchedJobIds = queueApi.GetFetchedJobIds(queue, from, perPage);

      return FetchedJobs(fetchedJobIds);
    }

    public IDictionary<DateTime, long> HourlySucceededJobs()
    {
      return GetHourlyTimelineStats("succeeded");
    }

    public IDictionary<DateTime, long> HourlyFailedJobs()
    {
      return GetHourlyTimelineStats("failed");
    }

    public JobDetailsDto JobDetails(string jobId)
    {
      return UseConnection(connection => {
        string sql = $@"
          SELECT ""id"" ""Id"", ""invocationdata"" ""InvocationData"", ""arguments"" ""Arguments"", ""createdat"" ""CreatedAt"", ""expireat"" ""ExpireAt"" 
          FROM ""{_storage.Options.SchemaName}"".""job"" 
          WHERE ""id"" = @Id;

          SELECT ""jobid"" ""JobId"", ""name"" ""Name"", ""value"" ""Value"" 
          FROM ""{_storage.Options.SchemaName}"".""jobparameter"" 
          WHERE ""jobid"" = @Id;

          SELECT ""jobid"" ""JobId"", ""name"" ""Name"", ""reason"" ""Reason"", ""createdat"" ""CreatedAt"", ""data"" ""Data"" 
          FROM ""{_storage.Options.SchemaName}"".""state"" 
          WHERE ""jobid"" = @Id 
          ORDER BY ""id"" DESC;
        ";
        using SqlMapper.GridReader multi = connection.QueryMultiple(sql, new { Id = Convert.ToInt64(jobId, CultureInfo.InvariantCulture) });
        SqlJob job = multi.Read<SqlJob>().SingleOrDefault();
        if (job == null)
        {
          return null;
        }

        Dictionary<string, string> parameters = multi.Read<JobParameter>().ToDictionary(x => x.Name, x => x.Value);
        List<StateHistoryDto> history =
          multi.Read<SqlState>()
            .ToList()
            .Select(x => new StateHistoryDto {
              StateName = x.Name,
              CreatedAt = x.CreatedAt,
              Reason = x.Reason,
              Data = new SafeDictionary<string, string>(SerializationHelper.Deserialize<Dictionary<string, string>>(x.Data),
                StringComparer.OrdinalIgnoreCase),
            })
            .ToList();

        return new JobDetailsDto {
          CreatedAt = job.CreatedAt,
          Job = DeserializeJob(job.InvocationData, job.Arguments),
          History = history,
          Properties = parameters,
          ExpireAt = job.ExpireAt,
        };
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
      return UseConnection(connection => {
        string sql = $@"
          SELECT ""statename"" ""State"", COUNT(""id"") ""Count"" 
          FROM ""{_storage.Options.SchemaName}"".""job""
          WHERE ""statename"" IS NOT NULL
          GROUP BY ""statename"";

          SELECT COUNT(*) 
          FROM ""{_storage.Options.SchemaName}"".""server"";

          SELECT SUM(""value"") FROM
            (SELECT SUM(""value"") AS value
            FROM ""{_storage.Options.SchemaName}"".""counter"" 
            WHERE ""key"" = 'stats:succeeded'
            UNION ALL
            SELECT SUM(""value"") AS value
            FROM ""{_storage.Options.SchemaName}"".""aggregatedcounter"" 
            WHERE ""key"" = 'stats:succeeded') c;

          SELECT SUM(""value"") FROM
            (SELECT SUM(""value"") AS value
            FROM ""{_storage.Options.SchemaName}"".""counter"" 
            WHERE ""key"" = 'stats:deleted'
            UNION ALL
            SELECT SUM(""value"") AS value
            FROM ""{_storage.Options.SchemaName}"".""aggregatedcounter"" 
            WHERE ""key"" = 'stats:deleted') c;

          SELECT COUNT(*) 
          FROM ""{_storage.Options.SchemaName}"".""set"" 
          WHERE ""key"" = 'recurring-jobs';
        ";

        StatisticsDto stats = new();
        using (SqlMapper.GridReader multi = connection.QueryMultiple(sql))
        {
          Dictionary<string, long> countByStates = multi.Read<(string StateName, long Count)>()
            .ToDictionary(x => x.StateName, x => x.Count);

          long GetCountIfExists(string name)
          {
            return countByStates.TryGetValue(name, out long stateCount) ? stateCount : 0;
          }

          stats.Enqueued = GetCountIfExists(EnqueuedState.StateName);
          stats.Failed = GetCountIfExists(FailedState.StateName);
          stats.Processing = GetCountIfExists(ProcessingState.StateName);
          stats.Scheduled = GetCountIfExists(ScheduledState.StateName);

          stats.Servers = multi.ReadSingle<long>();

          stats.Succeeded = multi.ReadSingleOrDefault<long?>() ?? 0;
          stats.Deleted = multi.ReadSingleOrDefault<long?>() ?? 0;

          stats.Recurring = multi.ReadSingle<long>();
        }

        stats.Queues = _queueProviders
          .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
          .Count();

        return stats;
      });
    }

    private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
    {
      DateTime endDate = DateTime.UtcNow;
      List<DateTime> dates = new();
      for (int i = 0; i < 24; i++)
      {
        dates.Add(endDate);
        endDate = endDate.AddHours(-1);
      }

      Dictionary<string, DateTime> keyMaps = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd-HH}", x => x);

      return GetTimelineStats(keyMaps);
    }

    private Dictionary<DateTime, long> GetTimelineStats(string type)
    {
      DateTime endDate = DateTime.UtcNow.Date;
      List<DateTime> dates = new();

      for (int i = 0; i < 7; i++)
      {
        dates.Add(endDate);
        endDate = endDate.AddDays(-1);
      }

      Dictionary<string, DateTime> keyMaps = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd}", x => x);

      return GetTimelineStats(keyMaps);
    }

    private Dictionary<DateTime, long> GetTimelineStats(IDictionary<string, DateTime> keyMaps)
    {
      string query =
        $"""
        WITH "aggregated_counters" AS (
          SELECT "key", "value"
          FROM "{_storage.Options.SchemaName}"."aggregatedcounter"
          WHERE "key" = ANY(@Keys)
        ), "regular_counters" AS (
          SELECT "key", "value"
          FROM "{_storage.Options.SchemaName}"."counter"
          WHERE "key" = ANY(@Keys)
        ), "all_counters" AS (
          SELECT * FROM "aggregated_counters"
          UNION ALL
          SELECT * FROM "regular_counters"
        )
        SELECT "key", COALESCE(SUM("value"), 0) AS "count"
        FROM "all_counters"
        GROUP BY "key"
        """;

      Dictionary<string, long> valuesMap = UseConnection(connection => connection.Query<(string Key, long Count)>(query,
          new { Keys = keyMaps.Keys.ToList() })
        .ToList()
        .ToDictionary(x => x.Key, x => x.Count));

      foreach (string key in keyMaps.Keys)
      {
        if (!valuesMap.ContainsKey(key))
        {
          valuesMap.Add(key, 0);
        }
      }

      Dictionary<DateTime, long> result = new();
      foreach (KeyValuePair<string, DateTime> keyMap in keyMaps)
      {
        long value = valuesMap[keyMap.Key];
        result.Add(keyMap.Value, value);
      }

      return result;
    }

    private IPersistentJobQueueMonitoringApi GetQueueApi(string queueName)
    {
      IPersistentJobQueueProvider provider = _queueProviders.GetProvider(queueName);
      IPersistentJobQueueMonitoringApi monitoringApi = provider.GetJobQueueMonitoringApi();

      return monitoringApi;
    }

    private JobList<EnqueuedJobDto> EnqueuedJobs(IEnumerable<long> jobIds)
    {
      string enqueuedJobsSql = $@"
        SELECT ""j"".""id"" ""Id"", ""j"".""invocationdata"" ""InvocationData"", ""j"".""arguments"" ""Arguments"", ""j"".""createdat"" ""CreatedAt"", 
          ""j"".""expireat"" ""ExpireAt"", ""s"".""name"" ""StateName"", ""s"".""reason"" ""StateReason"", ""s"".""data"" ""StateData""
        FROM ""{_storage.Options.SchemaName}"".""job"" ""j""
        LEFT JOIN ""{_storage.Options.SchemaName}"".""state"" ""s"" ON ""s"".""id"" = ""j"".""stateid""
        LEFT JOIN ""{_storage.Options.SchemaName}"".""jobqueue"" ""jq"" ON ""jq"".""jobid"" = ""j"".""id""
        WHERE ""j"".""id"" = ANY (@JobIds)
        AND ""jq"".""fetchedat"" IS NULL;
      ";

      List<SqlJob> jobs = UseConnection(connection => connection.Query<SqlJob>(enqueuedJobsSql,
          new { JobIds = jobIds.ToList() })
        .ToList());

      return DeserializeJobs(jobs,
        (sqlJob, job, stateData) => new EnqueuedJobDto {
          Job = job,
          State = sqlJob.StateName,
          EnqueuedAt = sqlJob.StateName == EnqueuedState.StateName
            ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
            : null,
        });
    }

    private long GetNumberOfJobsByStateName(string stateName)
    {
      string sqlQuery = $@"SELECT COUNT(""id"") FROM ""{_storage.Options.SchemaName}"".""job"" WHERE ""statename"" = @StateName;";

      return UseConnection(connection => connection.QuerySingle<long>(sqlQuery,
        new { StateName = stateName }));
    }

    private static Job DeserializeJob(string invocationData, string arguments)
    {
      InvocationData data = SerializationHelper.Deserialize<InvocationData>(invocationData);
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

    private JobList<TDto> GetJobs<TDto>(int from, int count, string stateName, Func<SqlJob, Job, Dictionary<string, string>, TDto> selector)
    {
      string jobsSql = $@"
        SELECT ""j"".""id"" ""Id"", ""j"".""invocationdata"" ""InvocationData"", ""j"".""arguments"" ""Arguments"", ""j"".""createdat"" ""CreatedAt"", 
          ""j"".""expireat"" ""ExpireAt"", NULL ""FetchedAt"", ""j"".""statename"" ""StateName"", ""s"".""reason"" ""StateReason"", ""s"".""data"" ""StateData""
        FROM ""{_storage.Options.SchemaName}"".""job"" ""j""
        LEFT JOIN ""{_storage.Options.SchemaName}"".""state"" ""s"" ON ""j"".""stateid"" = ""s"".""id""
        WHERE ""j"".""statename"" = @StateName 
        ORDER BY ""j"".""id"" DESC
        LIMIT @Limit OFFSET @Offset;
      ";

      List<SqlJob> jobs = UseConnection(connection => connection.Query<SqlJob>(jobsSql,
          new { StateName = stateName, Limit = count, Offset = from })
        .ToList());

      return DeserializeJobs(jobs, selector);
    }

    private static JobList<TDto> DeserializeJobs<TDto>(
      ICollection<SqlJob> jobs,
      Func<SqlJob, Job, SafeDictionary<string, string>, TDto> selector)
    {
      List<KeyValuePair<string, TDto>> result = new(jobs.Count);

      foreach (SqlJob job in jobs)
      {
        TDto dto = default;

        if (job.InvocationData != null)
        {
          Dictionary<string, string> deserializedData = SerializationHelper.Deserialize<Dictionary<string, string>>(job.StateData);
          SafeDictionary<string, string> stateData = deserializedData != null
            ? new SafeDictionary<string, string>(deserializedData, StringComparer.OrdinalIgnoreCase)
            : null;

          dto = selector(job, DeserializeJob(job.InvocationData, job.Arguments), stateData);
        }

        result.Add(new KeyValuePair<string, TDto>(job.Id.ToString(), dto));
      }

      return new JobList<TDto>(result);
    }

    private JobList<FetchedJobDto> FetchedJobs(
      IEnumerable<long> jobIds)
    {
      string fetchedJobsSql = $@"
        SELECT ""j"".""id"" ""Id"", ""j"".""invocationdata"" ""InvocationData"", ""j"".""arguments"" ""Arguments"", 
          ""j"".""createdat"" ""CreatedAt"", ""j"".""expireat"" ""ExpireAt"", ""jq"".""fetchedat"" ""FetchedAt"", 
          ""j"".""statename"" ""StateName"", ""s"".""reason"" ""StateReason"", ""s"".""data"" ""StateData""
        FROM ""{_storage.Options.SchemaName}"".""job"" ""j""
        LEFT JOIN ""{_storage.Options.SchemaName}"".""state"" ""s"" ON ""j"".""stateid"" = ""s"".""id""
        LEFT JOIN ""{_storage.Options.SchemaName}"".""jobqueue"" ""jq"" ON ""jq"".""jobid"" = ""j"".""id""
        WHERE ""j"".""id"" = ANY (@JobIds)
        AND ""jq"".""fetchedat"" IS NOT NULL;
      ";

      List<SqlJob> jobs = UseConnection(connection => connection.Query<SqlJob>(fetchedJobsSql,
          new { JobIds = jobIds.ToList() })
        .ToList());

      Dictionary<string, FetchedJobDto> result = jobs.ToDictionary(job => job.Id.ToString(), job => new FetchedJobDto {
        Job = DeserializeJob(job.InvocationData, job.Arguments),
        State = job.StateName,
        FetchedAt = job.FetchedAt,
      });

      return new JobList<FetchedJobDto>(result);
    }

    private T UseConnection<T>(Func<IDbConnection, T> func)
    {
      return _storage.UseConnection(null, func);
    }

    /// <summary>
    ///   Overloaded dictionary that doesn't throw if given an invalid key
    ///   Fixes issues such as https://github.com/frankhommers/Hangfire.PostgreSql/issues/79
    /// </summary>
    private class SafeDictionary<TKey, TValue> : Dictionary<TKey, TValue>
    {
      public SafeDictionary(IDictionary<TKey, TValue> dictionary, IEqualityComparer<TKey> comparer)
        : base(dictionary, comparer) { }

      public new TValue this[TKey i]
      {
        // ReSharper disable once ArrangeDefaultValueWhenTypeNotEvident
        get => ContainsKey(i) ? base[i] : default;
        // ReSharper disable once UnusedMember.Local
        set => base[i] = value;
      }
    }
  }
}
