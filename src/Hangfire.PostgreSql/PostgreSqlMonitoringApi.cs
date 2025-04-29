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

using System.Data;
using System.Globalization;
using Hangfire.Common;
using Hangfire.PostgreSql.Entities;
using Hangfire.PostgreSql.Utils;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.PostgreSql;

public class PostgreSqlMonitoringApi : IMonitoringApi
{
  private readonly PostgreSqlStorageContext _context;

  internal PostgreSqlMonitoringApi(PostgreSqlStorageContext context)
  {
    _context = context ?? throw new ArgumentNullException(nameof(context));
  }

  public IDictionary<DateTime, long> SucceededByDatesCount()
  {
    return GetTimelineStats("succeeded", TimelineStatsRange.Daily);
  }

  public IDictionary<DateTime, long> FailedByDatesCount()
  {
    return GetTimelineStats("failed", TimelineStatsRange.Daily);
  }

  public IDictionary<DateTime, long> HourlySucceededJobs()
  {
    return GetTimelineStats("succeeded", TimelineStatsRange.Hourly);
  }

  public IDictionary<DateTime, long> HourlyFailedJobs()
  {
    return GetTimelineStats("failed", TimelineStatsRange.Hourly);
  }

  private enum TimelineStatsRange 
  {
    Daily,
    Hourly,
  }

  private Dictionary<DateTime, long> GetTimelineStats(string state, TimelineStatsRange range)
  {
    DateTime now = DateTime.UtcNow;
    now = range switch {
      TimelineStatsRange.Daily => DateTime.UtcNow.Date,
      TimelineStatsRange.Hourly => new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Utc),
      var _ => throw new ArgumentOutOfRangeException(nameof(range), range, null),
    };
    Dictionary<DateTime, long> result = (range switch {
      TimelineStatsRange.Daily => Enumerable.Range(0, 7).Select(x => now.AddDays(-x)),
      TimelineStatsRange.Hourly => Enumerable.Range(0, 24).Select(x => now.AddHours(-x)),
      var _ => throw new ArgumentOutOfRangeException(nameof(range), range, null),
    }).ToDictionary(x => x, _ => 0L);
    string keySuffix = range == TimelineStatsRange.Daily ? "yyyy-MM-dd" : "yyyy-MM-dd-HH";
    List<string> keys = result.Keys.Select(x => $"stats:{state}:{x.ToString(keySuffix)}").ToList();

    string query = _context.QueryProvider.GetQuery(
      // language=sql
      """
      WITH aggregated_counters AS (
        SELECT key, value FROM hangfire.aggregatedcounter WHERE key = ANY($1)
      ), regular_counters AS (
        SELECT key, value FROM hangfire.counter WHERE key = ANY($1)
      ), all_counters AS (
        SELECT * FROM aggregated_counters
        UNION ALL
        SELECT * FROM regular_counters
      )
      SELECT key, COALESCE(SUM(value), 0)
      FROM all_counters
      GROUP BY key
      """);
    UseConnection(connection => connection
      .Process(query)
      .WithParameter(keys)
      .ForEach(reader => {
        string key = reader.GetString(0);
        long count = reader.IsDBNull(1) ? 0 : reader.GetInt64(1);
        DateTime date = DateTime.ParseExact(key.Substring(key.LastIndexOf(':') + 1), keySuffix, CultureInfo.InvariantCulture);
        result[date] = count;
      }));
    return result;
  }

  public long EnqueuedCount(string queue)
  {
    return GetCountFromQueueMonitoring(queue, x => x.EnqueuedCount);
  }

  public long FetchedCount(string queue)
  {
    return GetCountFromQueueMonitoring(queue, x => x.FetchedCount);
  }

  private long GetCountFromQueueMonitoring(string queue, Func<EnqueuedAndFetchedCountDto, long> accessor)
  {
    IPersistentJobQueueMonitoringApi queueApi = GetQueueApi(queue);
    EnqueuedAndFetchedCountDto counters = queueApi.GetEnqueuedAndFetchedCount(queue);
    return accessor(counters);
  }

  public long ScheduledCount()
  {
    return GetNumberOfJobsByStateName(ScheduledState.StateName);
  }

  public long FailedCount()
  {
    return GetNumberOfJobsByStateName(FailedState.StateName);
  }

  public long ProcessingCount()
  {
    return GetNumberOfJobsByStateName(ProcessingState.StateName);
  }

  public long SucceededListCount()
  {
    return GetNumberOfJobsByStateName(SucceededState.StateName);
  }

  public long DeletedListCount()
  {
    return GetNumberOfJobsByStateName(DeletedState.StateName);
  }

  private long GetNumberOfJobsByStateName(string stateName)
  {
    string query = _context.QueryProvider.GetQuery("SELECT COUNT(id) FROM hangfire.job WHERE statename = $1");
    return UseConnection(connection => connection.Process(query).WithParameter(stateName).Select(reader => reader.GetInt64(0)).Single());
  }

  public StatisticsDto GetStatistics()
  {
    return UseConnection(connection => {
      string serverQuery = _context.QueryProvider.GetQuery("SELECT COUNT(*) FROM hangfire.server");
      string setQuery = _context.QueryProvider.GetQuery("SELECT COUNT(*) FROM hangfire.set WHERE key = $1");
      string stateQuery = _context.QueryProvider.GetQuery("SELECT statename, COUNT(id) FROM hangfire.job WHERE statename IS NOT NULL GROUP BY statename");
      string aggregateQuery = _context.QueryProvider.GetQuery(
        """
        SELECT SUM(value) FROM (
          SELECT SUM(value) AS value FROM hangfire.counter WHERE key = $1
          UNION ALL
          SELECT SUM(value) AS value FROM hangfire.aggregatedcounter WHERE key = $1
        ) c
        """);

      Dictionary<string, long> countsByState = connection.Process(stateQuery)
        .Select(reader => (reader.GetString(0), reader.GetInt64(1)))
        .ToDictionary(x => x.Item1, x => x.Item2);

      long succeededCount = GetCountAggregate("stats:succeeded");
      long deletedCount = GetCountAggregate("stats:deleted");
      long recurringCount = GetCountFromSet("recurring-jobs");
      long serverCount = connection.Process(serverQuery).Select(ReadInt64OrDefault).FirstOrDefault();

      return new StatisticsDto {
        Enqueued = GetCountCached(EnqueuedState.StateName),
        Failed = GetCountCached(FailedState.StateName),
        Processing = GetCountCached(ProcessingState.StateName),
        Scheduled = GetCountCached(ScheduledState.StateName),
        Succeeded = succeededCount,
        Deleted = deletedCount,
        Recurring = recurringCount,
        Servers = serverCount,
        Queues = _context.QueueProviders
          .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
          .Count(),
      };

      long GetCountCached(string name)
      {
        return countsByState.TryGetValue(name, out long stateCount) ? stateCount : 0;
      }

      long GetCountAggregate(string name)
      {
        return GetCountFromDatabase(aggregateQuery, name);
      }

      long GetCountFromSet(string name)
      {
        return GetCountFromDatabase(setQuery, name);
      }

      long GetCountFromDatabase(string query, string argument)
      {
        return connection.Process(query)
          .WithParameter(argument)
          .Select(ReadInt64OrDefault)
          .FirstOrDefault();
      }
      
      long ReadInt64OrDefault(IDataRecord reader)
      {
        return reader.IsDBNull(0) ? 0 : reader.GetInt64(0);
      }
    });
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

  private JobList<TDto> GetJobs<TDto>(int from, int count, string stateName, Func<SqlJob, Job?, Dictionary<string, string>, TDto> selector)
  {
    string query = _context.QueryProvider.GetQuery(
      """
      SELECT j.id, j.invocationdata, j.arguments, j.createdat, j.expireat, j.statename, s.reason, s.data
      FROM hangfire.job AS j
      LEFT JOIN hangfire.state AS s ON j.stateid = s.id
      WHERE j.statename = $1
      ORDER BY j.id DESC
      LIMIT $2 OFFSET $3
      """);
    List<SqlJob> jobs = UseConnection(connection => connection
      .Process(query)
      .WithParameters(stateName, count, from)
      .Select(reader => new SqlJob {
        Id = reader.GetInt64(0),
        InvocationData = reader.GetString(1),
        Arguments = reader.GetString(2),
        CreatedAt = reader.GetDateTime(3),
        ExpireAt = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
        StateName = reader.IsDBNull(5) ? null : reader.GetString(5),
        StateReason = reader.IsDBNull(6) ? null : reader.GetString(6),
        StateData = reader.IsDBNull(7) ? null : reader.GetString(7),
      })
      .ToList());
    return DeserializeJobs(jobs, selector);
  }

  public IList<QueueWithTopEnqueuedJobsDto> Queues()
  {
    var tuples = _context.QueueProviders
      .Select(x => x.GetJobQueueMonitoringApi())
      .SelectMany(x => x.GetQueues(), (monitoring, queue) => new { Monitoring = monitoring, Queue = queue })
      .OrderBy(x => x.Queue)
      .ToArray();

    List<QueueWithTopEnqueuedJobsDto> result = new(tuples.Length);

    foreach (var tuple in tuples)
    {
      ICollection<long> enqueuedJobIds = tuple.Monitoring.GetEnqueuedJobIds(tuple.Queue, 0, 5);
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
    ICollection<long> enqueuedJobIds = queueApi.GetEnqueuedJobIds(queue, from, perPage);
    return EnqueuedJobs(enqueuedJobIds);
  }

  public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
  {
    IPersistentJobQueueMonitoringApi queueApi = GetQueueApi(queue);
    ICollection<long> fetchedJobIds = queueApi.GetFetchedJobIds(queue, from, perPage);
    return FetchedJobs(fetchedJobIds);
  }

  private IPersistentJobQueueMonitoringApi GetQueueApi(string queue)
  {
    IPersistentJobQueueProvider provider = _context.QueueProviders.GetProvider(queue);
    return provider.GetJobQueueMonitoringApi();
  }

  private JobList<EnqueuedJobDto> EnqueuedJobs(ICollection<long> jobIds)
  {
    if (jobIds.Count == 0)
    {
      return new JobList<EnqueuedJobDto>([]);
    }

    string query = _context.QueryProvider.GetQuery(
      """
      SELECT j.id, j.invocationdata, j.arguments, j.createdat, j.expireat, s.name, s.reason, s.data
      FROM hangfire.job AS j
      LEFT JOIN hangfire.state AS s ON s.id = j.stateid
      LEFT JOIN hangfire.jobqueue AS jq ON jq.jobid = j.id
      WHERE j.id = ANY ($1) AND jq.fetchedat IS NULL
      """);

    List<SqlJob> jobs = UseConnection(connection => connection
      .Process(query)
      .WithParameter(jobIds.ToList())
      .Select(reader => new SqlJob {
        Id = reader.GetInt64(0),
        InvocationData = reader.GetString(1),
        Arguments = reader.GetString(2),
        CreatedAt = reader.GetDateTime(3),
        ExpireAt = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
        StateName = reader.IsDBNull(5) ? null : reader.GetString(5),
        StateReason = reader.IsDBNull(6) ? null : reader.GetString(6),
        StateData = reader.IsDBNull(7) ? null : reader.GetString(7),
      })
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

  private JobList<FetchedJobDto> FetchedJobs(ICollection<long> jobIds)
  {
    if (jobIds.Count == 0)
    {
      return new JobList<FetchedJobDto>([]);
    }

    string query = _context.QueryProvider.GetQuery(
      """
      SELECT j.id, j.invocationdata, j.arguments, j.createdat, j.expireat, jq.fetchedat, j.statename, s.reason, s.data
      FROM hangfire.job AS j
      LEFT JOIN hangfire.state AS s ON j.stateid = s.id
      LEFT JOIN hangfire.jobqueue AS jq ON jq.jobid = j.id
      WHERE j.id = ANY ($1) AND jq.fetchedat IS NOT NULL
      """);
    Dictionary<string, FetchedJobDto> result = UseConnection(connection => connection
      .Process(query)
      .WithParameter(jobIds.ToList())
      .Select(reader => new SqlJob {
        Id = reader.GetInt64(0),
        InvocationData = reader.GetString(1),
        Arguments = reader.GetString(2),
        CreatedAt = reader.GetDateTime(3),
        ExpireAt = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
        FetchedAt = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
        StateName = reader.IsDBNull(6) ? null : reader.GetString(6),
        StateReason = reader.IsDBNull(7) ? null : reader.GetString(7),
        StateData = reader.IsDBNull(8) ? null : reader.GetString(8),
      })
      .ToDictionary(job => job.Id.ToString(), job => new FetchedJobDto {
        Job = DeserializeJob(job.InvocationData, job.Arguments),
        State = job.StateName,
        FetchedAt = job.FetchedAt,
      }));
    return new JobList<FetchedJobDto>(result);
  }

  public JobDetailsDto? JobDetails(string jobIdStr)
  {
    long jobId = jobIdStr.ParseJobId();
    return UseConnection(connection => {
      string jobQuery = _context.QueryProvider.GetQuery("SELECT id, invocationdata, arguments, createdat, expireat FROM hangfire.job WHERE id = $1");
      SqlJob? job = connection.Process(jobQuery)
        .WithParameter(jobId)
        .Select(reader => new SqlJob {
          Id = reader.GetInt64(0),
          InvocationData = reader.GetString(1),
          Arguments = reader.GetString(2),
          CreatedAt = reader.GetDateTime(3),
          ExpireAt = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
        })
        .FirstOrDefault();
      if (job == null)
      {
        return null;
      }
      
      string parametersQuery = _context.QueryProvider.GetQuery("SELECT name, value FROM hangfire.jobparameter WHERE jobid = $1");
      Dictionary<string, string> parameters = connection.Process(parametersQuery)
        .WithParameter(jobId)
        .Select(reader => (reader.GetString(0), reader.GetString(1)))
        .ToDictionary(x => x.Item1, x => x.Item2);
      
      string stateHistoryQuery = _context.QueryProvider.GetQuery("SELECT name, reason, createdat, data FROM hangfire.state WHERE jobid = $1 ORDER BY id DESC");
      List<StateHistoryDto> history = connection.Process(stateHistoryQuery)
        .WithParameter(jobId)
        .Select(reader => new StateHistoryDto {
          StateName = reader.GetString(0),
          Reason = reader.IsDBNull(1) ? null : reader.GetString(1),
          CreatedAt = reader.GetDateTime(2),
          Data = new SafeDictionary<string>(SerializationHelper.Deserialize<Dictionary<string, string>>(reader.GetString(3))),
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

  public IList<ServerDto> Servers()
  {
    return UseConnection(connection => {
      string query = _context.QueryProvider.GetQuery("SELECT id, data, lastheartbeat FROM hangfire.server");
      return connection.Process(query)
        .Select(reader => {
          string id = reader.GetString(0);
          string data = reader.GetString(1);
          DateTime lastHeartbeat = reader.GetDateTime(2);
          ServerData serverData = ServerData.Deserialize(data);
          return new ServerDto {
            Name = id,
            Heartbeat = lastHeartbeat,
            Queues = serverData.Queues,
            StartedAt = serverData.StartedAt ?? DateTime.MinValue,
            WorkersCount = serverData.WorkerCount,
          };
        })
        .ToList();
    });
  }

  private static JobList<TDto> DeserializeJobs<TDto>(
    ICollection<SqlJob> jobs,
    Func<SqlJob, Job?, SafeDictionary<string>, TDto> selector)
  {
    List<KeyValuePair<string, TDto>> result = new(jobs.Count);

    foreach (SqlJob job in jobs)
    {
      Dictionary<string, string> deserializedData = SerializationHelper.Deserialize<Dictionary<string, string>>(job.StateData) ?? new Dictionary<string, string>();
      SafeDictionary<string> stateData = new(deserializedData);

      TDto? dto = selector(job, DeserializeJob(job.InvocationData, job.Arguments), stateData);

      result.Add(new KeyValuePair<string, TDto>(job.Id.ToString(), dto));
    }

    return new JobList<TDto>(result);
  }

  private static Job? DeserializeJob(string invocationData, string arguments)
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

  private T UseConnection<T>(Func<IDbConnection, T> func)
  {
    return _context.ConnectionManager.UseConnection(null, func);
  }

  private void UseConnection(Action<IDbConnection> action)
  {
    _context.ConnectionManager.UseConnection(null, action);
  }

  /// <summary>
  ///   Overloaded dictionary that doesn't throw if given an invalid key
  ///   Fixes issues such as https://github.com/frankhommers/Hangfire.PostgreSql/issues/79
  /// </summary>
  private class SafeDictionary<TValue>(IDictionary<string, TValue> dictionary) : Dictionary<string, TValue>(dictionary, StringComparer.OrdinalIgnoreCase)
  {
    public new TValue? this[string i]
    {
      // ReSharper disable once ArrangeDefaultValueWhenTypeNotEvident
      get => ContainsKey(i) ? base[i] : default;
      // ReSharper disable once UnusedMember.Local
      set => base[i] = value!;
    }
  }
}