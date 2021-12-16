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
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Entities;
using Hangfire.PostgreSql.Extensions;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;
using IsolationLevel = System.Transactions.IsolationLevel;

namespace Hangfire.PostgreSql
{
  public class PostgreSqlConnection : JobStorageConnection
  {
    private readonly Dictionary<string, HashSet<Guid>> _lockedResources;
    private readonly PostgreSqlStorageOptions _options;
    private readonly PostgreSqlStorage _storage;

    private DbConnection _dedicatedConnection;

    public PostgreSqlConnection(PostgreSqlStorage storage)
    {
      _storage = storage ?? throw new ArgumentNullException(nameof(storage));
      _options = storage.Options ?? throw new ArgumentNullException(nameof(storage.Options));
      _lockedResources = new Dictionary<string, HashSet<Guid>>();
    }

    public override void Dispose()
    {
      if (_dedicatedConnection != null)
      {
        _dedicatedConnection.Dispose();
        _dedicatedConnection = null;
      }
    }

    public override IWriteOnlyTransaction CreateWriteTransaction()
    {
      return new PostgreSqlWriteOnlyTransaction(_storage, () => _dedicatedConnection);
    }

    public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
    {
      if (string.IsNullOrEmpty(resource))
      {
        throw new ArgumentNullException(nameof(resource));
      }

      return AcquireLock($"{_options.SchemaName}:{resource}", timeout);
    }

    public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
    {
      if (queues == null || queues.Length == 0) throw new ArgumentNullException(nameof(queues));

      IPersistentJobQueueProvider[] providers = queues
        .Select(queue => _storage.QueueProviders.GetProvider(queue))
        .Distinct()
        .ToArray();

      if (providers.Length != 1)
      {
        throw new InvalidOperationException(
          $"Multiple provider instances registered for queues: {string.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");
      }

      IPersistentJobQueue persistentQueue = providers[0].GetJobQueue();
      return persistentQueue.Dequeue(queues, cancellationToken);
    }

    public override string CreateExpiredJob(
      Job job,
      IDictionary<string, string> parameters,
      DateTime createdAt,
      TimeSpan expireIn)
    {
      if (job == null) throw new ArgumentNullException(nameof(job));
      if (parameters == null) throw new ArgumentNullException(nameof(parameters));

      string createJobSql = $@"
        INSERT INTO ""{_options.SchemaName.GetProperDbObjectName()}"".""{"job".GetProperDbObjectName()}"" (""{"invocationdata".GetProperDbObjectName()}"", 
                                                                                                           ""{"arguments".GetProperDbObjectName()}"",
                                                                                                           ""{"createdat".GetProperDbObjectName()}"", 
                                                                                                           ""{"expireat".GetProperDbObjectName()}"")
        VALUES (@InvocationData, @Arguments, @CreatedAt, @ExpireAt) 
        RETURNING ""{"id".GetProperDbObjectName()}"";
      ";

      InvocationData invocationData = InvocationData.SerializeJob(job);

      return _storage.UseTransaction(_dedicatedConnection, (connection, transaction) => {
        string jobId = connection.QuerySingle<long>(createJobSql,
          new {
            InvocationData = SerializationHelper.Serialize(invocationData),
            invocationData.Arguments,
            CreatedAt = createdAt,
            ExpireAt = createdAt.Add(expireIn),
          }).ToString(CultureInfo.InvariantCulture);

        if (parameters.Count > 0)
        {
          object[] parameterArray = new object[parameters.Count];
          int parameterIndex = 0;
          foreach (KeyValuePair<string, string> parameter in parameters)
          {
            parameterArray[parameterIndex++] = new {
              JobId = Convert.ToInt64(jobId, CultureInfo.InvariantCulture),
              Name = parameter.Key,
              parameter.Value,
            };
          }

          string insertParameterSql = $@"
            INSERT INTO ""{_options.SchemaName.GetProperDbObjectName()}"".""{"jobparameter".GetProperDbObjectName()}"" (""{"jobid".GetProperDbObjectName()}"", 
                                                                             ""{"name".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"")
            VALUES (@JobId, @Name, @Value);
          ";

          connection.Execute(insertParameterSql, parameterArray, transaction);
        }

        return jobId;
      });
    }

    public override JobData GetJobData(string id)
    {
      if (id == null) throw new ArgumentNullException(nameof(id));

      string sql = $@"
        SELECT ""{"invocationdata".GetProperDbObjectName()}"" ""InvocationData"", 
               ""{"statename".GetProperDbObjectName()}"" ""StateName"", 
               ""{"arguments".GetProperDbObjectName()}"" ""Arguments"",
               ""{"createdat".GetProperDbObjectName()}"" ""CreatedAt""
        FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"job".GetProperDbObjectName()}"" 
        WHERE ""{"id".GetProperDbObjectName()}"" = @Id;
      ";

      SqlJob jobData = _storage.UseConnection(_dedicatedConnection,
        connection => connection
          .Query<SqlJob>(sql, new { Id = Convert.ToInt64(id, CultureInfo.InvariantCulture) })
          .SingleOrDefault());

      if (jobData == null) return null;

      // TODO: conversion exception could be thrown.
      InvocationData invocationData = SerializationHelper.Deserialize<InvocationData>(jobData.InvocationData);
      invocationData.Arguments = jobData.Arguments;

      Job job = null;
      JobLoadException loadException = null;

      try
      {
        job = invocationData.DeserializeJob();
      }
      catch (JobLoadException ex)
      {
        loadException = ex;
      }

      return new JobData {
        Job = job,
        State = jobData.StateName,
        CreatedAt = jobData.CreatedAt,
        LoadException = loadException,
      };
    }

    public override StateData GetStateData(string jobId)
    {
      if (jobId == null) throw new ArgumentNullException(nameof(jobId));

      string sql = $@"
        SELECT s.""{"name".GetProperDbObjectName()}"" ""Name"", 
               s.""{"reason".GetProperDbObjectName()}"" ""Reason"",
               s.""{"data".GetProperDbObjectName()}"" ""Data""
        FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"state".GetProperDbObjectName()}"" s
        INNER JOIN ""{_options.SchemaName.GetProperDbObjectName()}"".""{"job".GetProperDbObjectName()}"" j on j.""{"stateid".GetProperDbObjectName()}"" = s.""{"id".GetProperDbObjectName()}""
        WHERE j.""{"id".GetProperDbObjectName()}"" = @JobId;
      ";

      SqlState sqlState = _storage.UseConnection(_dedicatedConnection,
        connection => connection
          .Query<SqlState>(sql, new { JobId = Convert.ToInt64(jobId, CultureInfo.InvariantCulture) })
          .SingleOrDefault());
      if (sqlState == null)
      {
        return null;
      }

      return new StateData {
        Name = sqlState.Name,
        Reason = sqlState.Reason,
        Data = SerializationHelper.Deserialize<Dictionary<string, string>>(sqlState.Data),
      };
    }

    public override void SetJobParameter(string id, string name, string value)
    {
      if (id == null) throw new ArgumentNullException(nameof(id));
      if (name == null) throw new ArgumentNullException(nameof(name));

      string sql = $@"
        WITH ""inputvalues"" AS (
	        SELECT @JobId ""{"jobid".GetProperDbObjectName()}"", @Name ""{"name".GetProperDbObjectName()}"", @Value ""{"value".GetProperDbObjectName()}""
        ), ""updatedrows"" AS ( 
	        UPDATE ""{_options.SchemaName.GetProperDbObjectName()}"".""{"jobparameter".GetProperDbObjectName()}"" ""updatetarget""
	        SET ""{"value".GetProperDbObjectName()}"" = ""inputvalues"".""{"value".GetProperDbObjectName()}""
	        FROM ""inputvalues""
	        WHERE ""updatetarget"".""{"jobid".GetProperDbObjectName()}"" = ""inputvalues"".""{"jobid".GetProperDbObjectName()}""
	        AND ""updatetarget"".""{"name".GetProperDbObjectName()}"" = ""inputvalues"".""{"name".GetProperDbObjectName()}""
	        RETURNING ""updatetarget"".""{"jobid".GetProperDbObjectName()}"", ""updatetarget"".""{"name".GetProperDbObjectName()}""
        )
        INSERT INTO ""{_options.SchemaName.GetProperDbObjectName()}"".""{"jobparameter".GetProperDbObjectName()}""(""{"jobid".GetProperDbObjectName()}"", ""{"name".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"")
        SELECT ""{"jobid".GetProperDbObjectName()}"", ""{"name".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"" 
        FROM ""inputvalues"" ""insertvalues""
        WHERE NOT EXISTS (
	        SELECT 1 
	        FROM ""updatedrows"" 
	        WHERE ""updatedrows"".""{"jobid".GetProperDbObjectName()}"" = ""insertvalues"".""{"jobid".GetProperDbObjectName()}"" 
	        AND ""updatedrows"".""{"name".GetProperDbObjectName()}"" = ""insertvalues"".""{"name".GetProperDbObjectName()}""
        );
      ";

      _storage.UseConnection(_dedicatedConnection, connection => connection
        .Execute(sql, new { JobId = Convert.ToInt64(id, CultureInfo.InvariantCulture), Name = name, Value = value }));
    }

    public override string GetJobParameter(string id, string name)
    {
      if (id == null) throw new ArgumentNullException(nameof(id));
      if (name == null) throw new ArgumentNullException(nameof(name));

      string query = $@"SELECT ""{"value".GetProperDbObjectName()}"" 
                        FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"jobparameter".GetProperDbObjectName()}""
                        WHERE ""{"jobid".GetProperDbObjectName()}"" = @Id AND ""{"name".GetProperDbObjectName()}"" = @Name;";

      return _storage.UseConnection(_dedicatedConnection, connection => connection
        .QuerySingleOrDefault<string>(query, new { Id = Convert.ToInt64(id, CultureInfo.InvariantCulture), Name = name }));
    }

    public override HashSet<string> GetAllItemsFromSet(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string query = $@"SELECT ""{"value".GetProperDbObjectName()}""    
                        FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"set".GetProperDbObjectName()}""
                        WHERE ""{"key".GetProperDbObjectName()}"" = @Key;";

      return _storage.UseConnection(_dedicatedConnection, connection => {
        IEnumerable<string> result = connection.Query<string>(query, new { Key = key });

        return new HashSet<string>(result);
      });
    }

    public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));
      if (toScore < fromScore) throw new ArgumentException($"The '{nameof(toScore)}' value must be higher or equal to the '{nameof(fromScore)}' value.");

      return _storage.UseConnection(_dedicatedConnection, connection => connection
        .QuerySingleOrDefault<string>($@"
          SELECT ""{"value".GetProperDbObjectName()}"" 
          FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"set".GetProperDbObjectName()}"" 
          WHERE ""{"key".GetProperDbObjectName()}"" = @Key 
          AND ""{"score".GetProperDbObjectName()}"" BETWEEN @FromScore AND @ToScore 
          ORDER BY ""{"score".GetProperDbObjectName()}"" LIMIT 1;
        ",
          new { Key = key, FromScore = fromScore, ToScore = toScore }));
    }

    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));
      if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

      string sql = $@"
        WITH ""inputvalues"" AS (
	        SELECT @Key ""{"key".GetProperDbObjectName()}"", @Field ""{"field".GetProperDbObjectName()}"", @Value ""{"value".GetProperDbObjectName()}""
        ), ""updatedrows"" AS ( 
	        UPDATE ""{_options.SchemaName.GetProperDbObjectName()}"".""{"hash".GetProperDbObjectName()}"" ""updatetarget""
	        SET ""{"value".GetProperDbObjectName()}"" = ""inputvalues"".""{"value".GetProperDbObjectName()}""
	        FROM ""inputvalues""
	        WHERE ""updatetarget"".""{"key".GetProperDbObjectName()}"" = ""inputvalues"".""{"key".GetProperDbObjectName()}""
	        AND ""updatetarget"".""{"field".GetProperDbObjectName()}"" = ""inputvalues"".""{"field".GetProperDbObjectName()}""
	        RETURNING ""updatetarget"".""{"key".GetProperDbObjectName()}"", ""updatetarget"".""{"field".GetProperDbObjectName()}""
        )
        INSERT INTO ""{_options.SchemaName.GetProperDbObjectName()}"".""{"hash".GetProperDbObjectName()}""(""{"key".GetProperDbObjectName()}"", ""{"field".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"")
        SELECT ""{"key".GetProperDbObjectName()}"", 
               ""{"field".GetProperDbObjectName()}"", 
               ""{"value".GetProperDbObjectName()}""
        FROM ""inputvalues"" ""insertvalues""
        WHERE NOT EXISTS (
	        SELECT 1 
	        FROM ""updatedrows"" 
	        WHERE ""updatedrows"".""{"key".GetProperDbObjectName()}"" = ""insertvalues"".""{"key".GetProperDbObjectName()}"" 
	        AND ""updatedrows"".""{"field".GetProperDbObjectName()}"" = ""insertvalues"".""{"field".GetProperDbObjectName()}""
        );
      ";

      Stopwatch executionTimer = Stopwatch.StartNew();
      while (true)
      {
        try
        {
          _storage.UseTransaction(_dedicatedConnection, (connection, transaction) => {
            foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
            {
              connection.Execute(sql, new { Key = key, Field = keyValuePair.Key, keyValuePair.Value }, transaction);
            }
          }, IsolationLevel.Serializable);

          return;
        }
        catch (PostgresException exception)
        {
          if (!exception.SqlState.Equals(PostgresErrorCodes.SerializationFailure)) // 40001
            throw;
        }

        if (executionTimer.Elapsed > _options.TransactionSynchronisationTimeout)
          throw new TimeoutException("SetRangeInHash experienced timeout while trying to execute transaction");
      }
    }

    public override Dictionary<string, string> GetAllEntriesFromHash(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      Dictionary<string, string> result = _storage.UseConnection(_dedicatedConnection, connection => connection
        .Query<SqlHash>($@"
          SELECT ""{"field".GetProperDbObjectName()}"" ""Field"", ""{"value".GetProperDbObjectName()}"" ""Value"" 
          FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"hash".GetProperDbObjectName()}"" 
          WHERE ""{"key".GetProperDbObjectName()}"" = @Key;",
          new { Key = key })
        .ToDictionary(x => x.Field, x => x.Value));

      return result.Count != 0 ? result : null;
    }

    public override void AnnounceServer(string serverId, ServerContext context)
    {
      if (serverId == null) throw new ArgumentNullException(nameof(serverId));
      if (context == null) throw new ArgumentNullException(nameof(context));

      ServerData data = new ServerData {
        WorkerCount = context.WorkerCount,
        Queues = context.Queues,
        StartedAt = DateTime.UtcNow,
      };

      string sql = $@"
        WITH ""inputvalues"" AS (
	        SELECT @Id ""{"id".GetProperDbObjectName()}"", @Data ""{"data".GetProperDbObjectName()}"", NOW() AT TIME ZONE 'UTC' ""{"lastheartbeat".GetProperDbObjectName()}""
        ), ""updatedrows"" AS ( 
	        UPDATE ""{_options.SchemaName.GetProperDbObjectName()}"".""{"server".GetProperDbObjectName()}"" ""updatetarget""
	        SET ""{"data".GetProperDbObjectName()}"" = ""inputvalues"".""{"data".GetProperDbObjectName()}"", ""{"lastheartbeat".GetProperDbObjectName()}"" = ""inputvalues"".""{"lastheartbeat".GetProperDbObjectName()}""
	        FROM ""inputvalues""
	        WHERE ""updatetarget"".""{"id".GetProperDbObjectName()}"" = ""inputvalues"".""{"id".GetProperDbObjectName()}""
	        RETURNING ""updatetarget"".""{"id".GetProperDbObjectName()}""
        )
        INSERT INTO ""{_options.SchemaName.GetProperDbObjectName()}"".""{"server".GetProperDbObjectName()}""(""{"id".GetProperDbObjectName()}"", ""{"data".GetProperDbObjectName()}"", ""{"lastheartbeat".GetProperDbObjectName()}"")
        SELECT ""{"id".GetProperDbObjectName()}"", ""{"data".GetProperDbObjectName()}"", ""{"lastheartbeat".GetProperDbObjectName()}""
        FROM ""inputvalues"" ""insertvalues""
        WHERE NOT EXISTS (
	        SELECT 1 
	        FROM ""updatedrows"" 
	        WHERE ""updatedrows"".""{"id".GetProperDbObjectName()}"" = ""insertvalues"".""{"id".GetProperDbObjectName()}"" 
        );
      ";

      _storage.UseConnection(_dedicatedConnection, connection => connection
        .Execute(sql, new { Id = serverId, Data = SerializationHelper.Serialize(data) }));
    }

    public override void RemoveServer(string serverId)
    {
      if (serverId == null) throw new ArgumentNullException(nameof(serverId));

      _storage.UseConnection(_dedicatedConnection, connection => connection
        .Execute($@"DELETE FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"server".GetProperDbObjectName()}"" WHERE ""{"id".GetProperDbObjectName()}"" = @Id;", new { Id = serverId }));
    }

    public override void Heartbeat(string serverId)
    {
      if (serverId == null) throw new ArgumentNullException(nameof(serverId));

      string query = $@"
        UPDATE ""{_options.SchemaName.GetProperDbObjectName()}"".""{"server".GetProperDbObjectName()}"" 
				SET ""{"lastheartbeat".GetProperDbObjectName()}"" = NOW() AT TIME ZONE 'UTC' 
				WHERE ""{"id".GetProperDbObjectName()}"" = @Id;
      ";

      int affectedRows = _storage.UseConnection(_dedicatedConnection, connection => connection
        .Execute(query, new { Id = serverId }));

      if (affectedRows == 0)
      {
        throw new BackgroundServerGoneException();
      }
    }

    public override int RemoveTimedOutServers(TimeSpan timeOut)
    {
      if (timeOut.Duration() != timeOut)
      {
        throw new ArgumentException("The 'timeOut' value must be positive.", nameof(timeOut));
      }

      string query = $@"
        DELETE FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"server".GetProperDbObjectName()}"" 
				WHERE ""{"lastheartbeat".GetProperDbObjectName()}"" < (NOW() AT TIME ZONE 'UTC' - INTERVAL '{((long)timeOut.TotalMilliseconds).ToString(CultureInfo.InvariantCulture)} MILLISECONDS');";

      return _storage.UseConnection(_dedicatedConnection, connection => connection.Execute(query));
    }

    public override long GetSetCount(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string query = $@"SELECT COUNT(""{"key".GetProperDbObjectName()}"") 
                        FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"set".GetProperDbObjectName()}"" 
                        WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      return _storage.UseConnection(_dedicatedConnection, connection => connection
        .QuerySingleOrDefault<long>(query, new { Key = key }));
    }

    public override List<string> GetAllItemsFromList(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string query = $@"SELECT ""{"value".GetProperDbObjectName()}"" 
                        FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"list".GetProperDbObjectName()}"" 
                        WHERE ""{"key".GetProperDbObjectName()}"" = @Key ORDER BY ""{"id".GetProperDbObjectName()}"" DESC";

      return _storage.UseConnection(_dedicatedConnection, connection => connection
        .Query<string>(query, new { Key = key })
        .ToList());
    }

    public override long GetCounter(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string query = $@"SELECT SUM(""{"value".GetProperDbObjectName()}"") 
                      FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"counter".GetProperDbObjectName()}"" 
                      WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      return _storage.UseConnection(_dedicatedConnection, connection => connection
          .QuerySingleOrDefault<long?>(query, new { Key = key }) ?? 0);
    }

    public override long GetListCount(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string query = $@"SELECT COUNT(""{"id".GetProperDbObjectName()}"") 
                        FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"list".GetProperDbObjectName()}"" 
                        WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      return _storage.UseConnection(_dedicatedConnection, connection => connection
        .QuerySingleOrDefault<long>(query, new { Key = key }));

    }

    public override TimeSpan GetListTtl(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string query = $@"SELECT min(""{"expireat".GetProperDbObjectName()}"") 
                        FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"list".GetProperDbObjectName()}"" 
                        WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      DateTime? result = _storage.UseConnection(_dedicatedConnection, connection => connection
        .QuerySingleOrDefault<DateTime?>(query, new { Key = key }));

      if (!result.HasValue) return TimeSpan.FromSeconds(-1);

      return result.Value - DateTime.UtcNow;
    }

    public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string query = $@"
					SELECT ""{"value".GetProperDbObjectName()}"" 
					FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"list".GetProperDbObjectName()}""
					WHERE ""{"key".GetProperDbObjectName()}"" = @Key
          ORDER BY ""{"id".GetProperDbObjectName()}"" DESC
          LIMIT @Limit OFFSET @Offset
        ";


      return _storage.UseConnection(_dedicatedConnection, connection =>
        connection
        .Query<string>(query, new { Key = key, Limit = endingAt - startingFrom + 1, Offset = startingFrom })
        .ToList()
          );
    }

    public override long GetHashCount(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string query = $@"SELECT COUNT(""{"id".GetProperDbObjectName()}"") 
                        FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"hash".GetProperDbObjectName()}""
                        WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      return _storage.UseConnection(_dedicatedConnection, connection => connection
        .QuerySingle<long>(query, new { Key = key }));
    }

    public override TimeSpan GetHashTtl(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string query = $@"SELECT MIN(""{"expireat".GetProperDbObjectName()}"") 
                      FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"hash".GetProperDbObjectName()}"" 
                      WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      DateTime? result = _storage.UseConnection(_dedicatedConnection, connection => connection
        .QuerySingleOrDefault<DateTime?>(query, new { Key = key }));

      if (!result.HasValue) return TimeSpan.FromSeconds(-1);

      return result.Value - DateTime.UtcNow;
    }

    public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string query = $@"
					SELECT ""{"value".GetProperDbObjectName()}"" 
					FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"set".GetProperDbObjectName()}""
					WHERE ""{"key".GetProperDbObjectName()}"" = @Key
          ORDER BY ""{"id".GetProperDbObjectName()}"" 
          LIMIT @Limit OFFSET @Offset
        ";

      return _storage.UseConnection(_dedicatedConnection, connection => connection
        .Query<string>(query, new { Key = key, Limit = endingAt - startingFrom + 1, Offset = startingFrom })
        .ToList());
    }

    public override TimeSpan GetSetTtl(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string query = $@"SELECT min(""{"expireat".GetProperDbObjectName()}"") 
                        FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"set".GetProperDbObjectName()}"" 
                        WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      DateTime? result = _storage.UseConnection(_dedicatedConnection, connection => connection
        .QuerySingleOrDefault<DateTime?>(query, new { Key = key }));

      if (!result.HasValue) return TimeSpan.FromSeconds(-1);

      return result.Value - DateTime.UtcNow;
    }

    public override string GetValueFromHash(string key, string name)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));
      if (name == null) throw new ArgumentNullException(nameof(name));

      string query = $@"SELECT ""{"value".GetProperDbObjectName()}"" 
                        FROM ""{_options.SchemaName.GetProperDbObjectName()}"".""{"hash".GetProperDbObjectName()}"" 
                        WHERE ""{"key".GetProperDbObjectName()}"" = @Key AND ""{"field".GetProperDbObjectName()}"" = @Field";

      return _storage.UseConnection(_dedicatedConnection, connection => connection
        .Query<string>(query, new { Key = key, Field = name })
        .SingleOrDefault());
    }

    private IDisposable AcquireLock(string resource, TimeSpan timeout)
    {
      if (_dedicatedConnection == null)
      {
        _dedicatedConnection = _storage.CreateAndOpenConnection();
      }

      Guid lockId = Guid.NewGuid();

      if (!_lockedResources.ContainsKey(resource))
      {
        try
        {
          PostgreSqlDistributedLock.Acquire(_dedicatedConnection, resource, timeout, _options);
        }
        catch (Exception)
        {
          ReleaseLock(resource, lockId, true);
          throw;
        }

        _lockedResources.Add(resource, new HashSet<Guid>());
      }

      _lockedResources[resource].Add(lockId);
      return new DisposableLock(this, resource, lockId);
    }

    private void ReleaseLock(string resource, Guid lockId, bool onDisposing)
    {
      try
      {
        if (!_lockedResources.TryGetValue(resource, out HashSet<Guid> resourceLocks))
        {
          return;
        }

        if (!resourceLocks.Contains(lockId))
        {
          return;
        }

        if (resourceLocks.Remove(lockId)
          && resourceLocks.Count == 0
          && _lockedResources.Remove(resource)
          && _dedicatedConnection.State == ConnectionState.Open)
        {
          PostgreSqlDistributedLock.Release(_dedicatedConnection, resource, _options);
        }
      }
      catch (Exception)
      {
        if (!onDisposing)
        {
          throw;
        }
      }
      finally
      {
        if (_lockedResources.Count == 0)
        {
          _storage.ReleaseConnection(_dedicatedConnection);
          _dedicatedConnection = null;
        }
      }
    }

    private class DisposableLock : IDisposable
    {
      private readonly PostgreSqlConnection _connection;
      private readonly Guid _lockId;
      private readonly string _resource;

      public DisposableLock(PostgreSqlConnection connection, string resource, Guid lockId)
      {
        _connection = connection;
        _resource = resource;
        _lockId = lockId;
      }

      public void Dispose()
      {
        _connection.ReleaseLock(_resource, _lockId, true);
      }
    }
  }
}
