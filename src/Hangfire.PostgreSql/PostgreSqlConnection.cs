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
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Entities;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;
using Hangfire.Annotations;

namespace Hangfire.PostgreSql
{
	public class PostgreSqlConnection : JobStorageConnection
	{
		private readonly NpgsqlConnection _connection;
		private readonly PersistentJobQueueProviderCollection _queueProviders;
		private readonly PostgreSqlStorageOptions _options;

		public PostgreSqlConnection(
			NpgsqlConnection connection,
			PersistentJobQueueProviderCollection queueProviders,
			PostgreSqlStorageOptions options)
			: this(connection, queueProviders, options, true)
		{
		}

		public PostgreSqlConnection(
			NpgsqlConnection connection,
			PersistentJobQueueProviderCollection queueProviders,
			PostgreSqlStorageOptions options,
			bool ownsConnection)
		{
			_connection = connection ?? throw new ArgumentNullException(nameof(connection));
			_queueProviders = queueProviders ?? throw new ArgumentNullException(nameof(queueProviders));
			_options = options ?? throw new ArgumentNullException(nameof(options));
			OwnsConnection = ownsConnection;
		}

		public bool OwnsConnection { get; private set; }
		public NpgsqlConnection Connection => _connection;

		public override void Dispose()
		{
            base.Dispose();
			if (OwnsConnection)
			{
				_connection.Dispose();
			}
		}

		public override IWriteOnlyTransaction CreateWriteTransaction()
		{
			return new PostgreSqlWriteOnlyTransaction(_connection, _options, _queueProviders);
		}

		public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
		{
			return new PostgreSqlDistributedLock(
				$"HangFire:{resource}",
				timeout,
				_connection,
				_options);
		}

		public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
		{
			if (queues == null || queues.Length == 0) throw new ArgumentNullException(nameof(queues));

			var providers = queues
				.Select(queue => _queueProviders.GetProvider(queue))
				.Distinct()
				.ToArray();

			if (providers.Length != 1)
			{
				throw new InvalidOperationException(
					$"Multiple provider instances registered for queues: {String.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");
			}

			var persistentQueue = providers[0].GetJobQueue();
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

			string createJobSql = @"
INSERT INTO """ + _options.SchemaName + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"", ""expireat"")
VALUES (@invocationData, @arguments, @createdAt, @expireAt) 
RETURNING ""id"";
";

			var invocationData = InvocationData.SerializeJob(job);

			var jobId = _connection.Query<long>(
				createJobSql,
				new
				{
					invocationData = SerializationHelper.Serialize(invocationData),
					arguments = invocationData.Arguments,
					createdAt = createdAt,
					expireAt = createdAt.Add(expireIn)
				}).Single().ToString(CultureInfo.InvariantCulture);

			if (parameters.Count > 0)
			{
				var parameterArray = new object[parameters.Count];
				int parameterIndex = 0;
				foreach (var parameter in parameters)
				{
					parameterArray[parameterIndex++] = new
					{
						jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture),
						name = parameter.Key,
						value = parameter.Value
					};
				}

				string insertParameterSql = @"
INSERT INTO """ + _options.SchemaName + @""".""jobparameter"" (""jobid"", ""name"", ""value"")
VALUES (@jobId, @name, @value);
";

				_connection.Execute(insertParameterSql, parameterArray);
			}

			return jobId;
		}

		public override JobData GetJobData(string id)
		{
			if (id == null) throw new ArgumentNullException(nameof(id));

			string sql =
				@"
SELECT ""invocationdata"" ""invocationData"", ""statename"" ""stateName"", ""arguments"", ""createdat"" ""createdAt"" 
FROM """ + _options.SchemaName + @""".""job"" 
WHERE ""id"" = @id;
";

			var jobData = _connection.Query<SqlJob>(sql, new { id = Convert.ToInt32(id, CultureInfo.InvariantCulture) })
				.SingleOrDefault();

			if (jobData == null) return null;

			// TODO: conversion exception could be thrown.
			var invocationData = SerializationHelper.Deserialize<InvocationData>(jobData.InvocationData);
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

			return new JobData
			{
				Job = job,
				State = jobData.StateName,
				CreatedAt = jobData.CreatedAt,
				LoadException = loadException
			};
		}

		public override StateData GetStateData(string jobId)
		{
			if (jobId == null) throw new ArgumentNullException(nameof(jobId));

			string sql = @"
SELECT s.""name"" ""Name"", s.""reason"" ""Reason"", s.""data"" ""Data""
FROM """ + _options.SchemaName + @""".""state"" s
INNER JOIN """ + _options.SchemaName + @""".""job"" j on j.""stateid"" = s.""id""
WHERE j.""id"" = @jobId;
";

			var sqlState = _connection.Query<SqlState>(sql, new { jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) }).SingleOrDefault();
			if (sqlState == null)
			{
				return null;
			}

			return new StateData
			{
				Name = sqlState.Name,
				Reason = sqlState.Reason,
				Data = SerializationHelper.Deserialize<Dictionary<string, string>>(sqlState.Data)
			};
		}

		public override void SetJobParameter(string id, string name, string value)
		{
			if (id == null) throw new ArgumentNullException(nameof(id));
			if (name == null) throw new ArgumentNullException(nameof(name));

			string sql = @"
WITH ""inputvalues"" AS (
	SELECT @jobid ""jobid"", @name ""name"", @value ""value""
), ""updatedrows"" AS ( 
	UPDATE """ + _options.SchemaName + @""".""jobparameter"" ""updatetarget""
	SET ""value"" = ""inputvalues"".""value""
	FROM ""inputvalues""
	WHERE ""updatetarget"".""jobid"" = ""inputvalues"".""jobid""
	AND ""updatetarget"".""name"" = ""inputvalues"".""name""
	RETURNING ""updatetarget"".""jobid"", ""updatetarget"".""name""
)
INSERT INTO """ + _options.SchemaName + @""".""jobparameter""(""jobid"", ""name"", ""value"")
SELECT ""jobid"", ""name"", ""value"" 
FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
	SELECT 1 
	FROM ""updatedrows"" 
	WHERE ""updatedrows"".""jobid"" = ""insertvalues"".""jobid"" 
	AND ""updatedrows"".""name"" = ""insertvalues"".""name""
);";

			_connection.Execute(sql,
				new { jobId = Convert.ToInt32(id, CultureInfo.InvariantCulture), name, value });
		}

		public override string GetJobParameter(string id, string name)
		{
			if (id == null) throw new ArgumentNullException(nameof(id));
			if (name == null) throw new ArgumentNullException(nameof(name));

			string query = $@"SELECT ""value"" FROM ""{_options.SchemaName}"".""jobparameter"" WHERE ""jobid"" = @id AND ""name"" = @name;
";

			return _connection.Query<string>(query,
				new { id = Convert.ToInt32(id, CultureInfo.InvariantCulture), name = name })
				.SingleOrDefault();
		}

		public override HashSet<string> GetAllItemsFromSet(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			string query = $@"SELECT ""value"" FROM ""{_options.SchemaName}"".""set"" WHERE ""key"" = @key;";

			var result = _connection.Query<string>(query, new { key });

			return new HashSet<string>(result);
		}

		public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));
			if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

			return _connection.Query<string>(
				@"
SELECT ""value"" 
FROM """ + _options.SchemaName + @""".""set"" 
WHERE ""key"" = @key 
AND ""score"" BETWEEN @from AND @to 
ORDER BY ""score"" LIMIT 1;
",
				new { key, from = fromScore, to = toScore })
				.SingleOrDefault();
		}

		public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));
			if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

			string sql = @"
WITH ""inputvalues"" AS (
	SELECT @key ""key"", @field ""field"", @value ""value""
), ""updatedrows"" AS ( 
	UPDATE """ + _options.SchemaName + @""".""hash"" ""updatetarget""
	SET ""value"" = ""inputvalues"".""value""
	FROM ""inputvalues""
	WHERE ""updatetarget"".""key"" = ""inputvalues"".""key""
	AND ""updatetarget"".""field"" = ""inputvalues"".""field""
	RETURNING ""updatetarget"".""key"", ""updatetarget"".""field""
)
INSERT INTO """ + _options.SchemaName + @""".""hash""(""key"", ""field"", ""value"")
SELECT ""key"", ""field"", ""value"" FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
	SELECT 1 
	FROM ""updatedrows"" 
	WHERE ""updatedrows"".""key"" = ""insertvalues"".""key"" 
	AND ""updatedrows"".""field"" = ""insertvalues"".""field""
);
";
			var execute = true;
			var executionTimer = Stopwatch.StartNew();
			while (execute)
			{
				try
				{
					using (var transaction = _connection.BeginTransaction(IsolationLevel.Serializable))
					{
						foreach (var keyValuePair in keyValuePairs)
						{
							_connection.Execute(sql, new {key = key, field = keyValuePair.Key, value = keyValuePair.Value}, transaction);
						}
						transaction.Commit();
						execute = false;
					}
				}
				catch (PostgresException exception)
				{
					if (!exception.SqlState.Equals("40001"))
						throw;
				}

				if(executionTimer.Elapsed > _options.TransactionSynchronisationTimeout)
					throw new TimeoutException("SetRangeInHash experienced timeout while trying to execute transaction");
			}
		}

		public override Dictionary<string, string> GetAllEntriesFromHash(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			var result = _connection.Query<SqlHash>(
				$@"SELECT ""field"" ""Field"", ""value"" ""Value"" 
					FROM ""{_options.SchemaName}"".""hash"" 
					WHERE ""key"" = @key;
					",
				new { key })
				.ToDictionary(x => x.Field, x => x.Value);

			return result.Count != 0 ? result : null;
		}

		public override void AnnounceServer(string serverId, ServerContext context)
		{
			if (serverId == null) throw new ArgumentNullException(nameof(serverId));
			if (context == null) throw new ArgumentNullException(nameof(context));

			var data = new ServerData
			{
				WorkerCount = context.WorkerCount,
				Queues = context.Queues,
				StartedAt = DateTime.UtcNow,
			};

			string sql = @"
WITH ""inputvalues"" AS (
	SELECT @id ""id"", @data ""data"", NOW() AT TIME ZONE 'UTC' ""lastheartbeat""
), ""updatedrows"" AS ( 
	UPDATE """ + _options.SchemaName + @""".""server"" ""updatetarget""
	SET ""data"" = ""inputvalues"".""data"", ""lastheartbeat"" = ""inputvalues"".""lastheartbeat""
	FROM ""inputvalues""
	WHERE ""updatetarget"".""id"" = ""inputvalues"".""id""
	RETURNING ""updatetarget"".""id""
)
INSERT INTO """ + _options.SchemaName + @""".""server""(""id"", ""data"", ""lastheartbeat"")
SELECT ""id"", ""data"", ""lastheartbeat"" FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
	SELECT 1 
	FROM ""updatedrows"" 
	WHERE ""updatedrows"".""id"" = ""insertvalues"".""id"" 
);
";

			_connection.Execute(sql,
				new { id = serverId, data = SerializationHelper.Serialize(data) });
		}

		public override void RemoveServer(string serverId)
		{
			if (serverId == null) throw new ArgumentNullException(nameof(serverId));

			_connection.Execute(
				$@"DELETE FROM ""{_options.SchemaName}"".""server"" WHERE ""id"" = @id;",
				new { id = serverId });
		}

		public override void Heartbeat(string serverId)
		{
			if (serverId == null) throw new ArgumentNullException(nameof(serverId));

			string query =
				$@"UPDATE ""{_options.SchemaName}"".""server"" 
				SET ""lastheartbeat"" = NOW() AT TIME ZONE 'UTC' 
				WHERE ""id"" = @id;";

			_connection.Execute(query, new { id = serverId });
		}

		public override int RemoveTimedOutServers(TimeSpan timeOut)
		{
			if (timeOut.Duration() != timeOut)
			{
				throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
			}

			string query =
				$@"DELETE FROM ""{_options.SchemaName}"".""server"" 
				WHERE ""lastheartbeat"" < (NOW() AT TIME ZONE 'UTC' - INTERVAL '{(
					long) timeOut.TotalMilliseconds} MILLISECONDS');";

			return _connection.Execute(query);
		}

		public override long GetSetCount(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			string query = $@"select count(""key"") from ""{_options.SchemaName}"".""set"" where ""key"" = @key";

			return _connection.Query<long>(query, new {key}).First();
		}

		public override List<string> GetAllItemsFromList(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			string query = $@"select ""value"" from ""{_options.SchemaName}"".""list"" where ""key"" = @key order by ""id"" desc";

			return _connection.Query<string>(query, new { key }).ToList();
		}

		public override long GetCounter(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			string query = $@"select sum(s.""Value"") from (select sum(""value"") as ""Value"" from ""{_options.SchemaName}"".""counter"" where ""key"" = @key) s";

			return _connection.Query<long?>(query, new {key}).SingleOrDefault() ?? 0;
		}

		public override long GetListCount(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			string query = $@"select count(""id"") from ""{_options.SchemaName}"".""list"" where ""key"" = @key";

			return _connection.Query<long>(query, new { key }).SingleOrDefault();
		}

		public override TimeSpan GetListTtl(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			string query = $@"select min(""expireat"") from ""{_options.SchemaName}"".""list"" where ""key"" = @key";

			var result = _connection.Query<DateTime?>(query, new { key }).Single();
			if (!result.HasValue) return TimeSpan.FromSeconds(-1);

			return result.Value - DateTime.UtcNow;
		}

		public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			string query = $@"select ""value"" from (
					select ""value"", row_number() over (order by ""id"" desc) as row_num 
					from ""{_options.SchemaName}"".""list""
					where ""key"" = @key 
				) as s where s.row_num between @startingFrom and @endingAt";

			return _connection.Query<string>(query, new {key, startingFrom = startingFrom + 1, endingAt = endingAt + 1}).ToList();
		}

		public override long GetHashCount(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			string query = $@"select count(""id"") from ""{_options.SchemaName}"".""hash"" where ""key"" = @key";

			return _connection.Query<long>(query, new { key }).SingleOrDefault();
		}

		public override TimeSpan GetHashTtl(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			string query = $@"select min(""expireat"") from ""{_options.SchemaName}"".""hash"" where ""key"" = @key";

			var result = _connection.Query<DateTime?>(query, new { key }).Single();
			if (!result.HasValue) return TimeSpan.FromSeconds(-1);

			return result.Value - DateTime.UtcNow;
		}

		public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			string query = $@"select ""value"" from (
					select ""value"", row_number() over (order by ""id"" ASC) as row_num 
					from ""{_options.SchemaName}"".""set""
					where ""key"" = @key 
				) as s where s.row_num between @startingFrom and @endingAt";

			return _connection.Query<string>(query, new { key, startingFrom = startingFrom + 1, endingAt = endingAt + 1 }).ToList();
		}

		public override TimeSpan GetSetTtl(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			string query = $@"select min(""expireat"") from ""{_options.SchemaName}"".""set"" where ""key"" = @key";

			var result = _connection.Query<DateTime?>(query, new { key }).SingleOrDefault();
			if (!result.HasValue) return TimeSpan.FromSeconds(-1);

			return result.Value - DateTime.UtcNow;
		}

		public override string GetValueFromHash(string key, string name)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));
			if (name == null) throw new ArgumentNullException(nameof(name));

			string query = $@"select ""value"" from ""{_options.SchemaName}"".""hash"" where ""key"" = @key and ""field"" = @field";

			return _connection.Query<string>(query, new { key, field = name }).SingleOrDefault();
		}
    }
}
