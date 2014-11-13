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
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Entities;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    internal class PostgreSqlConnection : IStorageConnection
    {
        private readonly NpgsqlConnection _connection;
        private readonly PersistentJobQueueProviderCollection _queueProviders;

        public PostgreSqlConnection(
            NpgsqlConnection connection, 
            PersistentJobQueueProviderCollection queueProviders)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            if (queueProviders == null) throw new ArgumentNullException("queueProviders");

            _connection = connection;
            _queueProviders = queueProviders;
        }

        public void Dispose()
        {
            _connection.Dispose();
        }

        public IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new PostgreSqlWriteOnlyTransaction(_connection, _queueProviders);
        }

        public IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new PostgreSqlDistributedLock(
                String.Format("HangFire:{0}", resource),
                timeout,
                _connection);
        }

        public IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException("queues");

            var providers = queues
                .Select(queue => _queueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException(String.Format(
                    "Multiple provider instances registered for queues: {0}. You should choose only one type of persistent queues per server instance.",
                    String.Join(", ", queues)));
            }

            var persistentQueue = providers[0].GetJobQueue(_connection); 
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public string CreateExpiredJob(
            Job job,
            IDictionary<string, string> parameters, 
            DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException("job");
            if (parameters == null) throw new ArgumentNullException("parameters");

            const string createJobSql = @"
INSERT INTO ""hangfire"".""job"" (""invocationdata"", ""arguments"", ""createdat"", ""expireat"")
VALUES (@invocationData, @arguments, @createdAt, @expireAt) 
RETURNING ""id"";
";

            var invocationData = InvocationData.Serialize(job);

            var jobId = _connection.Query<int>(
                createJobSql,
                new
                {
                    invocationData = JobHelper.ToJson(invocationData),
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

                const string insertParameterSql = @"
INSERT INTO ""hangfire"".""jobparameter"" (""jobid"", ""name"", ""value"")
VALUES (@jobId, @name, @value);
";

                _connection.Execute(insertParameterSql, parameterArray);
            }

            return jobId;
        }

        public JobData GetJobData(string id)
        {
            if (id == null) throw new ArgumentNullException("id");

            const string sql = 
                @"
SELECT ""invocationdata"" ""invocationData"", ""statename"" ""stateName"", ""arguments"", ""createdat"" ""createdAt"" 
FROM ""hangfire"".""job"" 
WHERE ""id"" = @id;
";

            var jobData = _connection.Query<SqlJob>(sql, new { id = Convert.ToInt32(id, CultureInfo.InvariantCulture) })
                .SingleOrDefault();

            if (jobData == null) return null;

            // TODO: conversion exception could be thrown.
            var invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);
            invocationData.Arguments = jobData.Arguments;

            Job job = null;
            JobLoadException loadException = null;

            try
            {
                job = invocationData.Deserialize();
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

        public StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException("jobId");

            const string sql = @"
SELECT s.""name"" ""Name"", s.""reason"" ""Reason"", s.""data"" ""Data""
FROM ""hangfire"".""state"" s
INNER JOIN ""hangfire"".""job"" j on j.""stateid"" = s.""id""
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
                Data = JobHelper.FromJson<Dictionary<string, string>>(sqlState.Data)
            };
        }

        public void SetJobParameter(string id, string name, string value)
        {
            if (id == null) throw new ArgumentNullException("id");
            if (name == null) throw new ArgumentNullException("name");

            const string sql = @"
WITH ""inputvalues"" AS (
	SELECT @jobid ""jobid"", @name ""name"", @value ""value""
), ""updatedrows"" AS ( 
	UPDATE ""hangfire"".""jobparameter"" ""updatetarget""
	SET ""value"" = ""inputvalues"".""value""
	FROM ""inputvalues""
	WHERE ""updatetarget"".""jobid"" = ""inputvalues"".""jobid""
	AND ""updatetarget"".""name"" = ""inputvalues"".""name""
	RETURNING ""updatetarget"".""jobid"", ""updatetarget"".""name""
)
INSERT INTO ""hangfire"".""jobparameter""(""jobid"", ""name"", ""value"")
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

                    public string GetJobParameter(string id, string name)
                    {
                        if (id == null) throw new ArgumentNullException("id");
                        if (name == null) throw new ArgumentNullException("name");

                        return _connection.Query<string>(
                            @"
SELECT ""value"" 
FROM ""hangfire"".""jobparameter"" 
WHERE ""jobid"" = @id 
AND ""name"" = @name;
",
                            new { id = Convert.ToInt32(id, CultureInfo.InvariantCulture), name = name })
                            .SingleOrDefault();
                    }

                    public HashSet<string> GetAllItemsFromSet(string key)
                    {
                        if (key == null) throw new ArgumentNullException("key");

                        var result = _connection.Query<string>(
                            @"
SELECT ""value"" 
FROM ""hangfire"".""set"" 
WHERE ""key"" = @key;
",
                            new { key });
            
                        return new HashSet<string>(result);
                    }

                    public string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
                    {
                        if (key == null) throw new ArgumentNullException("key");
                        if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

                        return _connection.Query<string>(
                            @"
SELECT ""value"" 
FROM ""hangfire"".""set"" 
WHERE ""key"" = @key 
AND ""score"" BETWEEN @from AND @to 
ORDER BY ""score"" LIMIT 1;
",
                            new { key, from = fromScore, to = toScore })
                            .SingleOrDefault();
                    }

                    public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
                    {
                        if (key == null) throw new ArgumentNullException("key");
                        if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

                        const string sql = @"
WITH ""inputvalues"" AS (
	SELECT @key ""key"", @field ""field"", @value ""value""
), ""updatedrows"" AS ( 
	UPDATE ""hangfire"".""hash"" ""updatetarget""
	SET ""value"" = ""inputvalues"".""value""
	FROM ""inputvalues""
	WHERE ""updatetarget"".""key"" = ""inputvalues"".""key""
	AND ""updatetarget"".""field"" = ""inputvalues"".""field""
	RETURNING ""updatetarget"".""key"", ""updatetarget"".""field""
)
INSERT INTO ""hangfire"".""hash""(""key"", ""field"", ""value"")
SELECT ""key"", ""field"", ""value"" FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
	SELECT 1 
	FROM ""updatedrows"" 
	WHERE ""updatedrows"".""key"" = ""insertvalues"".""key"" 
	AND ""updatedrows"".""field"" = ""insertvalues"".""field""
);
";

            using (var transaction = _connection.BeginTransaction(IsolationLevel.Serializable))
            {
                foreach (var keyValuePair in keyValuePairs)
                {
                    _connection.Execute(sql, new { key = key, field = keyValuePair.Key, value = keyValuePair.Value }, transaction);
                }
                transaction.Commit();
            }
        }

        public Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var result = _connection.Query<SqlHash>(
                @"
SELECT ""field"" ""Field"", ""value"" ""Value"" 
FROM ""hangfire"".""hash"" 
WHERE ""key"" = @key;
",
                new { key })
                .ToDictionary(x => x.Field, x => x.Value);

            return result.Count != 0 ? result : null;
        }

        public void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");
            if (context == null) throw new ArgumentNullException("context");

            var data = new ServerData
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = DateTime.UtcNow,
            };

            const string sql = @"
WITH ""inputvalues"" AS (
	SELECT @id ""id"", @data ""data"", NOW() AT TIME ZONE 'UTC' ""lastheartbeat""
), ""updatedrows"" AS ( 
	UPDATE ""hangfire"".""server"" ""updatetarget""
	SET ""data"" = ""inputvalues"".""data"", ""lastheartbeat"" = ""inputvalues"".""lastheartbeat""
	FROM ""inputvalues""
	WHERE ""updatetarget"".""id"" = ""inputvalues"".""id""
	RETURNING ""updatetarget"".""id""
)
INSERT INTO ""hangfire"".""server""(""id"", ""data"", ""lastheartbeat"")
SELECT ""id"", ""data"", ""lastheartbeat"" FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
	SELECT 1 
	FROM ""updatedrows"" 
	WHERE ""updatedrows"".""id"" = ""insertvalues"".""id"" 
);
";

            _connection.Execute(sql,
                new { id = serverId, data = JobHelper.ToJson(data) });
        }

        public void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");

            _connection.Execute(
                @"
DELETE FROM ""hangfire"".""server"" 
WHERE ""id"" = @id;
",
                new { id = serverId });
        }

        public void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");

            _connection.Execute(
                @"
UPDATE ""hangfire"".""server"" 
SET ""lastheartbeat"" = NOW() AT TIME ZONE 'UTC' 
WHERE ""id"" = @id;
",
                new { id = serverId });
        }

        public int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", "timeOut");
            }

            return _connection.Execute(
                string.Format(@"
DELETE FROM ""hangfire"".""server"" 
WHERE ""lastheartbeat"" < (NOW() AT TIME ZONE 'UTC' - INTERVAL '{0} MILLISECONDS');
", (long)timeOut.TotalMilliseconds));
        }
    }
}
