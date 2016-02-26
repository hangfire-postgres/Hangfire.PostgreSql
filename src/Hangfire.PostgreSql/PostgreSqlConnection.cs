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
using Hangfire.Annotations;

namespace Hangfire.PostgreSql
{
    public class PostgreSqlConnection : JobStorageConnection, IStorageConnection
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
            if (connection == null) throw new ArgumentNullException("connection");
            if (queueProviders == null) throw new ArgumentNullException("queueProviders");
            if (options == null) throw new ArgumentNullException("options");

            _connection = connection;
            _queueProviders = queueProviders;
            _options = options;
            OwnsConnection = ownsConnection;
        }

        public bool OwnsConnection { get; private set; }
        public NpgsqlConnection Connection { get { return _connection; } }

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
                String.Format("HangFire:{0}", resource),
                timeout,
                _connection,
                _options);
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
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

        public override string CreateExpiredJob(
            Job job,
            IDictionary<string, string> parameters, 
            DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException("job");
            if (parameters == null) throw new ArgumentNullException("parameters");

            string createJobSql = @"
INSERT INTO """ + _options.SchemaName + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"", ""expireat"")
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
            if (id == null) throw new ArgumentNullException("id");

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

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException("jobId");

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
                Data = JobHelper.FromJson<Dictionary<string, string>>(sqlState.Data)
            };
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null) throw new ArgumentNullException("id");
            if (name == null) throw new ArgumentNullException("name");

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
                        if (id == null) throw new ArgumentNullException("id");
                        if (name == null) throw new ArgumentNullException("name");

                        return _connection.Query<string>(
                            @"
SELECT ""value"" 
FROM """ + _options.SchemaName + @""".""jobparameter"" 
WHERE ""jobid"" = @id 
AND ""name"" = @name;
",
                new { id = Convert.ToInt32(id, CultureInfo.InvariantCulture), name = name })
                            .SingleOrDefault();
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
                    {
                        if (key == null) throw new ArgumentNullException("key");

                        var result = _connection.Query<string>(
                            @"
SELECT ""value"" 
FROM """ + _options.SchemaName + @""".""set"" 
WHERE ""key"" = @key;
",
                            new { key });
            
                        return new HashSet<string>(result);
                    }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
                    {
                        if (key == null) throw new ArgumentNullException("key");
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
                        if (key == null) throw new ArgumentNullException("key");
                        if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

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

            using (var transaction = _connection.BeginTransaction(IsolationLevel.Serializable))
            {
                foreach (var keyValuePair in keyValuePairs)
                {
                    _connection.Execute(sql, new { key = key, field = keyValuePair.Key, value = keyValuePair.Value }, transaction);
                }
                transaction.Commit();
            }
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var result = _connection.Query<SqlHash>(
                @"
SELECT ""field"" ""Field"", ""value"" ""Value"" 
FROM """ + _options.SchemaName + @""".""hash"" 
WHERE ""key"" = @key;
",
                new { key })
                .ToDictionary(x => x.Field, x => x.Value);

            return result.Count != 0 ? result : null;
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");
            if (context == null) throw new ArgumentNullException("context");

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
                new { id = serverId, data = JobHelper.ToJson(data) });
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");

            _connection.Execute(
                @"
DELETE FROM """ + _options.SchemaName + @""".""server"" 
WHERE ""id"" = @id;
",
                new { id = serverId });
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");

            _connection.Execute(
                @"
UPDATE """ + _options.SchemaName + @""".""server"" 
SET ""lastheartbeat"" = NOW() AT TIME ZONE 'UTC' 
WHERE ""id"" = @id;
",
                new { id = serverId });
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", "timeOut");
            }

            return _connection.Execute(
                string.Format(@"
DELETE FROM """ + _options.SchemaName + @""".""server"" 
WHERE ""lastheartbeat"" < (NOW() AT TIME ZONE 'UTC' - INTERVAL '{0} MILLISECONDS');
", (long)timeOut.TotalMilliseconds));
        }



        ///////////////
        public override long GetSetCount([NotNull] string key)
        {
            string sql = @" SELECT count(*) FROM """ + _options.SchemaName + @""".""set"" WHERE ""key"" = @key;";
            return  _connection.Query<int>(sql, new { key = key }).SingleOrDefault();
        }

        public override List<string> GetRangeFromSet([NotNull] string key, int startingFrom, int endingAt)
        {
            string sql = @" SELECT value FROM """ + _options.SchemaName + @""".""set"" WHERE ""key"" = @key ORDER BY value LIMIT @count OFFSET @start ;";
            return  _connection.Query<string>(sql, new { key = key, start=startingFrom,count=(endingAt-startingFrom+1) }).ToList();
            //throw new NotSupportedException();
        }

        public override TimeSpan GetSetTtl([NotNull] string key)
        {
            string sql = @"SELECT min(expireat) FROM """ + _options.SchemaName + @""".""set"" WHERE ""key"" = @key;";
            return  _connection.Query<TimeSpan>(sql, new { key = key }).SingleOrDefault();
        }


        public override string GetValueFromHash([NotNull] string key, [NotNull] string name)
        {
            string sql = @" SELECT value FROM """ + _options.SchemaName + @""".""hash"" WHERE ""key"" = @key AND field = @field;";
            return  _connection.Query<string>(sql, new { key = key, field = name }).SingleOrDefault();
        }

        public override long GetHashCount([NotNull] string key)
        {
            string sql = @" SELECT count(*) FROM """ + _options.SchemaName + @""".""hash"" WHERE ""key"" = @key;";
            return  _connection.Query<long>(sql, new { key = key }).SingleOrDefault();
        }

        public override TimeSpan GetHashTtl([NotNull] string key)
        {
            string sql = @"SELECT min(expireat) FROM """ + _options.SchemaName + @""".""hash"" WHERE ""key"" = @key;";
            return  _connection.Query<TimeSpan>(sql, new { key = key }).SingleOrDefault();
        }

        // Lists
        public override long GetListCount([NotNull] string key)
        {
            string sql = @" SELECT count(*) FROM """ + _options.SchemaName + @""".""list"" WHERE ""key"" = @key;";
            return  _connection.Query<int>(sql, new { key = key }).SingleOrDefault();
        }

        public override List<string> GetAllItemsFromList([NotNull] string key)
        {
            string sql = @" SELECT value FROM """ + _options.SchemaName + @""".""list"" WHERE ""key"" = @key ORDER BY id DESC;";
            return  _connection.Query<string>(sql, new { key = key}).ToList();
        }

        public override List<string> GetRangeFromList([NotNull] string key, int startingFrom, int endingAt)
        {
            string sql = @" SELECT value FROM """ + _options.SchemaName + @""".""list"" WHERE ""key"" = @key ORDER BY id DESC LIMIT @count OFFSET @start ;";
            return  _connection.Query<string>(sql, new { key = key, start=startingFrom,count=(endingAt-startingFrom+1) }).ToList();
        }

        public override TimeSpan GetListTtl([NotNull] string key)
        {   
            string sql = @"SELECT min(expireat) FROM """ + _options.SchemaName + @""".""list"" WHERE ""key"" = @key;";
            return  _connection.Query<TimeSpan>(sql, new { key = key }).SingleOrDefault();
        }

        // Counters
        public override long GetCounter([NotNull] string key)
        {            
            string sql = string.Format(@"select sum(value) from ""{0}"".counter where key = @key", _options.SchemaName);
            return _connection.Query<long>(sql, new { key = key }).SingleOrDefault();
        }
    }
}
