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
using Dapper;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    internal class PostgreSqlWriteOnlyTransaction : IWriteOnlyTransaction
    {
        private readonly Queue<Action<NpgsqlConnection, NpgsqlTransaction>> _commandQueue
            = new Queue<Action<NpgsqlConnection, NpgsqlTransaction>>();

        private readonly NpgsqlConnection _connection;
        private readonly PersistentJobQueueProviderCollection _queueProviders;
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlWriteOnlyTransaction(
            NpgsqlConnection connection,
            PostgreSqlStorageOptions options,
            PersistentJobQueueProviderCollection queueProviders)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            if (queueProviders == null) throw new ArgumentNullException("queueProviders");
            if (options == null) throw new ArgumentNullException("options");

            _connection = connection;
;
            _options = options;
            _queueProviders = queueProviders;
        }

        public void Dispose()
        {
        }

        public void Commit()
        {
            using (var transaction = _connection.BeginTransaction(IsolationLevel.RepeatableRead))
            {

                foreach (var command in _commandQueue)
                {
                    try
                    {
                        command(_connection, transaction);
                    }
                    catch
                    {
                        throw;
                    }
                }
                transaction.Commit();
                PostgreSqlJobQueue.NewItemInQueueEvent.Set();
            }
        }

        public void ExpireJob(string jobId, TimeSpan expireIn)
        {
            string sql =
                string.Format(
                    @"
UPDATE """ + _options.SchemaName + @""".""job""
SET ""expireat"" = NOW() AT TIME ZONE 'UTC' + INTERVAL '{0} SECONDS'
WHERE ""id"" = @id;
",
                    (long)expireIn.TotalSeconds);


            QueueCommand((con, trx) => con.Execute(
                sql,
                new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) }, trx));
        }

        public void PersistJob(string jobId)
        {
            QueueCommand((con, trx) => con.Execute(
                @"
UPDATE """ + _options.SchemaName + @""".""job"" 
SET ""expireat"" = NULL 
WHERE ""id"" = @id;
",
                new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) }, trx));
        }

        public void SetJobState(string jobId, IState state)
        {

            string addAndSetStateSql = @"
WITH s AS (
    INSERT INTO """ + _options.SchemaName + @""".""state"" (""jobid"", ""name"", ""reason"", ""createdat"", ""data"")
    VALUES (@jobId, @name, @reason, @createdAt, @data) RETURNING ""id""
)
UPDATE """ + _options.SchemaName + @""".""job"" j
SET ""stateid"" = s.""id"", ""statename"" = @name
FROM s
WHERE j.""id"" = @id;
";

            QueueCommand((con, trx) => con.Execute(
                addAndSetStateSql,
                new
                {
                    jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture),
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = JobHelper.ToJson(state.SerializeData()),
                    id = Convert.ToInt32(jobId,CultureInfo.InvariantCulture)
                }, trx));
        }

        public void AddJobState(string jobId, IState state)
        {
            string addStateSql = @"
INSERT INTO """ + _options.SchemaName + @""".""state"" (""jobid"", ""name"", ""reason"", ""createdat"", ""data"")
VALUES (@jobId, @name, @reason, @createdAt, @data);
";

            QueueCommand((con, trx) => con.Execute(
                addStateSql,
                new
                {
                    jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture),
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = JobHelper.ToJson(state.SerializeData())
                }, trx));
        }

        public void AddToQueue(string queue, string jobId)
        {
            var provider = _queueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue(_connection);

            QueueCommand((con, trx) => persistentQueue.Enqueue(queue, jobId));
        }

 
       public void IncrementCounter(string key)
        {
            QueueCommand((con, trx) => con.Execute(
                @"
INSERT INTO """ + _options.SchemaName + @""".""counter"" (""key"", ""value"") 
VALUES (@key, @value);
",
                new { key, value = +1 }, trx));
        }

        public void IncrementCounter(string key, TimeSpan expireIn)
        {
            string sql =
                string.Format(
                    @"
INSERT INTO """ + _options.SchemaName + @""".""counter""(""key"", ""value"", ""expireat"") 
VALUES (@key, @value, NOW() AT TIME ZONE 'UTC' + INTERVAL '{0} SECONDS');
",
                    (long)expireIn.TotalSeconds);


            QueueCommand((con, trx) => con.Execute(
                sql,
                new { key, value = +1 }, trx));
        }

        public void DecrementCounter(string key)
        {
            QueueCommand((con, trx) => con.Execute(
                @"
INSERT INTO """ + _options.SchemaName + @""".""counter""(""key"", ""value"") 
VALUES (@key, @value)
",
                new { key, value = -1 }, trx));
        }

        public void DecrementCounter(string key, TimeSpan expireIn)
        {
            string sql =
                string.Format(
                    @"
INSERT INTO """ + _options.SchemaName + @""".""counter""(""key"", ""value"", ""expireat"") 
VALUES (@key, @value, NOW() AT TIME ZONE 'UTC' + INTERVAL '{0} SECONDS');
",
                    (long) expireIn.TotalSeconds);

            QueueCommand((con, trx) => con.Execute(sql
                ,
                new { key, value = -1 }, trx));
        }

        public void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public void AddToSet(string key, string value, double score)
        {
            string addSql = @"
WITH ""inputvalues"" AS (
	SELECT @key ""key"", @value ""value"", @score ""score""
), ""updatedrows"" AS ( 
	UPDATE """ + _options.SchemaName + @""".""set"" ""updatetarget""
	SET ""score"" = ""inputvalues"".""score""
	FROM ""inputvalues""
	WHERE ""updatetarget"".""key"" = ""inputvalues"".""key""
	AND ""updatetarget"".""value"" = ""inputvalues"".""value""
	RETURNING ""updatetarget"".""key"", ""updatetarget"".""value""
)
INSERT INTO """ + _options.SchemaName + @""".""set""(""key"", ""value"", ""score"")
SELECT ""key"", ""value"", ""score"" FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
	SELECT 1 
	FROM ""updatedrows"" 
	WHERE ""updatedrows"".""key"" = ""insertvalues"".""key"" 
	AND ""updatedrows"".""value"" = ""insertvalues"".""value""
);
";

            QueueCommand((con, trx) => con.Execute(
                addSql,
                new { key, value, score }, trx));
        }

        public void RemoveFromSet(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(
                @"
DELETE FROM """ + _options.SchemaName + @""".""set"" 
WHERE ""key"" = @key 
AND ""value"" = @value;
",
                new { key, value }, trx));
        }

        public void InsertToList(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(
                @"
INSERT INTO """ + _options.SchemaName + @""".""list"" (""key"", ""value"") 
VALUES (@key, @value);
",
                new { key, value }, trx));
        }

        public void RemoveFromList(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(
                @"
DELETE FROM """ + _options.SchemaName + @""".""list"" 
WHERE ""key"" = @key 
AND ""value"" = @value;
",
                new { key, value }, trx));
        }

        public void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            string trimSql =
                @"
DELETE FROM """ + _options.SchemaName + @""".""list"" AS source
WHERE ""key"" = @key
AND ""id"" NOT IN (
    SELECT ""id"" 
    FROM """ + _options.SchemaName + @""".""list"" AS keep
    WHERE keep.""key"" = source.""key""
    ORDER BY ""id"" 
    OFFSET @start LIMIT @end
);
";

            QueueCommand((con, trx) => con.Execute(
                trimSql,
                new { key = key, start = keepStartingFrom, end = (keepEndingAt - keepStartingFrom + 1) }, trx));
        }

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
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
SELECT ""key"", ""field"", ""value"" 
FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
	SELECT 1 
	FROM ""updatedrows"" 
	WHERE ""updatedrows"".""key"" = ""insertvalues"".""key"" 
	AND ""updatedrows"".""field"" = ""insertvalues"".""field""
);
";

            foreach (var keyValuePair in keyValuePairs)
            {
                var pair = keyValuePair;

                QueueCommand((con, trx) => con.Execute(sql, new { key = key, field = pair.Key, value = pair.Value }, trx));
            }
        }

        public void RemoveHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            QueueCommand((con, trx) => con.Execute(
                @"
DELETE FROM """ + _options.SchemaName + @""".""hash"" 
WHERE ""key"" = @key;
",
                new { key }, trx));
        }

        internal void QueueCommand(Action<NpgsqlConnection, NpgsqlTransaction> action)
        {
            _commandQueue.Enqueue(action);
        }
    }
}