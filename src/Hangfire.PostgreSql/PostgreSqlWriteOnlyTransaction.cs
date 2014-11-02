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

        public PostgreSqlWriteOnlyTransaction(
            NpgsqlConnection connection,
            PersistentJobQueueProviderCollection queueProviders)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            if (queueProviders == null) throw new ArgumentNullException("queueProviders");

            _connection = connection;
;
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
            }
        }

        public void ExpireJob(string jobId, TimeSpan expireIn)
        {
            string sql =
                string.Format(
                    @"UPDATE ""hangfire"".""job"" SET ""expireat"" = now() at time zone 'utc' + interval '{0} seconds' WHERE ""id"" = @id",
                    (long)expireIn.TotalSeconds);


            QueueCommand((con, trx) => con.Execute(
                sql,
                new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) }, trx));
        }

        public void PersistJob(string jobId)
        {
            QueueCommand((con, trx) => con.Execute(
                @"UPDATE ""hangfire"".""job"" SET ""expireat"" = NULL WHERE ""id"" = @id",
                new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) }, trx));
        }

        public void SetJobState(string jobId, IState state)
        {

            const string addAndSetStateSql = @"
WITH s AS (
INSERT INTO ""hangfire"".""state"" (""jobid"", ""name"", ""reason"", ""createdat"", ""data"") 
VALUES (@jobId, @name, @reason, @createdAt, @data) 
RETURNING ""id""
)
UPDATE ""hangfire"".""job"" j
SET ""stateid"" = s.""id"", 
  ""statename"" = @name 
FROM s
WHERE j.""id"" = @id;";

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
            const string addStateSql = @"
insert into ""hangfire"".""state"" (""jobid"", ""name"", ""reason"", ""createdat"", ""data"")
values (@jobId, @name, @reason, @createdAt, @data)";

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
                @"insert into ""hangfire"".""counter"" (""key"", ""value"") values (@key, @value)",
                new { key, value = +1 }, trx));
        }

        public void IncrementCounter(string key, TimeSpan expireIn)
        {
            string sql =
                string.Format(
                    @"insert into ""hangfire"".""counter""(""key"", ""value"", ""expireat"") values (@key, @value, now() at time zone 'utc' + interval '{0} seconds')",
                    (long)expireIn.TotalSeconds);


            QueueCommand((con, trx) => con.Execute(
                sql,
                new { key, value = +1 }, trx));
        }

        public void DecrementCounter(string key)
        {
            QueueCommand((con, trx) => con.Execute(
                @"insert into ""hangfire"".""counter""(""key"", ""value"") values (@key, @value)",
                new { key, value = -1 }, trx));
        }

        public void DecrementCounter(string key, TimeSpan expireIn)
        {
            string sql =
                string.Format(
                    @"insert into ""hangfire"".""counter""(""key"", ""value"", ""expireat"") values (@key, @value, now() at time zone 'utc' + interval '{0} seconds')",
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
            const string addSql = @"
WITH ""inputvalues"" AS (
	SELECT @key ""key"", @value ""value"", @score ""score""
), ""updatedrows"" AS ( 
	UPDATE ""hangfire"".""set"" ""updatetarget""
	SET ""score"" = ""inputvalues"".""score""
	FROM ""inputvalues""
	WHERE ""updatetarget"".""key"" = ""inputvalues"".""key""
	AND ""updatetarget"".""value"" = ""inputvalues"".""value""
	RETURNING ""updatetarget"".""key"", ""updatetarget"".""value""
)
INSERT INTO ""hangfire"".""set""(""key"", ""value"", ""score"")
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
                @"delete from ""hangfire"".""set"" where ""key"" = @key and ""value"" = @value",
                new { key, value }, trx));
        }

        public void InsertToList(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(
                @"insert into ""hangfire"".""list"" (""key"", ""value"") values (@key, @value)",
                new { key, value }, trx));
        }

        public void RemoveFromList(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(
                @"delete from ""hangfire"".""list"" where ""key"" = @key and ""value"" = @value",
                new { key, value }, trx));
        }

        public void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            const string trimSql =
                @"
delete from ""hangfire"".""list"" as source
where ""key"" = @key
and ""id"" not in (
    select ""id"" 
    from ""hangfire"".""list"" as keep
    where keep.""key"" = source.""key""
    order by ""id"" 
    offset @start limit @end);
";

            QueueCommand((con, trx) => con.Execute(
                trimSql,
                new { key = key, start = keepStartingFrom, end = (keepEndingAt - keepStartingFrom + 1) }, trx));
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
);";

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
                @"delete from ""hangfire"".""hash"" where ""key"" = @key",
                new { key }, trx));
        }

        internal void QueueCommand(Action<NpgsqlConnection, NpgsqlTransaction> action)
        {
            _commandQueue.Enqueue(action);
        }
    }
}