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
using System.Globalization;
using System.Linq;
using System.Transactions;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Extensions;
using Hangfire.States;
using Hangfire.Storage;
using IsolationLevel = System.Transactions.IsolationLevel;

namespace Hangfire.PostgreSql
{
  public class PostgreSqlWriteOnlyTransaction : JobStorageTransaction
  {
    private readonly Queue<Action<IDbConnection>> _commandQueue = new Queue<Action<IDbConnection>>();
    private readonly Func<DbConnection> _dedicatedConnectionFunc;

    private readonly PostgreSqlStorage _storage;

    public PostgreSqlWriteOnlyTransaction(
      PostgreSqlStorage storage,
      Func<DbConnection> dedicatedConnectionFunc)
    {
      _storage = storage ?? throw new ArgumentNullException(nameof(storage));
      _dedicatedConnectionFunc = dedicatedConnectionFunc ?? throw new ArgumentNullException(nameof(dedicatedConnectionFunc));
    }

    public override void Commit()
    {
      _storage.UseTransaction(_dedicatedConnectionFunc(), (connection, transaction) => {
        foreach (Action<IDbConnection> command in _commandQueue)
        {
          command(connection);
        }
      }, CreateTransactionScope);
    }

    private TransactionScope CreateTransactionScope()
    {
      IsolationLevel isolationLevel = IsolationLevel.ReadCommitted;
      TransactionScopeOption scopeOption = TransactionScopeOption.RequiresNew;
      if (_storage.Options.EnableTransactionScopeEnlistment)
      {
        Transaction currentTransaction = Transaction.Current;
        if (currentTransaction != null)
        {
          isolationLevel = currentTransaction.IsolationLevel;
          scopeOption = TransactionScopeOption.Required;
        }
      }

      TransactionOptions transactionOptions = new TransactionOptions {
        IsolationLevel = isolationLevel,
        Timeout = TransactionManager.MaximumTimeout,
      };

      return new TransactionScope(scopeOption, transactionOptions);
    }

    public override void ExpireJob(string jobId, TimeSpan expireIn)
    {
      string sql = $@"
        UPDATE ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"job".GetProperDbObjectName()}""
        SET ""{"expireat".GetProperDbObjectName()}"" = NOW() AT TIME ZONE 'UTC' + INTERVAL '{(long)expireIn.TotalSeconds} SECONDS'
        WHERE ""{"id".GetProperDbObjectName()}"" = @Id;
      ";

      QueueCommand(con => con.Execute(sql,
        new { Id = Convert.ToInt64(jobId, CultureInfo.InvariantCulture) }));
    }

    public override void PersistJob(string jobId)
    {
      string sql = $@"
        UPDATE ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"job".GetProperDbObjectName()}"" 
        SET ""{"expireat".GetProperDbObjectName()}"" = NULL 
        WHERE ""{"id".GetProperDbObjectName()}"" = @Id;
      ";
      
      QueueCommand(con => con.Execute(sql,
        new { Id = Convert.ToInt64(jobId, CultureInfo.InvariantCulture) }));
    }

    public override void SetJobState(string jobId, IState state)
    {
      string addAndSetStateSql = $@"
        WITH ""s"" AS (
            INSERT INTO ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"state".GetProperDbObjectName()}"" 
            (""{"jobid".GetProperDbObjectName()}"", ""{"name".GetProperDbObjectName()}"", ""{"reason".GetProperDbObjectName()}"", ""{"createdat".GetProperDbObjectName()}"", ""{"data".GetProperDbObjectName()}"")
            VALUES (@JobId, @Name, @Reason, @CreatedAt, @Data) RETURNING ""{"id".GetProperDbObjectName()}""
        )
        UPDATE ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"job".GetProperDbObjectName()}"" ""j""
        SET ""{"stateid".GetProperDbObjectName()}"" = s.""{"id".GetProperDbObjectName()}"", ""{"statename".GetProperDbObjectName()}"" = @Name
        FROM ""s""
        WHERE ""j"".""{"id".GetProperDbObjectName()}"" = @Id;
      ";

      QueueCommand(con => con.Execute(addAndSetStateSql,
        new {
          JobId = Convert.ToInt64(jobId, CultureInfo.InvariantCulture),
          state.Name,
          state.Reason,
          CreatedAt = DateTime.UtcNow,
          Data = SerializationHelper.Serialize(state.SerializeData()),
          Id = Convert.ToInt64(jobId, CultureInfo.InvariantCulture),
        }));
    }

    public override void AddJobState(string jobId, IState state)
    {
      string addStateSql = $@"
        INSERT INTO ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"state".GetProperDbObjectName()}"" 
        (""{"jobid".GetProperDbObjectName()}"", ""{"name".GetProperDbObjectName()}"", ""{"reason".GetProperDbObjectName()}"", ""{"createdat".GetProperDbObjectName()}"", ""{"data".GetProperDbObjectName()}"")        
        VALUES (@JobId, @Name, @Reason, @CreatedAt, @Data);
      ";

      QueueCommand(con => con.Execute(addStateSql,
        new {
          JobId = Convert.ToInt64(jobId, CultureInfo.InvariantCulture),
          state.Name,
          state.Reason,
          CreatedAt = DateTime.UtcNow,
          Data = SerializationHelper.Serialize(state.SerializeData()),
        }));
    }

    public override void AddToQueue(string queue, string jobId)
    {
      IPersistentJobQueueProvider provider = _storage.QueueProviders.GetProvider(queue);
      IPersistentJobQueue persistentQueue = provider.GetJobQueue();

      QueueCommand(con => persistentQueue.Enqueue(con, queue, jobId));
    }

    public override void IncrementCounter(string key)
    {
      string sql = $@"
        INSERT INTO ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"counter".GetProperDbObjectName()}"" 
        (""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"") 
        VALUES (@Key, @Value);
      ";
      QueueCommand(con => con.Execute(sql,
        new { Key = key, Value = +1 }));
    }

    public override void IncrementCounter(string key, TimeSpan expireIn)
    {
      string sql = $@"
        INSERT INTO ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"counter".GetProperDbObjectName()}""
        (""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"", ""{"expireat".GetProperDbObjectName()}"") 
        VALUES (@Key, @Value, NOW() AT TIME ZONE 'UTC' + INTERVAL '{(long)expireIn.TotalSeconds} SECONDS');
      ";
      QueueCommand(con => con.Execute(sql,
        new { Key = key, Value = +1 }));
    }

    public override void DecrementCounter(string key)
    {
      string sql = $@"
        INSERT INTO ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"counter".GetProperDbObjectName()}"" 
        (""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"") 
        VALUES (@Key, @Value);
      ";
      QueueCommand(con => con.Execute(sql,
        new { Key = key, Value = -1 }));
    }

    public override void DecrementCounter(string key, TimeSpan expireIn)
    {
      string sql = $@"
        INSERT INTO ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"counter".GetProperDbObjectName()}""
        (""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"", ""{"expireat".GetProperDbObjectName()}"") 
        VALUES (@Key, @Value, NOW() AT TIME ZONE 'UTC' + INTERVAL '{((long)expireIn.TotalSeconds).ToString(CultureInfo.InvariantCulture)} SECONDS');
      ";
      QueueCommand(con => con.Execute(sql, new { Key = key, Value = -1 }));
    }

    public override void AddToSet(string key, string value)
    {
      AddToSet(key, value, 0.0);
    }

    public override void AddToSet(string key, string value, double score)
    {
      string addSql = $@"
        INSERT INTO ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"set".GetProperDbObjectName()}""
        (""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"", ""{"score".GetProperDbObjectName()}"")
        VALUES(@Key, @Value, @Score)
        ON CONFLICT (""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"")
        DO UPDATE SET ""{"score".GetProperDbObjectName()}"" = EXCLUDED.""{"score".GetProperDbObjectName()}""
      ";
      QueueCommand(con => con.Execute(addSql,
        new { Key = key, Value = value, Score = score }));
    }

    public override void RemoveFromSet(string key, string value)
    {
      QueueCommand(con => con.Execute($@"
        DELETE FROM ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"set".GetProperDbObjectName()}"" 
        WHERE ""{"key".GetProperDbObjectName()}"" = @Key 
        AND ""{"value".GetProperDbObjectName()}"" = @Value;
      ",
      new { Key = key, Value = value }));
    }

    public override void InsertToList(string key, string value)
    {
      QueueCommand(con => con.Execute($@"
        INSERT INTO ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"list".GetProperDbObjectName()}""
        (""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"") 
        VALUES (@Key, @Value);
      ",
      new { Key = key, Value = value }));
    }

    public override void RemoveFromList(string key, string value)
    {
      QueueCommand(con => con.Execute($@"
        DELETE FROM ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"list".GetProperDbObjectName()}"" 
        WHERE ""{"key".GetProperDbObjectName()}"" = @Key 
        AND ""{"value".GetProperDbObjectName()}"" = @Value;
      ", new { Key = key, Value = value }));
    }

    public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
    {
      string trimSql = $@"
        DELETE FROM ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"list".GetProperDbObjectName()}"" AS source
        WHERE ""{"key".GetProperDbObjectName()}"" = @Key
        AND ""{"id".GetProperDbObjectName()}"" NOT IN (
            SELECT ""{"id".GetProperDbObjectName()}"" 
            FROM ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"list".GetProperDbObjectName()}"" AS keep
            WHERE keep.""{"key".GetProperDbObjectName()}"" = source.""{"key".GetProperDbObjectName()}""
            ORDER BY ""{"id".GetProperDbObjectName()}"" 
            OFFSET @Offset LIMIT @Limit
        );
      ";

      QueueCommand(con => con.Execute(trimSql,
        new { Key = key, Offset = keepStartingFrom, Limit = keepEndingAt - keepStartingFrom + 1 }));
    }

    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
      if (key == null) throw new ArgumentNullException("key");
      if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

      string sql = $@"
        WITH ""inputvalues"" AS (
	        SELECT @Key ""{"key".GetProperDbObjectName()}"", @Field ""{"field".GetProperDbObjectName()}"", @Value ""{"value".GetProperDbObjectName()}""
        ), ""updatedrows"" AS ( 
	        UPDATE ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"hash".GetProperDbObjectName()}"" ""updatetarget""
	        SET ""{"value".GetProperDbObjectName()}"" = ""inputvalues"".""{"value".GetProperDbObjectName()}""
	        FROM ""inputvalues""
	        WHERE ""updatetarget"".""{"key".GetProperDbObjectName()}"" = ""inputvalues"".""{"key".GetProperDbObjectName()}""
	        AND ""updatetarget"".""{"field".GetProperDbObjectName()}"" = ""inputvalues"".""{"field".GetProperDbObjectName()}""
	        RETURNING ""updatetarget"".""{"key".GetProperDbObjectName()}"", ""updatetarget"".""{"field".GetProperDbObjectName()}""
        )
        INSERT INTO ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"hash".GetProperDbObjectName()}""
        (""{"key".GetProperDbObjectName()}"", ""{"field".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"")
        SELECT ""{"key".GetProperDbObjectName()}"", ""{"field".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}""
        FROM ""inputvalues"" ""insertvalues""
        WHERE NOT EXISTS (
	        SELECT 1 
	        FROM ""updatedrows"" 
	        WHERE ""updatedrows"".""{"key".GetProperDbObjectName()}"" = ""insertvalues"".""{"key".GetProperDbObjectName()}"" 
	        AND ""updatedrows"".""{"field".GetProperDbObjectName()}"" = ""insertvalues"".""{"field".GetProperDbObjectName()}""
        );
      ";
      foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
      {
        KeyValuePair<string, string> pair = keyValuePair;
        QueueCommand(con => con.Execute(sql, new { Key = key, Field = pair.Key, pair.Value }));
      }
    }

    public override void RemoveHash(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string sql = $@"DELETE FROM ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"hash".GetProperDbObjectName()}"" 
                      WHERE ""{"key".GetProperDbObjectName()}"" = @Key";
      QueueCommand(con => con.Execute(sql,
        new { Key = key }));
    }

    public override void ExpireSet(string key, TimeSpan expireIn)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string sql = $@"UPDATE ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"set".GetProperDbObjectName()}"" 
                      SET ""{"expireat".GetProperDbObjectName()}"" = @ExpireAt WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      QueueCommand(connection => connection.Execute(sql,
        new { Key = key, ExpireAt = DateTime.UtcNow.Add(expireIn) }));
    }

    public override void ExpireList(string key, TimeSpan expireIn)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string sql = $@"UPDATE ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"list".GetProperDbObjectName()}"" 
                     SET ""{"expireat".GetProperDbObjectName()}"" = @ExpireAt WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      QueueCommand(connection => connection.Execute(sql,
        new { Key = key, ExpireAt = DateTime.UtcNow.Add(expireIn) }));
    }

    public override void ExpireHash(string key, TimeSpan expireIn)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string sql = $@"UPDATE ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"hash".GetProperDbObjectName()}"" 
                      SET ""{"expireat".GetProperDbObjectName()}"" = @ExpireAt WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      QueueCommand(connection => connection.Execute(sql,
        new { Key = key, ExpireAt = DateTime.UtcNow.Add(expireIn) }));
    }

    public override void PersistSet(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string sql = $@"UPDATE ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"set".GetProperDbObjectName()}"" 
                      SET ""{"expireat".GetProperDbObjectName()}"" = null WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      QueueCommand(connection => connection.Execute(sql, new { Key = key }));
    }

    public override void PersistList(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string sql = $@"UPDATE ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"list".GetProperDbObjectName()}"" 
                      SET ""{"expireat".GetProperDbObjectName()}"" = null WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      QueueCommand(connection => connection.Execute(sql, new { Key = key }));
    }

    public override void PersistHash(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string sql = $@"UPDATE ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"hash".GetProperDbObjectName()}"" 
                      SET ""{"expireat".GetProperDbObjectName()}"" = null WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      QueueCommand(connection => connection.Execute(sql,
        new { Key = key }));
    }

    public override void AddRangeToSet(string key, IList<string> items)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));
      if (items == null) throw new ArgumentNullException(nameof(items));

      string sql = $@"INSERT INTO ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"set".GetProperDbObjectName()}"" 
                    (""{"key".GetProperDbObjectName()}"", ""{"value".GetProperDbObjectName()}"", ""{"score".GetProperDbObjectName()}"") 
                    VALUES (@Key, @Value, 0.0)";

      QueueCommand(connection => connection.Execute(sql,
        items.Select(value => new { Key = key, Value = value }).ToList()));
    }

    public override void RemoveSet(string key)
    {
      if (key == null) throw new ArgumentNullException(nameof(key));

      string sql = $@"DELETE FROM ""{_storage.Options.SchemaName.GetProperDbObjectName()}"".""{"set".GetProperDbObjectName()}"" 
                      WHERE ""{"key".GetProperDbObjectName()}"" = @Key";

      QueueCommand(connection => connection.Execute(sql, new { Key = key }));
    }

    internal void QueueCommand(Action<IDbConnection> action)
    {
      _commandQueue.Enqueue(action);
    }
  }
}
