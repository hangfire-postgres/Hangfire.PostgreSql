﻿// This file is part of Hangfire.PostgreSql.
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
using Hangfire.PostgreSql.Utils;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
  public class PostgreSqlWriteOnlyTransaction : JobStorageTransaction
  {
    private readonly Queue<Action<IDbConnection>> _commandQueue = new();
    private readonly Func<DbConnection> _dedicatedConnectionFunc;
    private readonly List<string> _queuesWithAddedJobs = [];

    private readonly PostgreSqlStorage _storage;

    public PostgreSqlWriteOnlyTransaction(PostgreSqlStorage storage, Func<DbConnection> dedicatedConnectionFunc)
    {
      _storage = storage ?? throw new ArgumentNullException(nameof(storage));
      _dedicatedConnectionFunc = dedicatedConnectionFunc ?? throw new ArgumentNullException(nameof(dedicatedConnectionFunc));
    }

    public override void Commit()
    {
      _storage.UseTransaction(_dedicatedConnectionFunc(), (connection, _) => {
        RegisterNewJobsEventWithTransactionCompletedEvent();
        foreach (Action<IDbConnection> command in _commandQueue)
        {
          command(connection);
        }
      }, CreateTransactionScope);
    }

    private void RegisterNewJobsEventWithTransactionCompletedEvent()
    {
      Transaction.Current.TransactionCompleted += (_, args) => {
        if (args.Transaction.TransactionInformation.Status == TransactionStatus.Committed)
        {
          _queuesWithAddedJobs.ForEach(PostgreSqlJobQueue._queueEventRegistry.Set);
          _queuesWithAddedJobs.Clear();
        }
      };
    }

    private TransactionScope CreateTransactionScope()
    {
      return _storage.CreateTransactionScope(null, TransactionManager.MaximumTimeout);
    }

    public override void ExpireJob(string jobId, TimeSpan expireIn)
    {
      string query = SqlQueryProvider.Instance.GetQuery(schemaName =>
        $"""
        UPDATE {schemaName}.job
        SET expireat = NOW() + INTERVAL '{(long)expireIn.TotalSeconds} SECONDS'
        WHERE id = @Id
        """);

      QueueCommand(con => con.Execute(query, new { Id = jobId.ParseJobId() }));
    }

    public override void PersistJob(string jobId)
    {
      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"""
        UPDATE {schemaName}.job 
        SET expireat = NULL 
        WHERE id = @Id
        """);

      QueueCommand(con => con.Execute(query, new { Id = jobId.ParseJobId() }));
    }

    public override void SetJobState(string jobId, IState state)
    {
      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"""
        WITH s AS (
            INSERT INTO {schemaName}.state (jobid, name, reason, createdat, data)
            VALUES (@JobId, @Name, @Reason, @CreatedAt, @Data) RETURNING id
        )
        UPDATE {schemaName}.job j
        SET stateid = s.id, statename = @Name
        FROM s
        WHERE j.id = @JobId
        """);

      QueueCommand(con => con.Execute(query,
        new {
          JobId = jobId.ParseJobId(),
          state.Name,
          state.Reason,
          CreatedAt = DateTime.UtcNow,
          Data = new JsonParameter(SerializationHelper.Serialize(state.SerializeData())),
        }));
    }

    public override void AddJobState(string jobId, IState state)
    {
      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"""
        INSERT INTO {schemaName}.state (jobid, name, reason, createdat, data)
        VALUES (@JobId, @Name, @Reason, @CreatedAt, @Data)
        """);

      QueueCommand(con => con.Execute(query,
        new {
          JobId = jobId.ParseJobId(),
          state.Name,
          state.Reason,
          CreatedAt = DateTime.UtcNow,
          Data = new JsonParameter(SerializationHelper.Serialize(state.SerializeData())),
        }));
    }

    public override void AddToQueue(string queue, string jobId)
    {
      IPersistentJobQueueProvider provider = _storage.QueueProviders.GetProvider(queue);
      IPersistentJobQueue persistentQueue = provider.GetJobQueue();

      QueueCommand(con => persistentQueue.Enqueue(con, queue, jobId));
      
      _queuesWithAddedJobs.Add(queue);
    }

    public override void IncrementCounter(string key)
    {
      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"""
        INSERT INTO {schemaName}.counter (key, value) 
        VALUES (@Key, @Value)
        """);

      QueueCommand(con => con.Execute(query,
        new { Key = key, Value = +1 }));
    }

    public override void IncrementCounter(string key, TimeSpan expireIn)
    {
      string query = SqlQueryProvider.Instance.GetQuery(schemaName =>
        $"""
        INSERT INTO {schemaName}.counter(key, value, expireat) 
        VALUES (@Key, @Value, NOW() + INTERVAL '{(long)expireIn.TotalSeconds} SECONDS')
        """);

      QueueCommand(con => con.Execute(query,
        new { Key = key, Value = +1 }));
    }

    public override void DecrementCounter(string key)
    {
      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"""
        INSERT INTO {schemaName}.counter (key, value) 
        VALUES (@Key, @Value)
        """);

      QueueCommand(con => con.Execute(query, new { Key = key, Value = -1 }));
    }

    public override void DecrementCounter(string key, TimeSpan expireIn)
    {
      string query = SqlQueryProvider.Instance.GetQuery(schemaName =>
        $"""
        INSERT INTO {schemaName}.counter(key, value, expireat) 
        VALUES (@Key, @Value, NOW() + INTERVAL '{((long)expireIn.TotalSeconds).ToString(CultureInfo.InvariantCulture)} SECONDS')
        """);

      QueueCommand(con => con.Execute(query, new { Key = key, Value = -1 }));
    }

    public override void AddToSet(string key, string value)
    {
      AddToSet(key, value, 0.0);
    }

    public override void AddToSet(string key, string value, double score)
    {
      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"""
        INSERT INTO {schemaName}.set(key, value, score)
        VALUES(@Key, @Value, @Score)
        ON CONFLICT (key, value)
        DO UPDATE SET score = EXCLUDED.score
        """);

      QueueCommand(con => con.Execute(query, new { Key = key, Value = value, Score = score }));
    }

    public override void RemoveFromSet(string key, string value)
    {
      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"""
        DELETE FROM {schemaName}.set 
        WHERE key = @Key 
        AND value = @Value
        """);

      QueueCommand(con => con.Execute(query, new { Key = key, Value = value }));
    }

    public override void InsertToList(string key, string value)
    {
      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"""
        INSERT INTO {schemaName}.list (key, value) 
        VALUES (@Key, @Value)
        """);

      QueueCommand(con => con.Execute(query,
        new { Key = key, Value = value }));
    }

    public override void RemoveFromList(string key, string value)
    {
      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"""
        DELETE FROM {schemaName}.list 
        WHERE key = @Key 
        AND value = @Value
        """);

      QueueCommand(con => con.Execute(query,
        new { Key = key, Value = value }));
    }

    public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
    {
      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"""
        DELETE FROM {schemaName}.list AS source
        WHERE key = @Key
        AND id NOT IN (
            SELECT id 
            FROM {schemaName}.list AS keep
            WHERE keep.key = source.key
            ORDER BY id 
            OFFSET @Offset LIMIT @Limit
        )
        """);

      QueueCommand(con => con.Execute(query,
        new { Key = key, Offset = keepStartingFrom, Limit = keepEndingAt - keepStartingFrom + 1 }));
    }

    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
      if (key == null)
      {
        throw new ArgumentNullException(nameof(key));
      }

      if (keyValuePairs == null)
      {
        throw new ArgumentNullException(nameof(keyValuePairs));
      }

      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"""
        WITH inputvalues AS (
          SELECT @Key key, @Field field, @Value value
        ), updatedrows AS ( 
          UPDATE {schemaName}.hash updatetarget
          SET value = inputvalues.value
          FROM inputvalues
          WHERE updatetarget.key = inputvalues.key
          AND updatetarget.field = inputvalues.field
          RETURNING updatetarget.key, updatetarget.field
        )
        INSERT INTO {schemaName}.hash(key, field, value)
        SELECT key, field, value 
        FROM inputvalues insertvalues
        WHERE NOT EXISTS (
          SELECT 1 
          FROM updatedrows 
          WHERE updatedrows.key = insertvalues.key 
          AND updatedrows.field = insertvalues.field
        )
        """);

      foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
      {
        KeyValuePair<string, string> pair = keyValuePair;
        QueueCommand(con => con.Execute(query, new { Key = key, Field = pair.Key, pair.Value }));
      }
    }

    public override void RemoveHash(string key)
    {
      if (key == null)
      {
        throw new ArgumentNullException(nameof(key));
      }

      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"DELETE FROM {schemaName}.hash WHERE key = @Key");

      QueueCommand(con => con.Execute(query,
        new { Key = key }));
    }

    public override void ExpireSet(string key, TimeSpan expireIn)
    {
      if (key == null)
      {
        throw new ArgumentNullException(nameof(key));
      }

      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"UPDATE {schemaName}.set SET expireat = @ExpireAt WHERE key = @Key");

      QueueCommand(connection => connection.Execute(query,
        new { Key = key, ExpireAt = DateTime.UtcNow.Add(expireIn) }));
    }

    public override void ExpireList(string key, TimeSpan expireIn)
    {
      if (key == null)
      {
        throw new ArgumentNullException(nameof(key));
      }

      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"UPDATE {schemaName}.list SET expireat = @ExpireAt WHERE key = @Key");

      QueueCommand(connection => connection.Execute(query,
        new { Key = key, ExpireAt = DateTime.UtcNow.Add(expireIn) }));
    }

    public override void ExpireHash(string key, TimeSpan expireIn)
    {
      if (key == null)
      {
        throw new ArgumentNullException(nameof(key));
      }

      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"UPDATE {schemaName}.hash SET expireat = @ExpireAt WHERE key = @Key");

      QueueCommand(connection => connection.Execute(query,
        new { Key = key, ExpireAt = DateTime.UtcNow.Add(expireIn) }));
    }

    public override void PersistSet(string key)
    {
      if (key == null)
      {
        throw new ArgumentNullException(nameof(key));
      }

      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"UPDATE {schemaName}.set SET expireat = null WHERE key = @Key");

      QueueCommand(connection => connection.Execute(query, new { Key = key }));
    }

    public override void PersistList(string key)
    {
      if (key == null)
      {
        throw new ArgumentNullException(nameof(key));
      }

      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"UPDATE {schemaName}.list SET expireat = null WHERE key = @Key");

      QueueCommand(connection => connection.Execute(query, new { Key = key }));
    }

    public override void PersistHash(string key)
    {
      if (key == null)
      {
        throw new ArgumentNullException(nameof(key));
      }

      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"UPDATE {schemaName}.hash SET expireat = null WHERE key = @Key");

      QueueCommand(connection => connection.Execute(query,
        new { Key = key }));
    }

    public override void AddRangeToSet(string key, IList<string> items)
    {
      if (key == null)
      {
        throw new ArgumentNullException(nameof(key));
      }

      if (items == null)
      {
        throw new ArgumentNullException(nameof(items));
      }

      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"INSERT INTO {schemaName}.set (key, value, score) VALUES (@Key, @Value, 0.0)");

      QueueCommand(connection => connection.Execute(query,
        items.Select(value => new { Key = key, Value = value }).ToList()));
    }

    public override void RemoveSet(string key)
    {
      if (key == null)
      {
        throw new ArgumentNullException(nameof(key));
      }

      string query = SqlQueryProvider.Instance.GetQuery(static schemaName =>
        $"DELETE FROM {schemaName}.set WHERE key = @Key");

      QueueCommand(connection => connection.Execute(query, new { Key = key }));
    }

    internal void QueueCommand(Action<IDbConnection> action)
    {
      _commandQueue.Enqueue(action);
    }
  }
}
