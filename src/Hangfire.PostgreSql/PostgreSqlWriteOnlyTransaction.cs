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
using System.Data.Common;
using System.Transactions;
using Hangfire.Common;
using Hangfire.PostgreSql.Utils;
using Hangfire.States;
using Hangfire.Storage;
using NpgsqlTypes;

namespace Hangfire.PostgreSql;

public class PostgreSqlWriteOnlyTransaction : JobStorageTransaction
{
  private readonly PostgreSqlStorageContext _context;
  private readonly Func<DbConnection?> _dedicatedConnectionFunc;
  private readonly Queue<Action<IDbConnection>> _commandQueue = new();
  private readonly List<string> _queuesWithAddedJobs = [];

  internal PostgreSqlWriteOnlyTransaction(PostgreSqlStorageContext context, Func<DbConnection?> dedicatedConnectionFunc)
  {
    _context = context ?? throw new ArgumentNullException(nameof(context));
    _dedicatedConnectionFunc = dedicatedConnectionFunc ?? throw new ArgumentNullException(nameof(dedicatedConnectionFunc));
  }

  public override void Commit()
  {
    _context.ConnectionManager.UseTransaction(_dedicatedConnectionFunc(), (connection, _) => {
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
    return _context.ConnectionManager.CreateTransactionScope(null, TransactionManager.MaximumTimeout);
  }

  public override void ExpireJob(string jobId, TimeSpan expireIn)
  {
    string query = _context.QueryProvider.GetQuery("UPDATE hangfire.job SET expireat = NOW() + $2 WHERE id = $1");
    QueueCommand(con => con.Process(query).WithParameters(jobId.ParseJobId(), expireIn).Execute());
  }

  public override void PersistJob(string jobId)
  {
    string query = _context.QueryProvider.GetQuery("UPDATE hangfire.job SET expireat = NULL WHERE id = $1");
    QueueCommand(con => con.Process(query).WithParameter(jobId.ParseJobId()).Execute());
  }

  public override void SetJobState(string jobId, IState state)
  {
    string query = _context.QueryProvider.GetQuery(
      // language=sql
      """
      WITH s AS (
        INSERT INTO hangfire.state (jobid, name, reason, createdat, data)
        VALUES ($1, $2, $3, $4, $5) RETURNING id
      )
      UPDATE hangfire.job j
      SET stateid = s.id, statename = $2
      FROM s
      WHERE j.id = $1
      """);

    QueueCommand(con => con.Process(query)
      .WithParameters(jobId.ParseJobId(), state.Name, state.Reason, DateTime.UtcNow)
      .WithParameter(SerializationHelper.Serialize(state.SerializeData()), NpgsqlDbType.Jsonb)
      .Execute());
  }

  public override void AddJobState(string jobId, IState state)
  {
    string query = _context.QueryProvider.GetQuery(
      "INSERT INTO hangfire.state (jobid, name, reason, createdat, data) VALUES ($1, $2, $3, $4, $5)");
    QueueCommand(con => con.Process(query)
      .WithParameters(jobId.ParseJobId(), state.Name, state.Reason, DateTime.UtcNow)
      .WithParameter(SerializationHelper.Serialize(state.SerializeData()), NpgsqlDbType.Jsonb)
      .Execute());
  }

  public override void AddToQueue(string queue, string jobId)
  {
    IPersistentJobQueueProvider provider = _context.QueueProviders.GetProvider(queue);
    IPersistentJobQueue persistentQueue = provider.GetJobQueue();

    QueueCommand(con => persistentQueue.Enqueue(con, queue, jobId));
      
    _queuesWithAddedJobs.Add(queue);
  }

  public override void IncrementCounter(string key)
  {
    ProcessCounter(key, CounterOperation.Increment, null);
  }

  public override void IncrementCounter(string key, TimeSpan expireIn)
  {
    ProcessCounter(key, CounterOperation.Increment, expireIn);
  }

  public override void DecrementCounter(string key)
  {
    ProcessCounter(key, CounterOperation.Decrement, null);
  }

  public override void DecrementCounter(string key, TimeSpan expireIn)
  {
    ProcessCounter(key, CounterOperation.Decrement, expireIn);
  }

  private enum CounterOperation
  {
    Increment,
    Decrement,
  }

  private void ProcessCounter(string key, CounterOperation operation, TimeSpan? expireIn)
  {
    string query = expireIn.HasValue
      ? _context.QueryProvider.GetQuery("INSERT INTO hangfire.counter(key, value, expireat) VALUES ($1, $2, NOW() + $3)")
      : _context.QueryProvider.GetQuery("INSERT INTO hangfire.counter(key, value) VALUES ($1, $2)");
    long value = operation switch {
      CounterOperation.Increment => 1,
      CounterOperation.Decrement => -1,
      var _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, null),
    };
    QueueCommand(con => {
      SqlQueryProcessor processor = con.Process(query).WithParameters(key, value);
      if (expireIn.HasValue)
      {
        processor.WithParameter(expireIn.Value);
      }
      processor.Execute();
    });
  }

  public override void AddToSet(string key, string value)
  {
    AddToSet(key, value, 0.0);
  }

  public override void AddToSet(string key, string value, double score)
  {
    string query = _context.QueryProvider.GetQuery(
      """
      INSERT INTO hangfire.set(key, value, score)
      VALUES($1, $2, $3)
      ON CONFLICT (key, value) DO UPDATE SET score = EXCLUDED.score
      """);
    QueueCommand(con => con.Process(query).WithParameters(key, value, score).Execute());
  }

  public override void RemoveFromSet(string key, string value)
  {
    string query = _context.QueryProvider.GetQuery("DELETE FROM hangfire.set WHERE key = $1 AND value = $2");
    QueueCommand(con => con.Process(query).WithParameters(key, value).Execute());
  }

  public override void InsertToList(string key, string value)
  {
    string query = _context.QueryProvider.GetQuery("INSERT INTO hangfire.list (key, value) VALUES ($1, $2)");
    QueueCommand(con => con.Process(query).WithParameters(key, value).Execute());
  }

  public override void RemoveFromList(string key, string value)
  {
    string query = _context.QueryProvider.GetQuery("DELETE FROM hangfire.list WHERE key = $1 AND value = $2");
    QueueCommand(con => con.Process(query).WithParameters(key, value).Execute());
  }

  public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
  {
    string query = _context.QueryProvider.GetQuery(
      """
      DELETE FROM hangfire.list AS source
      WHERE key = $1
      AND id NOT IN (
          SELECT id 
          FROM hangfire.list AS keep
          WHERE keep.key = source.key
          ORDER BY id 
          LIMIT $2 OFFSET $3
      )
      """);
    QueueCommand(con => con.Process(query).WithParameters(key, keepEndingAt - keepStartingFrom + 1, keepStartingFrom).Execute());
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

    string query = _context.QueryProvider.GetQuery(
      // language=sql
      """
      WITH inputvalues AS (
        SELECT $1 AS key, $2 AS field, $3 AS value
      ), updatedrows AS (
        UPDATE hangfire.hash updatetarget
        SET value = inputvalues.value
        FROM inputvalues
        WHERE updatetarget.key = inputvalues.key AND updatetarget.field = inputvalues.field
        RETURNING updatetarget.key, updatetarget.field
      )
      INSERT INTO hangfire.hash(key, field, value)
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
      QueueCommand(con => con.Process(query).WithParameters(key, pair.Key, pair.Value).Execute());
    }
  }

  public override void RemoveHash(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("DELETE FROM hangfire.hash WHERE key = $1");
    QueueCommand(con => con.Process(query).WithParameter(key).Execute());
  }

  public override void ExpireSet(string key, TimeSpan expireIn)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("UPDATE hangfire.set SET expireat = $2 WHERE key = $1");
    QueueCommand(con => con.Process(query).WithParameters(key, DateTime.UtcNow.Add(expireIn)).Execute());
  }

  public override void ExpireList(string key, TimeSpan expireIn)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("UPDATE hangfire.list SET expireat = $2 WHERE key = $1");
    QueueCommand(con => con.Process(query).WithParameters(key, DateTime.UtcNow.Add(expireIn)).Execute());
  }

  public override void ExpireHash(string key, TimeSpan expireIn)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("UPDATE hangfire.hash SET expireat = $2 WHERE key = $1");
    QueueCommand(con => con.Process(query).WithParameters(key, DateTime.UtcNow.Add(expireIn)).Execute());
  }

  public override void PersistSet(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("UPDATE hangfire.set SET expireat = null WHERE key = $1");
    QueueCommand(con => con.Process(query).WithParameter(key).Execute());
  }

  public override void PersistList(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("UPDATE hangfire.list SET expireat = null WHERE key = $1");
    QueueCommand(con => con.Process(query).WithParameter(key).Execute());
  }

  public override void PersistHash(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("UPDATE hangfire.hash SET expireat = null WHERE key = $1");
    QueueCommand(con => con.Process(query).WithParameter(key).Execute());
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

    string query = _context.QueryProvider.GetQuery("INSERT INTO hangfire.set (key, value, score) VALUES ($1, $2, 0.0)");
    foreach (string item in items)
    {
      QueueCommand(con => con.Process(query).WithParameters(key, item).Execute());
    }
  }

  public override void RemoveSet(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("DELETE FROM hangfire.set WHERE key = $1");
    QueueCommand(con => con.Process(query).WithParameter(key).Execute());
  }

  internal void QueueCommand(Action<IDbConnection> accessor)
  {
    _commandQueue.Enqueue(accessor);
  }
}