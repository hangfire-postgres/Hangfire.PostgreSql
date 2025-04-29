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
using System.Diagnostics;
using System.Globalization;
using Hangfire.Common;
using Hangfire.PostgreSql.Entities;
using Hangfire.PostgreSql.Utils;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;
using NpgsqlTypes;
using IsolationLevel = System.Transactions.IsolationLevel;

namespace Hangfire.PostgreSql;

public class PostgreSqlConnection : JobStorageConnection
{
  private readonly PostgreSqlStorageContext _context;
  private readonly Dictionary<string, HashSet<Guid>> _lockedResources;

  private DbConnection? _dedicatedConnection;

  internal PostgreSqlConnection(PostgreSqlStorageContext context)
  {
    _context = context ?? throw new ArgumentNullException(nameof(context));
    _lockedResources = new Dictionary<string, HashSet<Guid>>();
  }

  public override void Dispose()
  {
    if (_dedicatedConnection == null)
    {
      return;
    }

    _dedicatedConnection.Dispose();
    _dedicatedConnection = null;
  }

  public override IWriteOnlyTransaction CreateWriteTransaction()
  {
    return new PostgreSqlWriteOnlyTransaction(_context, () => _dedicatedConnection);
  }

  public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
  {
    return string.IsNullOrEmpty(resource)
      ? throw new ArgumentNullException(nameof(resource))
      : AcquireLock($"{_context.Options.SchemaName}:{resource}", timeout);
  }

  public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
  {
    if (queues == null || queues.Length == 0)
    {
      throw new ArgumentNullException(nameof(queues));
    }

    IPersistentJobQueueProvider[] providers = queues
      .Select(_context.QueueProviders.GetProvider)
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
    if (job == null)
    {
      throw new ArgumentNullException(nameof(job));
    }

    if (parameters == null)
    {
      throw new ArgumentNullException(nameof(parameters));
    }

    InvocationData invocationData = InvocationData.SerializeJob(job);

    string query = _context.QueryProvider.GetQuery(
      "INSERT INTO hangfire.job (invocationdata, arguments, createdat, expireat) VALUES ($1, $2, $3, $4) RETURNING id");
    return _context.ConnectionManager.UseTransaction(_dedicatedConnection, (connection, transaction) => {
      long jobId = connection.Process(query)
        .WithParameter(SerializationHelper.Serialize(invocationData), NpgsqlDbType.Jsonb)
        .WithParameter(invocationData.Arguments, NpgsqlDbType.Jsonb)
        .WithParameters(createdAt, createdAt.Add(expireIn))
        .Select(reader => reader.GetInt64(0))
        .Single();
      if (parameters.Count > 0)
      {
        string insertQuery = _context.QueryProvider.GetQuery("INSERT INTO hangfire.jobparameter (jobid, name, value) VALUES ($1, $2, $3)");
        foreach (KeyValuePair<string, string> parameter in parameters)
        {
          connection.Process(insertQuery, transaction).WithParameters(jobId, parameter.Key, parameter.Value).Execute();
        }
      }

      return jobId.ToString(CultureInfo.InvariantCulture);
    });
  }

  public override JobData? GetJobData(string id)
  {
    if (id == null)
    {
      throw new ArgumentNullException(nameof(id));
    }

    string query = _context.QueryProvider.GetQuery("SELECT statename, createdat, invocationdata, arguments FROM hangfire.job WHERE id = $1");
    return _context.ConnectionManager.UseConnection(_dedicatedConnection,
      connection => connection.Process(query)
        .WithParameter(id.ParseJobId())
        .Select(reader => {
          string state = reader.GetString(0);
          DateTime createdAt = reader.GetDateTime(1);
          InvocationData invocationData = SerializationHelper.Deserialize<InvocationData>(reader.GetString(2));
          invocationData.Arguments = reader.GetString(3);
          return new JobData {
            Job = TryLoadJob(invocationData, out JobLoadException? exception),
            State = state,
            CreatedAt = createdAt,
            LoadException = exception,
          };
        })
        .SingleOrDefault());

    Job? TryLoadJob(InvocationData invocationData, out JobLoadException? exception)
    {
      exception = null;
      try
      {
        return invocationData.DeserializeJob();
      }
      catch (JobLoadException ex)
      {
        exception = ex;
        return null;
      }
    }
  }

  public override StateData? GetStateData(string jobId)
  {
    if (jobId == null)
    {
      throw new ArgumentNullException(nameof(jobId));
    }

    string query = _context.QueryProvider.GetQuery(
      """
      SELECT s.name, s.reason, s.data
      FROM hangfire.state s
      INNER JOIN hangfire.job j ON j.stateid = s.id
      WHERE j.id = $1
      """);
    return _context.ConnectionManager.UseConnection(_dedicatedConnection,
      connection => connection
        .Process(query)
        .WithParameter(jobId.ParseJobId())
        .Select(reader => new StateData {
          Name = reader.GetString(0),
          Reason = reader.GetString(1),
          Data = SerializationHelper.Deserialize<Dictionary<string, string>>(reader.GetString(2)),
        })
        .SingleOrDefault());
  }

  public override void SetJobParameter(string id, string name, string value)
  {
    if (id == null)
    {
      throw new ArgumentNullException(nameof(id));
    }

    if (name == null)
    {
      throw new ArgumentNullException(nameof(name));
    }

    string query = _context.QueryProvider.GetQuery(
      // language=sql
      """
      WITH inputvalues AS (
        SELECT $1 AS jobid, $2 AS name, $3 AS value
      ), updatedrows AS ( 
        UPDATE hangfire.jobparameter AS updatetarget
        SET value = inputvalues.value
        FROM inputvalues
        WHERE updatetarget.jobid = inputvalues.jobid AND updatetarget.name = inputvalues.name
        RETURNING updatetarget.jobid, updatetarget.name
      )
      INSERT INTO hangfire.jobparameter (jobid, name, value)
      SELECT jobid, name, value 
      FROM inputvalues
      WHERE NOT EXISTS (
        SELECT 1 
        FROM updatedrows 
        WHERE updatedrows.jobid = inputvalues.jobid AND updatedrows.name = inputvalues.name
      )
      """);
    _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameters(id.ParseJobId(), name, value)
      .Execute());
  }

  public override string? GetJobParameter(string id, string name)
  {
    if (id == null)
    {
      throw new ArgumentNullException(nameof(id));
    }

    if (name == null)
    {
      throw new ArgumentNullException(nameof(name));
    }

    string query = _context.QueryProvider.GetQuery("SELECT value FROM hangfire.jobparameter WHERE jobid = $1 AND name = $2");
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection =>
      connection.Process(query)
        .WithParameters(id.ParseJobId(), name)
        .Select(reader => reader.GetString(0))
        .SingleOrDefault());
  }

  public override HashSet<string> GetAllItemsFromSet(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("SELECT value FROM hangfire.set WHERE key = $1");
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection =>
      new HashSet<string>(connection
        .Process(query)
        .WithParameter(key)
        .Select(reader => reader.GetString(0))));
  }

  public override string? GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    if (toScore < fromScore)
    {
      throw new ArgumentException($"The '{nameof(toScore)}' value must be higher or equal to the '{nameof(fromScore)}' value.");
    }

    string query = _context.QueryProvider.GetQuery(
      """
      SELECT value
      FROM hangfire.set
      WHERE key = $1 AND score BETWEEN $2 AND $3
      ORDER BY score LIMIT 1
      """);
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameters(key, fromScore, toScore)
      .Select(reader => reader.GetString(0))
      .SingleOrDefault());
  }

  public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    if (toScore < fromScore)
    {
      throw new ArgumentException($"The '{nameof(toScore)}' value must be higher or equal to the '{nameof(fromScore)}' value.");
    }

    if (count < 1)
    {
      throw new ArgumentException($"The '{nameof(count)}' value must be greater than zero (0).");
    }

    string query = _context.QueryProvider.GetQuery(
      """
      SELECT value
      FROM hangfire.set
      WHERE key = $1 AND score BETWEEN $2 AND $3
      ORDER BY score LIMIT $4
      """);
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameters(key, fromScore, toScore, count)
      .Select(reader => reader.GetString(0))
      .ToList());
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
      SELECT key, field, value FROM inputvalues insertvalues
      WHERE NOT EXISTS (
        SELECT 1
        FROM updatedrows
        WHERE updatedrows.key = insertvalues.key AND updatedrows.field = insertvalues.field
      )
      """);

    Stopwatch executionTimer = Stopwatch.StartNew();
    while (true)
    {
      try
      {
        _context.ConnectionManager.UseTransaction(_dedicatedConnection, (connection, transaction) => {
          foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
          {
            connection.Process(query, transaction).WithParameters(key, keyValuePair.Key, keyValuePair.Value).Execute();
          }
        }, IsolationLevel.Serializable);

        return;
      }
      catch (PostgresException exception)
      {
        if (!exception.SqlState.Equals(PostgresErrorCodes.SerializationFailure)) // 40001
        {
          throw;
        }
      }

      if (executionTimer.Elapsed > _context.Options.TransactionSynchronisationTimeout)
      {
        throw new TimeoutException("SetRangeInHash experienced timeout while trying to execute transaction");
      }
    }
  }

  public override Dictionary<string, string>? GetAllEntriesFromHash(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("SELECT field, value FROM hangfire.hash WHERE key = $1");
    Dictionary<string, string> result = _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameter(key)
      .Select(reader => new { Field = reader.GetString(0), Value = reader.GetString(1) })
      .ToDictionary(x => x.Field, x => x.Value));
    return result.Count != 0 ? result : null;
  }

  public override void AnnounceServer(string serverId, ServerContext context)
  {
    if (serverId == null)
    {
      throw new ArgumentNullException(nameof(serverId));
    }

    if (context == null)
    {
      throw new ArgumentNullException(nameof(context));
    }

    ServerData data = new() {
      WorkerCount = context.WorkerCount,
      Queues = context.Queues,
      StartedAt = DateTime.UtcNow,
    };

    string query = _context.QueryProvider.GetQuery(
      // language=sql
      """
      WITH inputvalues AS (
        SELECT $1 AS id, $2 AS data, NOW() AS lastheartbeat
      ), updatedrows AS (
        UPDATE hangfire.server AS updatetarget
        SET data = inputvalues.data, lastheartbeat = inputvalues.lastheartbeat
        FROM inputvalues
        WHERE updatetarget.id = inputvalues.id
        RETURNING updatetarget.id
      )
      INSERT INTO hangfire.server(id, data, lastheartbeat)
      SELECT id, data, lastheartbeat
      FROM inputvalues AS insertvalues
      WHERE NOT EXISTS (
        SELECT 1
        FROM updatedrows 
        WHERE updatedrows.id = insertvalues.id 
      )
      """);
    _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameter(serverId)
      .WithParameter(data.Serialize(), NpgsqlDbType.Jsonb)
      .Execute());
  }

  public override void RemoveServer(string serverId)
  {
    if (serverId == null)
    {
      throw new ArgumentNullException(nameof(serverId));
    }

    string query = _context.QueryProvider.GetQuery("DELETE FROM hangfire.server WHERE id = $1");
    _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection.Process(query).WithParameter(serverId).Execute());
  }

  public override void Heartbeat(string serverId)
  {
    if (serverId == null)
    {
      throw new ArgumentNullException(nameof(serverId));
    }

    string query = _context.QueryProvider.GetQuery("UPDATE hangfire.server SET lastheartbeat = NOW() WHERE id = $1");
    int affectedRows = _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection.Process(query).WithParameter(serverId).Execute());
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

    string query = _context.QueryProvider.GetQuery("DELETE FROM hangfire.server WHERE hangfire.server.lastheartbeat < (NOW() - $1)");
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection.Process(query).WithParameter(timeOut).Execute());
  }

  public override long GetSetCount(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("SELECT COUNT(key) FROM hangfire.set WHERE key = $1");
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameter(key)
      .Select(reader => reader.GetInt64(0))
      .SingleOrDefault());
  }

  public override List<string> GetAllItemsFromList(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("SELECT value FROM hangfire.list WHERE key = $1 ORDER BY id DESC");
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameter(key)
      .Select(reader => reader.GetString(0))
      .ToList());
  }

  public override long GetCounter(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery(
      """
      SELECT SUM(value)
      FROM (
        SELECT SUM(value) AS value
        FROM hangfire.counter
        WHERE key = $1
        UNION ALL
        SELECT SUM(value) AS value
        FROM hangfire.aggregatedcounter
        WHERE key = $1
      ) c
      """);
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameter(key)
      .Select(reader => reader.IsDBNull(0) ? 0 : reader.GetInt64(0))
      .Single());
  }

  public override long GetListCount(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("SELECT COUNT(id) FROM hangfire.list WHERE key = $1");
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameter(key)
      .Select(reader => reader.GetInt64(0))
      .SingleOrDefault());
  }

  public override TimeSpan GetListTtl(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("SELECT min(expireat) FROM hangfire.list WHERE key = $1");
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameter(key)
      .Select(reader => reader.IsDBNull(0) ? TimeSpan.FromSeconds(-1) : reader.GetDateTime(0) - DateTime.UtcNow)
      .Single());
  }

  public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery(
      """
      SELECT value
      FROM hangfire.list
      WHERE key = $1
      ORDER BY id DESC
      LIMIT $2 OFFSET $3
      """);
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameters(key, endingAt - startingFrom + 1, startingFrom)
      .Select(reader => reader.GetString(0))
      .ToList());
  }

  public override long GetHashCount(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("SELECT COUNT(id) FROM hangfire.hash WHERE key = $1");
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameter(key)
      .Select(reader => reader.GetInt64(0))
      .Single());
  }

  public override TimeSpan GetHashTtl(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("SELECT MIN(expireat) FROM hangfire.hash WHERE key = $1");
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameter(key)
      .Select(reader => reader.IsDBNull(0) ? TimeSpan.FromSeconds(-1) : reader.GetDateTime(0) - DateTime.UtcNow)
      .Single());
  }

  public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery(
      """
      SELECT value
      FROM hangfire.set
      WHERE key = $1
      ORDER BY id
      LIMIT $2 OFFSET $3
      """);
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameters(key, endingAt - startingFrom + 1, startingFrom)
      .Select(reader => reader.GetString(0))
      .ToList());
  }

  public override TimeSpan GetSetTtl(string key)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    string query = _context.QueryProvider.GetQuery("SELECT min(expireat) FROM hangfire.set WHERE key = $1");
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameter(key)
      .Select(reader => reader.IsDBNull(0) ? TimeSpan.FromSeconds(-1) : reader.GetDateTime(0) - DateTime.UtcNow)
      .Single());
  }

  public override string? GetValueFromHash(string key, string name)
  {
    if (key == null)
    {
      throw new ArgumentNullException(nameof(key));
    }

    if (name == null)
    {
      throw new ArgumentNullException(nameof(name));
    }

    string query = _context.QueryProvider.GetQuery("SELECT value FROM hangfire.hash WHERE key = $1 AND field = $2");
    return _context.ConnectionManager.UseConnection(_dedicatedConnection, connection => connection
      .Process(query)
      .WithParameters(key, name)
      .Select(reader => reader.GetString(0))
      .SingleOrDefault());
  }

  private IDisposable AcquireLock(string resource, TimeSpan timeout)
  {
    _dedicatedConnection ??= _context.ConnectionManager.CreateAndOpenConnection();

    Guid lockId = Guid.NewGuid();

    if (!_lockedResources.ContainsKey(resource))
    {
      try
      {
        PostgreSqlDistributedLock.Acquire(_dedicatedConnection, resource, timeout, _context);
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
          && _dedicatedConnection?.State == ConnectionState.Open)
      {
        PostgreSqlDistributedLock.Release(_dedicatedConnection, resource, _context);
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
        _context.ConnectionManager.ReleaseConnection(_dedicatedConnection);
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