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
using Npgsql;
using NpgsqlTypes;

namespace Hangfire.PostgreSql;

internal static class DbConnectionExtensions
{
  public static SqlQueryProcessor Process(this IDbConnection connection, string query, IDbTransaction? transaction = null)
  {
    return SqlQueryProcessor.Create(connection, query, transaction);
  }
}

internal sealed class SqlQueryProcessor
{
  private readonly NpgsqlConnection _connection;
  private readonly string _query;
  private readonly NpgsqlTransaction? _transaction;
  private readonly List<NpgsqlParameter> _parameters = [];
  private int? _commandTimeout;

  private SqlQueryProcessor(NpgsqlConnection connection, string query, NpgsqlTransaction? transaction = null)
  {
    if (string.IsNullOrWhiteSpace(query))
    {
      throw new ArgumentException("Query cannot be null or empty.", nameof(query));
    }

    _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    _query = query;
    _transaction = transaction;
  }

  public static SqlQueryProcessor Create(IDbConnection connection, string query, IDbTransaction? transaction = null)
  {
    if (connection is not NpgsqlConnection npgsqlConnection)
    {
      throw new ArgumentException("Connection must be of type NpgsqlConnection.", nameof(connection));
    }

    if (transaction is not null && transaction is not NpgsqlTransaction)
    {
      throw new ArgumentException("Transaction must be of type NpgsqlTransaction.", nameof(transaction));
    }

    return new SqlQueryProcessor(npgsqlConnection, query, transaction as NpgsqlTransaction);
  }

  public SqlQueryProcessor WithParameters(params NpgsqlParameter[] parameters)
  {
    if (parameters == null)
    {
      throw new ArgumentNullException(nameof(parameters));
    }

    if (parameters.Length > 0)
    {
      _parameters.AddRange(parameters);
    }

    return this;
  }

  public SqlQueryProcessor WithParameters(params object?[] parameters)
  {
    if (parameters == null)
    {
      throw new ArgumentNullException(nameof(parameters));
    }
    
    if (parameters.Length > 0)
    {
      _parameters.AddRange(parameters.Select(x => new NpgsqlParameter(null, x ?? DBNull.Value)));
    }

    return this;
  }

  public SqlQueryProcessor WithParameter(NpgsqlParameter parameter)
  {
    if (parameter == null)
    {
      throw new ArgumentNullException(nameof(parameter));
    }

    _parameters.Add(parameter);
    return this;
  }

  public SqlQueryProcessor WithParameter(object? value)
  {
    _parameters.Add(new NpgsqlParameter(null, value ?? DBNull.Value));
    return this;
  }
  
  public SqlQueryProcessor WithParameter(object? value, NpgsqlDbType type) 
  {
    _parameters.Add(new NpgsqlParameter(null, value ?? DBNull.Value) { NpgsqlDbType = type });
    return this;
  }

  public SqlQueryProcessor WithCommandTimeout(int? seconds)
  {
    _commandTimeout = seconds;
    return this;
  }

  public int Execute()
  {
    using NpgsqlCommandWrapper command = CreateCommand();
    return command.Command.ExecuteNonQuery();
  }

  public async Task<int> ExecuteAsync(CancellationToken cancellationToken = default)
  {
    using NpgsqlCommandWrapper command = CreateCommand();
    return await command.Command.ExecuteNonQueryAsync(cancellationToken);
  }

  public IEnumerable<T> Select<T>(Func<NpgsqlDataReader, T> readerFunc)
  {
    if (readerFunc == null)
    {
      throw new ArgumentNullException(nameof(readerFunc));
    }
    
    using NpgsqlCommandWrapper command = CreateCommand();
    using NpgsqlDataReader reader = command.Command.ExecuteReader();
    if (!reader.HasRows)
    {
      yield break;
    }
    while (reader.Read())
    {
      yield return readerFunc(reader);
    }
  }

  public void ForEach(Action<NpgsqlDataReader> readerFunc)
  {
    if (readerFunc == null)
    {
      throw new ArgumentNullException(nameof(readerFunc));
    }

    using NpgsqlCommandWrapper wrapper = CreateCommand();
    using NpgsqlDataReader reader = wrapper.Command.ExecuteReader();
    if (!reader.HasRows)
    {
      return;
    }
    while (reader.Read())
    {
      readerFunc(reader);
    }
  }

  private NpgsqlCommandWrapper CreateCommand()
  {
    return new NpgsqlCommandWrapper(_connection, _query, _parameters, _transaction, _commandTimeout);
  }

  private class NpgsqlCommandWrapper : IDisposable
  {
    private readonly NpgsqlConnection _connection;
    private readonly NpgsqlCommand _command;
    private bool _wasOpened;

    public NpgsqlCommandWrapper(NpgsqlConnection connection, string query, List<NpgsqlParameter> parameters, NpgsqlTransaction? transaction, int? commandTimeout)
    {
      _connection = connection;

      NpgsqlCommand command = new(query, connection, transaction);

      if (parameters.Count > 0)
      {
        command.Parameters.AddRange(parameters.ToArray());
      }
      if (commandTimeout.HasValue)
      {
        command.CommandTimeout = commandTimeout.Value;
      }

      _command = command;
    }

    public NpgsqlCommand Command
    {
      get {
        if (_connection.State != ConnectionState.Open)
        {
          _connection.Open();
          _wasOpened = true;
        }

        return _command;
      }
    }

    public void Dispose()
    {
      _command.Dispose();
      if (_wasOpened)
      {
        _connection.Close();
      }
    }
  }
}
