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

using Npgsql;

namespace Hangfire.PostgreSql.Factories;

public abstract class NpgsqlInstanceConnectionFactoryBase : IConnectionFactory
{
  private readonly PostgreSqlStorageOptions _options;
  private NpgsqlConnectionStringBuilder? _connectionStringBuilder;
  private string? _connectionString;

  protected NpgsqlInstanceConnectionFactoryBase(PostgreSqlStorageOptions options)
  {
    _options = options ?? throw new ArgumentNullException(nameof(options));
  }

  /// <summary>
  /// Gets the connection string builder associated with the current instance.
  /// </summary>
  /// <exception cref="InvalidOperationException">Throws if connection string builder has not been initialized.</exception>
  public NpgsqlConnectionStringBuilder ConnectionString =>
    _connectionStringBuilder ?? throw new InvalidOperationException("Connection string builder has not been initialized");

  protected NpgsqlConnectionStringBuilder SetupConnectionStringBuilder(string connectionString)
  {
    if (_connectionStringBuilder != null && string.Equals(_connectionString, connectionString, StringComparison.OrdinalIgnoreCase))
    {
      return _connectionStringBuilder;
    }

    try
    {
      _connectionString = connectionString;
      NpgsqlConnectionStringBuilder builder = new(connectionString);

      // The connection string must not be modified when transaction enlistment is enabled, otherwise it will cause
      // prepared transactions and probably fail when other statements (outside of hangfire) ran within the same
      // transaction. Also see #248.
      if (!_options.EnableTransactionScopeEnlistment && builder.Enlist)
      {
        throw new ArgumentException($"TransactionScope enlistment must be enabled by setting {nameof(PostgreSqlStorageOptions)}.{nameof(PostgreSqlStorageOptions.EnableTransactionScopeEnlistment)} to `true`.");
      }

      return _connectionStringBuilder = builder;
    }
    catch (ArgumentException ex)
    {
      throw new ArgumentException($"Connection string is not valid", nameof(connectionString), ex);
    }
  }

  /// <inheritdoc />
  public abstract NpgsqlConnection GetOrCreateConnection();
}