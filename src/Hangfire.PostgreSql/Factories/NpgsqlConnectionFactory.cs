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

/// <summary>
/// Connection factory that creates a new <see cref="NpgsqlConnection"/> based on the connection string.
/// </summary>
public sealed class NpgsqlConnectionFactory : NpgsqlInstanceConnectionFactoryBase
{
  private readonly string _connectionString;
  private readonly Action<NpgsqlConnection>? _connectionSetup;
  private readonly Func<string>? _getConnectionString;

  /// <summary>
  /// Instantiates the factory using specified <paramref name="connectionString"/>.
  /// </summary>
  /// <param name="connectionString">Connection string.</param>
  /// <param name="options"><see cref="PostgreSqlStorageOptions"/> used for connection string verification.</param>
  /// <param name="connectionSetup">Optional additional connection setup action to be performed on the created <see cref="NpgsqlConnection"/>.</param>
  /// <exception cref="ArgumentNullException">Throws if <paramref name="connectionString"/> is null.</exception>
  public NpgsqlConnectionFactory(string connectionString, PostgreSqlStorageOptions options, Action<NpgsqlConnection>? connectionSetup = null) : base(options)
  {
    _connectionString = SetupConnectionStringBuilder(connectionString ?? throw new ArgumentNullException(nameof(connectionString))).ConnectionString;
    _connectionSetup = connectionSetup;
  }

  public NpgsqlConnectionFactory(Func<string> getConnectionString, PostgreSqlStorageOptions options, Action<NpgsqlConnection>? connectionSetup = null) : this(getConnectionString.Invoke(), options, connectionSetup)
  {
    _getConnectionString = getConnectionString;
  }

  /// <inheritdoc />
  public override NpgsqlConnection GetOrCreateConnection()
  {
    string connectionString = _connectionString;
    if (_getConnectionString != null)
    {
      connectionString = SetupConnectionStringBuilder(_getConnectionString.Invoke()).ConnectionString;
    }
    
    NpgsqlConnection connection = new(connectionString);
    _connectionSetup?.Invoke(connection);
    return connection;
  }
}