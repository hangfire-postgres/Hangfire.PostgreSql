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

using Hangfire.PostgreSql.Factories;
using Npgsql;

namespace Hangfire.PostgreSql;

/// <summary>
/// Bootstrapper options.
/// </summary>
public class PostgreSqlBootstrapperOptions
{
  private readonly PostgreSqlStorageOptions _options;

  internal PostgreSqlBootstrapperOptions(PostgreSqlStorageOptions options)
  {
    _options = options ?? throw new ArgumentNullException(nameof(options));
  }

  internal IConnectionFactory? ConnectionFactory { get; private set; }

  /// <summary>
  /// Configures the bootstrapper to use a custom <see cref="IConnectionFactory"/> to use for each database action.
  /// </summary>
  /// <param name="connectionFactory">Instance of <see cref="IConnectionFactory"/>.</param>
  /// <returns>This instance.</returns>
  /// <exception cref="ArgumentNullException">Throws if <paramref name="connectionFactory"/> is null.</exception>
  public PostgreSqlBootstrapperOptions UseConnectionFactory(IConnectionFactory connectionFactory)
  {
    ConnectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
    return this;
  }

  /// <summary>
  /// Configures the bootstrapper to create a new <see cref="NpgsqlConnection"/> for each database action.
  /// </summary>
  /// <param name="connectionString">Connection string.</param>
  /// <param name="connectionSetup">Optional additional connection setup action to be performed on the created <see cref="NpgsqlConnection"/>.</param>
  /// <returns>This instance.</returns>
  public PostgreSqlBootstrapperOptions UseNpgsqlConnection(string connectionString, Action<NpgsqlConnection>? connectionSetup = null)
  {
    return UseConnectionFactory(new NpgsqlConnectionFactory(connectionString, _options, connectionSetup));
  }

  public PostgreSqlBootstrapperOptions UseNpgsqlConnection(Func<string> getConnectionString, Action<NpgsqlConnection>? connectionSetup = null)
  {
    return UseConnectionFactory(new NpgsqlConnectionFactory(getConnectionString, _options, connectionSetup));
  }

  /// <summary>
  /// Configures the bootstrapper to use the existing <see cref="NpgsqlConnection"/> for each database action.
  /// </summary>
  /// <param name="connection"><see cref="NpgsqlConnection"/> to use.</param>
  /// <returns>This instance.</returns>
  public PostgreSqlBootstrapperOptions UseExistingNpgsqlConnection(NpgsqlConnection connection)
  {
    return UseConnectionFactory(new ExistingNpgsqlConnectionFactory(connection, _options));
  }
}
