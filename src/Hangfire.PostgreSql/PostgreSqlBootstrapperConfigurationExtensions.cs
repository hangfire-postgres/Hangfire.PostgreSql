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
using Npgsql;

namespace Hangfire.PostgreSql
{
  public static class PostgreSqlBootstrapperConfigurationExtensions
  {
    /// <summary>
    ///   Tells the bootstrapper to use PostgreSQL as a job storage,
    ///   that can be accessed using the given connection string or
    ///   its name.
    /// </summary>
    /// <param name="configuration">Configuration</param>
    /// <param name="connectionString">Connection string</param>
    public static IGlobalConfiguration<PostgreSqlStorage> UsePostgreSqlStorage(
      this IGlobalConfiguration configuration,
      string connectionString)
    {
      return configuration.UsePostgreSqlStorage(connectionString, null, new PostgreSqlStorageOptions());
    }

    /// <summary>
    ///   Tells the bootstrapper to use PostgreSQL as a job storage
    ///   with the given options, that can be accessed using the specified
    ///   connection string.
    /// </summary>
    /// <param name="configuration">Configuration</param>
    /// <param name="connectionString">Connection string</param>
    /// <param name="options">Advanced options</param>
    public static IGlobalConfiguration<PostgreSqlStorage> UsePostgreSqlStorage(
      this IGlobalConfiguration configuration,
      string connectionString,
      PostgreSqlStorageOptions options)
    {
      return configuration.UsePostgreSqlStorage(connectionString, null, options);
    }

    /// <summary>
    ///   Tells the bootstrapper to use PostgreSQL as a job storage
    ///   with the given options, that can be accessed using the specified
    ///   connection string.
    /// </summary>
    /// <param name="configuration">Configuration</param>
    /// <param name="connectionString">Connection string</param>
    /// <param name="connectionSetup">Optional setup action to apply to created connections</param>
    /// <param name="options">Advanced options</param>
    public static IGlobalConfiguration<PostgreSqlStorage> UsePostgreSqlStorage(
      this IGlobalConfiguration configuration,
      string connectionString,
      Action<NpgsqlConnection> connectionSetup,
      PostgreSqlStorageOptions options)
    {
      PostgreSqlStorage storage = new(connectionString, connectionSetup, options);

      return configuration.UseStorage(storage);
    }

    /// <summary>
    ///   Tells the bootstrapper to use PostgreSQL as a job storage
    ///   with the given options, that can be accessed using the specified
    ///   connection factory.
    /// </summary>
    /// <param name="configuration">Configuration</param>
    /// <param name="connectionFactory">Connection factory</param>
    /// <param name="options">Advanced options</param>
    public static IGlobalConfiguration<PostgreSqlStorage> UsePostgreSqlStorage(
      this IGlobalConfiguration configuration,
      IConnectionFactory connectionFactory,
      PostgreSqlStorageOptions options)
    {
      PostgreSqlStorage storage = new(connectionFactory, options);

      return configuration.UseStorage(storage);
    }

    /// <summary>
    ///   Tells the bootstrapper to use PostgreSQL as a job storage
    ///   with the given options, that can be accessed using the specified
    ///   connection factory.
    /// </summary>
    /// <param name="configuration">Configuration</param>
    /// <param name="connectionFactory">Connection factory</param>
    public static IGlobalConfiguration<PostgreSqlStorage> UsePostgreSqlStorage(
      this IGlobalConfiguration configuration,
      IConnectionFactory connectionFactory)
    {
      PostgreSqlStorage storage = new(connectionFactory, new PostgreSqlStorageOptions());

      return configuration.UseStorage(storage);
    }
  }
}
