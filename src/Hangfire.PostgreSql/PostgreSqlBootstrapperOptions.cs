using System;
using Hangfire.Annotations;
using Hangfire.PostgreSql.Factories;
using Npgsql;

namespace Hangfire.PostgreSql;

public class PostgreSqlBootstrapperOptions
{
  private readonly PostgreSqlStorageOptions _options;

  public PostgreSqlBootstrapperOptions(PostgreSqlStorageOptions options)
  {
    _options = options ?? throw new ArgumentNullException(nameof(options));
  }

  [CanBeNull] internal IConnectionFactory ConnectionFactory { get; private set; }

  public PostgreSqlBootstrapperOptions UseConnectionFactory(IConnectionFactory connectionFactory)
  {
    ConnectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
    return this;
  }

  public PostgreSqlBootstrapperOptions UseNewNpgsqlConnection(string connectionString, [CanBeNull] Action<NpgsqlConnection> connectionSetup = null)
  {
    return UseConnectionFactory(new NewNpgsqlConnectionFactory(connectionString, _options, connectionSetup));
  }

  public PostgreSqlBootstrapperOptions UseExistingNpgsqlConnection(NpgsqlConnection connection)
  {
    return UseConnectionFactory(new ExistingNpgsqlConnectionFactory(connection, _options));
  }
}
