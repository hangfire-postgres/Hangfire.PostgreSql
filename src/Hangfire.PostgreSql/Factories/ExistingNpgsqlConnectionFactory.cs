using System;
using Npgsql;

namespace Hangfire.PostgreSql.Factories;

public sealed class ExistingNpgsqlConnectionFactory : NpgsqlInstanceConnectionFactoryBase
{
  private readonly NpgsqlConnection _connection;

  public ExistingNpgsqlConnectionFactory(NpgsqlConnection connection, PostgreSqlStorageOptions options) : base(options)
  {
    _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    // To ensure valid connection string - throws internally
    SetupConnectionStringBuilder(_connection.ConnectionString);
  }

  public override NpgsqlConnection GetOrCreateConnection()
  {
    return _connection;
  }
}
