using System;
using Hangfire.Annotations;
using Npgsql;

namespace Hangfire.PostgreSql.Factories;

public sealed class NewNpgsqlConnectionFactory : NpgsqlInstanceConnectionFactoryBase
{
  private readonly string _connectionString;
  [CanBeNull] private readonly Action<NpgsqlConnection> _connectionSetup;

  public NewNpgsqlConnectionFactory(string connectionString, PostgreSqlStorageOptions options, [CanBeNull] Action<NpgsqlConnection> connectionSetup = null) : base(options)
  {
    _connectionString = SetupConnectionStringBuilder(connectionString ?? throw new ArgumentNullException(nameof(connectionString))).ConnectionString;
    _connectionSetup = connectionSetup;
  }

  public override NpgsqlConnection GetOrCreateConnection()
  {
    NpgsqlConnection connection = new(_connectionString);
    _connectionSetup?.Invoke(connection);
    return connection;
  }
}