using System;
using Npgsql;

namespace Hangfire.PostgreSql.Factories;

/// <summary>
/// Connection factory that utilizes an already-existing <see cref="NpgsqlConnection"/>.
/// </summary>
public sealed class ExistingNpgsqlConnectionFactory : NpgsqlInstanceConnectionFactoryBase
{
  private readonly NpgsqlConnection _connection;

  /// <summary>
  /// Instantiates the factory using specified <paramref name="connection"/>.
  /// </summary>
  /// <param name="connection"><see cref="NpgsqlConnection"/> to use.</param>
  /// <param name="options"><see cref="PostgreSqlStorageOptions"/> used for connection string verification.</param>
  /// <exception cref="ArgumentNullException"></exception>
  public ExistingNpgsqlConnectionFactory(NpgsqlConnection connection, PostgreSqlStorageOptions options) : base(options)
  {
    _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    // To ensure valid connection string - throws internally
    SetupConnectionStringBuilder(_connection.ConnectionString);
  }

  /// <inheritdoc />
  public override NpgsqlConnection GetOrCreateConnection()
  {
    return _connection;
  }
}
