using System;
using Hangfire.Annotations;
using Npgsql;

namespace Hangfire.PostgreSql.Factories;

/// <summary>
/// Connection factory that creates a new <see cref="NpgsqlConnection"/> based on the connection string.
/// </summary>
public sealed class NpgsqlConnectionFactory : NpgsqlInstanceConnectionFactoryBase
{
  private readonly string _connectionString;
  [CanBeNull] private readonly Action<NpgsqlConnection> _connectionSetup;

  /// <summary>
  /// Instantiates the factory using specified <paramref name="connectionString"/>.
  /// </summary>
  /// <param name="connectionString">Connection string.</param>
  /// <param name="options"><see cref="PostgreSqlStorageOptions"/> used for connection string verification.</param>
  /// <param name="connectionSetup">Optional additional connection setup action to be performed on the created <see cref="NpgsqlConnection"/>.</param>
  /// <exception cref="ArgumentNullException">Throws if <paramref name="connectionString"/> is null.</exception>
  public NpgsqlConnectionFactory(string connectionString, PostgreSqlStorageOptions options, [CanBeNull] Action<NpgsqlConnection> connectionSetup = null) : base(options)
  {
    _connectionString = SetupConnectionStringBuilder(connectionString ?? throw new ArgumentNullException(nameof(connectionString))).ConnectionString;
    _connectionSetup = connectionSetup;
  }

  /// <inheritdoc />
  public override NpgsqlConnection GetOrCreateConnection()
  {
    NpgsqlConnection connection = new(_connectionString);
    _connectionSetup?.Invoke(connection);
    return connection;
  }
}