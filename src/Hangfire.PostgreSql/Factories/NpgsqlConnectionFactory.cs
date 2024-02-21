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
  [CanBeNull] private readonly Func<string> _getConnectionString;

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

  public NpgsqlConnectionFactory(Func<string> getConnectionString, PostgreSqlStorageOptions options, [CanBeNull] Action<NpgsqlConnection> connectionSetup = null) : this(getConnectionString.Invoke(), options, connectionSetup)
  {
    _getConnectionString = getConnectionString;
  }

  /// <inheritdoc />
  public override NpgsqlConnection GetOrCreateConnection()
  {
    var connectionString = _connectionString;
    if (_getConnectionString != null)
    {
      connectionString = SetupConnectionStringBuilder(_getConnectionString.Invoke()).ConnectionString;
    }
    
    NpgsqlConnection connection = new(connectionString);
    _connectionSetup?.Invoke(connection);
    return connection;
  }
}