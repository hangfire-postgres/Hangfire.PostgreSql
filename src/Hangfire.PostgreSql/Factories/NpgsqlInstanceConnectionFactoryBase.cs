using System;
using Hangfire.Annotations;
using Npgsql;

namespace Hangfire.PostgreSql.Factories;

public abstract class NpgsqlInstanceConnectionFactoryBase : IConnectionFactory
{
  private readonly PostgreSqlStorageOptions _options;
  [CanBeNull] private NpgsqlConnectionStringBuilder _connectionStringBuilder;
  [CanBeNull] private string _connectionString;

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