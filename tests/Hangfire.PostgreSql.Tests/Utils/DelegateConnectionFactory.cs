using System;
using Npgsql;

namespace Hangfire.PostgreSql.Tests.Utils
{
  /// <summary>
  /// Simple test-only connection factory that delegates connection creation to the provided function.
  /// </summary>
  internal sealed class DelegateConnectionFactory : IConnectionFactory
  {
    private readonly Func<NpgsqlConnection> _factory;

    public DelegateConnectionFactory(Func<NpgsqlConnection> factory)
    {
      _factory = factory ?? throw new ArgumentNullException(nameof(factory));
    }

    public NpgsqlConnection GetOrCreateConnection()
    {
      return _factory();
    }
  }
}

