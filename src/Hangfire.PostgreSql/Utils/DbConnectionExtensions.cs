using System.Data;
using Npgsql;

namespace Hangfire.PostgreSql.Utils;

internal static class DbConnectionExtensions
{
  private static bool? _supportsNotifications;

  internal static bool SupportsNotifications(this IDbConnection connection)
  {
    if (_supportsNotifications.HasValue)
    {
      return _supportsNotifications.Value;
    }

    if (connection is not NpgsqlConnection npgsqlConnection)
    {
      _supportsNotifications = false;
      return false;
    }

    if (npgsqlConnection.State != ConnectionState.Open)
    {
      npgsqlConnection.Open();
    }

    _supportsNotifications = npgsqlConnection.PostgreSqlVersion.Major >= 11;
    return _supportsNotifications.Value;
  }
}
