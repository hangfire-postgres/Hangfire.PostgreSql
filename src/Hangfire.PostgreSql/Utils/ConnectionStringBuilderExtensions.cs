using Npgsql;

namespace Hangfire.PostgreSql.Utils
{
  internal static class ConnectionStringBuilderExtensions
  {
    /// <summary>
    /// Timezone must be UTC for compatibility with Npgsql 6 and our usage of "timestamp without time zone" columns
    /// See https://github.com/frankhommers/Hangfire.PostgreSql/issues/221
    /// </summary>
    /// <param name="connectionStringBuilder">The ConnectionStringBuilder to set the Timezone property for</param>
    internal static void SetTimezoneToUtcForNpgsqlCompatibility(this NpgsqlConnectionStringBuilder connectionStringBuilder)
    {
      if (connectionStringBuilder == null)
      {
        return;
      }

      connectionStringBuilder.Timezone = "UTC";
    }
  }
}
