namespace Hangfire.PostgreSql.Tests.Utils
{
  internal static class PostgreSqlStorageExtensions
  {
    public static PostgreSqlConnection GetStorageConnection(this PostgreSqlStorage storage)
    {
      return storage.GetConnection() as PostgreSqlConnection;
    }
  }
}
