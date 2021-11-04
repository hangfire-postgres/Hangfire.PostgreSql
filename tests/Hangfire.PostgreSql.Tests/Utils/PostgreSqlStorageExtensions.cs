namespace Hangfire.PostgreSql.Tests
{
    internal static class PostgreSqlStorageExtensions
    {
        public static PostgreSqlConnection GetStorageConnection(this PostgreSqlStorage storage) =>
            storage.GetConnection() as PostgreSqlConnection;
    }
}