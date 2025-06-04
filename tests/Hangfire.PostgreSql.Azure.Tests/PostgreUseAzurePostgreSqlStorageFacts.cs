using Hangfire.PostgreSql.Azure.Factories;
using Hangfire.PostgreSql.Tests.Utils;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Azure.Tests
{
  public class PostgreUseAzurePostgreSqlStorageFacts : IClassFixture<PostgreSqlStorageFixture>
  {
    private readonly PostgreSqlStorageOptions _options;

    public PostgreUseAzurePostgreSqlStorageFacts()
    {
      _options = new();
    }

    [Fact]
    public async Task AzureNpgsqlConnectionFactory_Can_Generate_Connection()
    {
      AzureNpgsqlConnectionFactory factory = new(ConnectionUtils.GetConnectionString().Replace("Password=password", ""), _options, dsb => {
        dsb.UsePeriodicPasswordProvider((_, _) => ValueTask.FromResult("password"), TimeSpan.FromHours(1), TimeSpan.FromSeconds(1));
      });

      using NpgsqlConnection connection = factory.GetOrCreateConnection();

      await connection.OpenAsync();

      await using NpgsqlCommand command = new("SELECT '8'", connection);
      await using NpgsqlDataReader reader = await command.ExecuteReaderAsync();
      await reader.ReadAsync();

      Assert.Equal("8", reader.GetValue(0));

      await connection.CloseAsync();
    }

  }
}
