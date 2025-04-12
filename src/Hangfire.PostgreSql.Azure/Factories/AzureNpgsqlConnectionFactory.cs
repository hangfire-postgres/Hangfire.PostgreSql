using Azure.Core;
using Azure.Identity;
using Hangfire.PostgreSql.Factories;
using Hangfire.PostgreSql.Properties;
using Npgsql;

namespace Hangfire.PostgreSql.Azure.Factories
{
  public class AzureNpgsqlConnectionFactory : NpgsqlInstanceConnectionFactoryBase
  {
    private readonly NpgsqlDataSource _dataSource;

    /// <summary>
    /// Instatiates a factory already configured to fetch tokens from azure
    /// Token is refreshed every 4 hours
    /// </summary>
    /// <param name="connectionString"></param>
    /// <param name="options"></param>
    /// <param name="dataSourceBuilderSetup">You have here the opportunity to override the datasource builder, including the password provider</param>
    public AzureNpgsqlConnectionFactory(string connectionString, PostgreSqlStorageOptions options, [CanBeNull] Action<NpgsqlDataSourceBuilder>? dataSourceBuilderSetup = null) : base(options)
    {
      NpgsqlDataSourceBuilder dataSourceBuilder = new(connectionString);

      ConfigurePeriodicPasswordProvider(dataSourceBuilder);

      dataSourceBuilderSetup?.Invoke(dataSourceBuilder);
      _dataSource = dataSourceBuilder.Build()!;
    }


    public override NpgsqlConnection GetOrCreateConnection()
    {
      return _dataSource.CreateConnection();
    }

    private static void ConfigurePeriodicPasswordProvider(NpgsqlDataSourceBuilder dataSourceBuilder)
    {
      //Kudos https://mattparker.dev/blog/azure-managed-identity-postgres-aspnetcore
      dataSourceBuilder.UsePeriodicPasswordProvider(
          async (connectionStringBuilder, cancellationToken) => {
            try
            {
              DefaultAzureCredentialOptions options = new();
              DefaultAzureCredential credentials = new(options);
              AccessToken token = await credentials.GetTokenAsync(new TokenRequestContext(["https://ossrdbms-aad.database.windows.net/.default"]), cancellationToken);
              return token.Token;
            }
            catch
            {
              throw;
            }
          },
          TimeSpan.FromHours(4), // successRefreshInterval
          TimeSpan.FromSeconds(10) // failureRefreshInterval
      );
    }
  }
}