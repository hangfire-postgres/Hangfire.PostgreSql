using Hangfire.Annotations;
using Hangfire.PostgreSql.Azure.Factories;
using Npgsql;

namespace Hangfire.PostgreSql.Azure
{
  public static class PostgreSqlBootstrapperConfigurationExtensions
  {
    /// <summary>
    ///   Tells the bootstrapper to use PostgreSQL as a job storage  with the given options
    /// </summary>
    /// <param name="configuration">Configuration</param>
    /// <param name="connectionString">Connection string</param>
    /// <param name="dataSourceBuilderSetup">You have here the opportunity to override the datasource builder, including the password provider</param>
    public static IGlobalConfiguration<PostgreSqlStorage> UseAzurePostgreSqlStorage(
      this IGlobalConfiguration configuration,
      string connectionString,
      PostgreSqlStorageOptions options,
      [CanBeNull] Action<NpgsqlDataSourceBuilder>? dataSourceBuilderSetup = null)
    {
      return configuration.UsePostgreSqlStorage(c => c.UseConnectionFactory(new AzureNpgsqlConnectionFactory(connectionString, options, dataSourceBuilderSetup)), options);
    }
  }

}
