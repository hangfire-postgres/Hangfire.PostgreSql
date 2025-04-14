using Hangfire.PostgreSql.Components;

namespace Hangfire.PostgreSql;

internal sealed class PostgreSqlStorageContext(PostgreSqlStorageOptions options, PostgreSqlDbConnectionManager connectionManager)
{
  public PostgreSqlStorageOptions Options { get; } = options;
  public PostgreSqlDbConnectionManager ConnectionManager { get; } = connectionManager;
  public SqlQueryProvider QueryProvider { get; } = new(options.SchemaName);
  public PostgreSqlHeartbeatProcess? HeartbeatProcess { get; } = options.UseSlidingInvisibilityTimeout ? new PostgreSqlHeartbeatProcess() : null;
  public PersistentJobQueueProviderCollection QueueProviders { get; internal set; } = null!;
}
