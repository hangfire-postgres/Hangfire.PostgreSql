using System;
using Hangfire.PostgreSql.Factories;
using Moq;
using Npgsql;

namespace Hangfire.PostgreSql.Tests.Utils
{
  public class PostgreSqlStorageFixture : IDisposable
  {
    private readonly PostgreSqlStorageOptions _storageOptions;
    private bool _initialized;
    private NpgsqlConnection _mainConnection;

    public PostgreSqlStorageFixture()
    {
      PersistentJobQueueMock = new Mock<IPersistentJobQueue>();

      Mock<IPersistentJobQueueProvider> provider = new();
      provider.Setup(x => x.GetJobQueue())
        .Returns(PersistentJobQueueMock.Object);

      PersistentJobQueueProviderCollection = new PersistentJobQueueProviderCollection(provider.Object);

      _storageOptions = new PostgreSqlStorageOptions {
        SchemaName = ConnectionUtils.GetSchemaName(),
        EnableTransactionScopeEnlistment = true,
      };
    }

    public Mock<IPersistentJobQueue> PersistentJobQueueMock { get; }

    public PersistentJobQueueProviderCollection PersistentJobQueueProviderCollection { get; }

    public PostgreSqlStorage Storage { get; private set; }
    public NpgsqlConnection MainConnection => _mainConnection ?? (_mainConnection = ConnectionUtils.CreateConnection());

    public void Dispose()
    {
      _mainConnection?.Dispose();
      _mainConnection = null;
    }

    public void SetupOptions(Action<PostgreSqlStorageOptions> storageOptionsConfigure)
    {
      storageOptionsConfigure(_storageOptions);
    }

    public PostgreSqlStorage SafeInit(NpgsqlConnection connection = null)
    {
      return _initialized
        ? Storage
        : ForceInit(connection);
    }

    public PostgreSqlStorage ForceInit(NpgsqlConnection connection = null)
    {
      Storage = new PostgreSqlStorage(new ExistingNpgsqlConnectionFactory(connection ?? MainConnection, _storageOptions), _storageOptions) {
        QueueProviders = PersistentJobQueueProviderCollection,
      };
      _initialized = true;
      return Storage;
    }

    public void SafeInit(PostgreSqlStorageOptions options,
      PersistentJobQueueProviderCollection jobQueueProviderCollection = null,
      NpgsqlConnection connection = null)
    {
      if (!_initialized)
      {
        ForceInit(options, jobQueueProviderCollection, connection);
        return;
      }

      Storage.QueueProviders = jobQueueProviderCollection;
    }

    public void ForceInit(PostgreSqlStorageOptions options,
      PersistentJobQueueProviderCollection jobQueueProviderCollection = null,
      NpgsqlConnection connection = null)
    {
      Storage = new PostgreSqlStorage(new ExistingNpgsqlConnectionFactory(connection ?? MainConnection, options), options) {
        QueueProviders = jobQueueProviderCollection,
      };
      _initialized = true;
    }
  }
}
