using Moq;
using Npgsql;
using System;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlStorageFixture : IDisposable
    {
        private readonly Mock<IPersistentJobQueue> _queueMock;
        private readonly PersistentJobQueueProviderCollection _providers;
        private readonly PostgreSqlStorageOptions _storageOptions;
        private NpgsqlConnection _mainConnection;
        private bool _initialized;

        public PostgreSqlStorageFixture()
        {
            _queueMock = new Mock<IPersistentJobQueue>();

            var provider = new Mock<IPersistentJobQueueProvider>();
            provider.Setup(x => x.GetJobQueue())
                .Returns(_queueMock.Object);

            _providers = new PersistentJobQueueProviderCollection(provider.Object);

            _storageOptions = new PostgreSqlStorageOptions
            {
                SchemaName = ConnectionUtils.GetSchemaName(),
                EnableTransactionScopeEnlistment = true
            };
        }

        public void Dispose()
        {
            _mainConnection?.Dispose();
            _mainConnection = null;
        }

        public Mock<IPersistentJobQueue> PersistentJobQueueMock => _queueMock;
        public PersistentJobQueueProviderCollection PersistentJobQueueProviderCollection => _providers;
        public PostgreSqlStorage Storage { get; private set; }
        public NpgsqlConnection MainConnection => _mainConnection ?? (_mainConnection = ConnectionUtils.CreateConnection());

        public void SetupOptions(Action<PostgreSqlStorageOptions> storageOptionsConfigure) =>
            storageOptionsConfigure(_storageOptions);

        public PostgreSqlStorage SafeInit(NpgsqlConnection connection = null)
        {
            return _initialized
                ? Storage
                : ForceInit(connection);
        }

        public PostgreSqlStorage ForceInit(NpgsqlConnection connection = null)
        {
            Storage = new PostgreSqlStorage(connection ?? MainConnection, _storageOptions)
            {
                QueueProviders = _providers
            };
            _initialized = true;
            return Storage;
        }

        public void SafeInit(PostgreSqlStorageOptions options, PersistentJobQueueProviderCollection jobQueueProviderCollection = null, NpgsqlConnection connection = null)
        {
            if (!_initialized)
            {
                ForceInit(options, jobQueueProviderCollection, connection);
                return;
            }

            Storage.QueueProviders = jobQueueProviderCollection;
        }

        public void ForceInit(PostgreSqlStorageOptions options, PersistentJobQueueProviderCollection jobQueueProviderCollection = null, NpgsqlConnection connection = null)
        {
            Storage = new PostgreSqlStorage(connection ?? MainConnection, options)
            {
                QueueProviders = jobQueueProviderCollection
            };
            _initialized = true;
        }
    }
}