using Npgsql;
using System;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlStorageFixture : IDisposable
    {
        private bool _initialized;

        public void SafeInit(PostgreSqlStorageOptions options, PersistentJobQueueProviderCollection jobQueueProviderCollection = null, NpgsqlConnection connection = null)
        {
            if (_initialized)
            {
                return;
            }

            ForceInit(options, jobQueueProviderCollection, connection);
        }

        public void ForceInit(PostgreSqlStorageOptions options, PersistentJobQueueProviderCollection jobQueueProviderCollection = null, NpgsqlConnection connection = null)
        {
            Storage = new PostgreSqlStorage(connection ?? ConnectionUtils.CreateConnection(), options);
            if (jobQueueProviderCollection != null)
            {
                Storage.QueueProviders = jobQueueProviderCollection;
            }
            _initialized = true;
        }

        public PostgreSqlStorage Storage { get; private set; }

        public void Dispose() { }
    }
}