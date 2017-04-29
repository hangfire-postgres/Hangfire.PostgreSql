// This file is part of Hangfire.PostgreSql.
// Copyright © 2014 Frank Hommers <http://hmm.rs/Hangfire.PostgreSql>.
// 
// Hangfire.PostgreSql is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.PostgreSql  is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.PostgreSql. If not, see <http://www.gnu.org/licenses/>.
//
// This work is based on the work of Sergey Odinokov, author of 
// Hangfire. <http://hangfire.io/>
//   
//    Special thanks goes to him.

using System;
using System.Data;
using System.Globalization;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.PostgreSql
{
#if (NETCORE1 || NETCORE50 || NETSTANDARD1_5 || NETSTANDARD1_6)
    public
#else
	internal
#endif
    class ExpirationManager : IBackgroundProcess, IServerComponent
    {
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

#if (NETCORE1 || NETCORE50 || NETSTANDARD1_5 || NETSTANDARD1_6)
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));
#else
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
#endif
        private static readonly string[] ProcessedCounterStates =
        {
            "stats:succeeded",
            "stats:deleted"
        };

        private static readonly string[] ProcessedTables =
        {
            "job",
            "list",
            "set",
            "hash",
        };

        private readonly PostgreSqlStorage _storage;
        private readonly TimeSpan _checkInterval;
        private readonly PostgreSqlStorageOptions _options;

        public ExpirationManager(PostgreSqlStorage storage, PostgreSqlStorageOptions options)
            : this(storage, options, TimeSpan.FromHours(1))
        {
        }

        public ExpirationManager(PostgreSqlStorage storage, PostgreSqlStorageOptions options, TimeSpan checkInterval)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _checkInterval = checkInterval;
        }

        public override string ToString() => "SQL Records Expiration Manager";

        public void Execute(BackgroundProcessContext context) => Execute(context.CancellationToken);

        public void Execute(CancellationToken cancellationToken)
        {
            AggregateCounters(cancellationToken);

            foreach (var table in ProcessedTables)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", table);

                int removedCount;

                do
                {
                    using (var storageConnection = (PostgreSqlConnection)_storage.GetConnection())
                    {
                        using (var transaction = storageConnection.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            removedCount = storageConnection.Connection.Execute(
                                string.Format(@"
DELETE FROM """ + _options.SchemaName + @""".""{0}"" 
WHERE ""id"" IN (
    SELECT ""id"" 
    FROM """ + _options.SchemaName + @""".""{0}"" 
    WHERE ""expireat"" < NOW() AT TIME ZONE 'UTC' 
    LIMIT {1}
)", table, NumberOfRecordsInSinglePass.ToString(CultureInfo.InvariantCulture)), transaction);

                            transaction.Commit();
                        }
                    }

                    if (removedCount > 0)
                    {
                        Logger.InfoFormat("Removed {0} outdated record(s) from '{1}' table.", removedCount, table);

                        cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                } while (removedCount != 0);
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        private void AggregateCounters(CancellationToken cancellationToken)
        {
            foreach (var counterState in ProcessedCounterStates)
            {
                AggregateCounter(counterState);
                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        private void AggregateCounter(string counterName)
        {
            Logger.DebugFormat("Removing outdated counters '{0}'...", counterName);
            using (var storageConnection = (PostgreSqlConnection)_storage.GetConnection())
            {
                using (var transaction = storageConnection.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    var aggregateQuery = $@"
WITH counters AS (
DELETE FROM ""{_options.SchemaName}"".""counter""
WHERE ""key"" LIKE '{counterName}%'
RETURNING *
)

SELECT SUM(value) FROM counters;
";
                    var value = storageConnection.Connection.ExecuteScalar<long>(aggregateQuery);

                    var insertQuery = $@"INSERT INTO ""{_options.SchemaName}"".""counter""(""key"", ""value"") VALUES (@key, @value);";
                    storageConnection.Connection.Execute(insertQuery, new { key = counterName, value });

                    transaction.Commit();
                }
            }
        }
    }
}
