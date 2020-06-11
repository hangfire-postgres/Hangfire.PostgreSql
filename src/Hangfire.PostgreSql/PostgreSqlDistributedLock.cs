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
using System.Diagnostics;
using System.Threading;
using Dapper;

namespace Hangfire.PostgreSql
{
    public sealed class PostgreSqlDistributedLock : IDisposable
    {
        private readonly string _resource;
        private readonly IDbConnection _connection;
        private readonly PostgreSqlStorageOptions _options;
        private bool _completed;

        public PostgreSqlDistributedLock(string resource, TimeSpan timeout, IDbConnection connection,
            PostgreSqlStorageOptions options)
        {
            if (string.IsNullOrEmpty(resource)) throw new ArgumentNullException(nameof(resource));

            _resource = resource;
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _options = options ?? throw new ArgumentNullException(nameof(options));

            if (_options.UseNativeDatabaseTransactions)
                PostgreSqlDistributedLock_Init_Transaction(resource, timeout, connection, options);
            else
                PostgreSqlDistributedLock_Init_UpdateCount(resource, timeout, connection, options);
        }

        private static void PostgreSqlDistributedLock_Init_Transaction(string resource, TimeSpan timeout,
            IDbConnection connection, PostgreSqlStorageOptions options)
        {
            var lockAcquiringTime = Stopwatch.StartNew();

            bool tryAcquireLock = true;

            while (tryAcquireLock)
            {
                TryRemoveDeadlock(resource, connection, options);

                try
                {
                    int rowsAffected = -1;
                    using (var trx = connection.BeginTransaction(IsolationLevel.RepeatableRead))
                    {
                        rowsAffected = connection.Execute($@"
INSERT INTO ""{options.SchemaName}"".""lock""(""resource"", ""acquired"") 
SELECT @resource, @acquired
WHERE NOT EXISTS (
    SELECT 1 FROM ""{options.SchemaName}"".""lock"" 
    WHERE ""resource"" = @resource
);
",
                            new
                            {
                                resource = resource,
                                acquired = DateTime.UtcNow
                            }, trx);
                        trx.Commit();
                    }
                    if (rowsAffected > 0) return;
                }
                catch
                {
                }

                if (lockAcquiringTime.ElapsedMilliseconds > timeout.TotalMilliseconds)
                {
                    tryAcquireLock = false;
                }
                else
                {
                    int sleepDuration = (int)(timeout.TotalMilliseconds - lockAcquiringTime.ElapsedMilliseconds);
                    if (sleepDuration > 1000) sleepDuration = 1000;
                    if (sleepDuration > 0)
                    {
                        Thread.Sleep(sleepDuration);
                    }
                    else
                    {
                        tryAcquireLock = false;
                    }
                }
            }

            throw new PostgreSqlDistributedLockException(
                $"Could not place a lock on the resource \'{resource}\': Lock timeout.");
        }

        private static void TryRemoveDeadlock(string resource, IDbConnection connection, PostgreSqlStorageOptions options)
        {
            try
            {
                using (var transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead))
                {
                    int affected = -1;

                    affected = connection.Execute($@"DELETE FROM ""{options.SchemaName}"".""lock"" WHERE ""resource"" = @resource AND ""acquired"" < @timeout",
                        new
                        {
                            resource = resource,
                            timeout = DateTime.UtcNow - options.DistributedLockTimeout
                        });

                    transaction.Commit();
                }
            }
            catch
            {
            }
        }

        private static void PostgreSqlDistributedLock_Init_UpdateCount(string resource, TimeSpan timeout, IDbConnection connection, PostgreSqlStorageOptions options)
        {
            var lockAcquiringTime = Stopwatch.StartNew();

            bool tryAcquireLock = true;

            while (tryAcquireLock)
            {
                try
                {
                    connection.Execute($@"
INSERT INTO ""{options.SchemaName}"".""lock""(""resource"", ""updatecount"", ""acquired"") 
SELECT @resource, 0, @acquired
WHERE NOT EXISTS (
    SELECT 1 FROM ""{options.SchemaName}"".""lock"" 
    WHERE ""resource"" = @resource
);
", new
                    {
                        resource = resource,
                        acquired = DateTime.UtcNow
                    });
                }
                catch (Exception)
                {
                }

                int rowsAffected = connection.Execute(
                    $@"UPDATE ""{options.SchemaName}"".""lock"" SET ""updatecount"" = 1 WHERE ""updatecount"" = 0 AND ""resource"" = @resource",
                    new { resource });

                if (rowsAffected > 0) return;

                if (lockAcquiringTime.ElapsedMilliseconds > timeout.TotalMilliseconds)
                    tryAcquireLock = false;
                else
                {
                    int sleepDuration = (int)(timeout.TotalMilliseconds - lockAcquiringTime.ElapsedMilliseconds);
                    if (sleepDuration > 1000) sleepDuration = 1000;
                    if (sleepDuration > 0)
                        Thread.Sleep(sleepDuration);
                    else
                        tryAcquireLock = false;
                }
            }

            throw new PostgreSqlDistributedLockException(
                $"Could not place a lock on the resource '{resource}': Lock timeout.");
        }

        public void Dispose()
        {
            if (_completed) return;

            _completed = true;

            int rowsAffected = _connection.Execute($@"
DELETE FROM ""{_options.SchemaName}"".""lock"" 
WHERE ""resource"" = @resource;
",
            new
            {
                resource = _resource
            });


            if (rowsAffected <= 0)
            {
                throw new PostgreSqlDistributedLockException(
                    $"Could not release a lock on the resource '{_resource}'. Lock does not exists.");
            }
        }
    }
}
