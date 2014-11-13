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
    internal class PostgreSqlDistributedLock : IDisposable
    {
        private readonly IDbConnection _connection;
        private readonly string _resource;

        private bool _completed;

        public PostgreSqlDistributedLock(string resource, TimeSpan timeout, IDbConnection connection)
        {
            if (String.IsNullOrEmpty(resource)) throw new ArgumentNullException("resource");
            if (connection == null) throw new ArgumentNullException("connection");

            _resource = resource;
            _connection = connection;

            Stopwatch lockAcquiringTime = new Stopwatch();
            lockAcquiringTime.Start();

            bool tryAcquireLock = true;

            while (tryAcquireLock)
            {
                try
                {
                    int rowsAffected = -1;
                    using (var trx = _connection.BeginTransaction(IsolationLevel.RepeatableRead))
                    {
                        rowsAffected = _connection.Execute(@"
INSERT INTO ""hangfire"".""lock""(""resource"") 
SELECT @resource
WHERE NOT EXISTS (
    SELECT 1 FROM ""hangfire"".""lock"" 
    WHERE ""resource"" = @resource
);
", new
                        {
                            resource = resource
                        }, trx);
                        trx.Commit();
                    }
                    if (rowsAffected > 0) return;
                }
                catch (Exception)
                {
                }

                if (lockAcquiringTime.ElapsedMilliseconds > timeout.TotalMilliseconds)
                    tryAcquireLock = false;
                else
                {
                    int sleepDuration = (int) (timeout.TotalMilliseconds - lockAcquiringTime.ElapsedMilliseconds);
                    if (sleepDuration > 1000) sleepDuration = 1000;
                    if (sleepDuration > 0)
                        Thread.Sleep(sleepDuration);
                    else
                        tryAcquireLock = false;
                }
            }

            throw new PostgreSqlDistributedLockException(
                String.Format(
                "Could not place a lock on the resource '{0}': {1}.",
                _resource,
                "Lock timeout"));
        }

        public void Dispose()
        {
            if (_completed) return;

            _completed = true;

            int rowsAffected = _connection.Execute(@"
DELETE FROM ""hangfire"".""lock"" 
WHERE ""resource"" = @resource;
", new
            {
                resource = _resource
            });


            if (rowsAffected <= 0)
            {
                throw new PostgreSqlDistributedLockException(
                    String.Format(
                        "Could not release a lock on the resource '{0}'. Lock does not exists.",
                        _resource));
            }
        }
    }
}