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
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.PostgreSql
{
    internal class ExpirationManager : IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private static readonly string[] ProcessedTables =
        {
            "Counter",
            "Job",
            "List",
            "Set",
            "Hash",
        };

        private readonly PostgreSqlStorage _storage;
        private readonly TimeSpan _checkInterval;

        public ExpirationManager(PostgreSqlStorage storage)
            : this(storage, TimeSpan.FromHours(1))
        {
        }

        public ExpirationManager(PostgreSqlStorage storage, TimeSpan checkInterval)
        {
            if (storage == null) throw new ArgumentNullException("storage");

            _storage = storage;
            _checkInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            using (var connection = _storage.CreateAndOpenConnection())
            {
                foreach (var table in ProcessedTables)
                {
                    Logger.DebugFormat("Removing outdated records from table '{0}'...", table);

                    using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted)) { 
                        int rowsAffected = connection.Execute(
                            String.Format(@"delete from ""hangfire"".""{0}"" where ""expireat"" < now() at time zone 'utc';", table.ToLower()), transaction);
                        transaction.Commit();
                        Logger.DebugFormat("Removed {0} outdated records from table '{1}'...", rowsAffected, table);
                    }


                }
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        public override string ToString()
        {
            return "SQL Records Expiration Manager";
        }
    }
}
