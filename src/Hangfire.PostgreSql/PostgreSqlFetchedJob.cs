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
using Dapper;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
#if (NETSTANDARD2_0)
    public
#else
	internal
#endif
    class PostgreSqlFetchedJob : IFetchedJob
    {
        private readonly IDbConnection _connection;
        private readonly PostgreSqlStorageOptions _options;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        public PostgreSqlFetchedJob(
            IDbConnection connection, 
            PostgreSqlStorageOptions options,
            long id, 
            string jobId, 
            string queue)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            if (jobId == null) throw new ArgumentNullException("jobId");
            if (queue == null) throw new ArgumentNullException("queue");
            if (options == null) throw new ArgumentNullException("options");

            _connection = connection;
            _options = options;

            Id = id;
            JobId = jobId;
            Queue = queue;
        }

        public long Id { get; private set; }
        public string JobId { get; private set; }
        public string Queue { get; private set; }

        public void RemoveFromQueue()
        {
            _connection.Execute(
                @"
DELETE FROM """ + _options.SchemaName + @""".""jobqueue"" 
WHERE ""id"" = @id;
",
                new { id = Id });

            _removedFromQueue = true;
        }

        public void Requeue()
        {
            _connection.Execute(
                @"
UPDATE """ + _options.SchemaName + @""".""jobqueue"" 
SET ""fetchedat"" = NULL 
WHERE ""id"" = @id;
",
                new { id = Id });

            _requeued = true;
        }

        public void Dispose()
        {
            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _disposed = true;
        }
    }
}
