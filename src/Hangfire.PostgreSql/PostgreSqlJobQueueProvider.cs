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

namespace Hangfire.PostgreSql
{
#if (NETCORE1 || NETCORE50 || NETSTANDARD1_5 || NETSTANDARD1_6)
	public
#else
	internal
#endif
	class PostgreSqlJobQueueProvider : IPersistentJobQueueProvider
    {
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlJobQueueProvider(PostgreSqlStorageOptions options)
        {
            if (options == null) throw new ArgumentNullException("options");
            _options = options;
        }

        public PostgreSqlStorageOptions Options { get { return _options; } }

        public IPersistentJobQueue GetJobQueue(IDbConnection connection)
        {
            return new PostgreSqlJobQueue(connection, _options);
        }

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(IDbConnection connection)
        {
            return new PostgreSqlJobQueueMonitoringApi(connection, _options);
        }
    }
}