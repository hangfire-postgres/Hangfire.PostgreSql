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
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Reflection;
using Dapper;
using Npgsql;
using System.Data;

namespace Hangfire.PostgreSql.Tests
{
    internal static class PostgreSqlTestObjectsInitializer
    {
        public static void CleanTables(NpgsqlConnection connection)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));

			string script = null;


               script = GetStringResource(
              typeof(PostgreSqlTestObjectsInitializer).GetTypeInfo().Assembly,
                "Hangfire.PostgreSql.Tests.Clean.sql").Replace("'hangfire'", string.Format("'{0}'", ConnectionUtils.GetSchemaName()));

			//connection.Execute(script);

			using (var transaction = connection.BeginTransaction(IsolationLevel.Serializable))
			using (var command = new NpgsqlCommand(script, connection, transaction))
			{
				command.CommandTimeout = 120;
				try
				{
					command.ExecuteNonQuery();
					transaction.Commit();
				}
				catch (NpgsqlException)
				{
					throw;
				}
			}
		}

        private static string GetStringResource(Assembly assembly, string resourceName)
        {
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null) 
                {
                    throw new InvalidOperationException(String.Format(
                        "Requested resource `{0}` was not found in the assembly `{1}`.",
                        resourceName,
                        assembly));
                }

                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }

    }
}
