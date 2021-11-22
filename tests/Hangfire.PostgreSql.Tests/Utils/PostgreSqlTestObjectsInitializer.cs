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
using System.IO;
using System.Reflection;
using Npgsql;

namespace Hangfire.PostgreSql.Tests.Utils
{
  internal static class PostgreSqlTestObjectsInitializer
  {
    public static void CleanTables(NpgsqlConnection connection)
    {
      if (connection == null) throw new ArgumentNullException(nameof(connection));

      string script = null;
      script = GetStringResource(typeof(PostgreSqlTestObjectsInitializer).GetTypeInfo().Assembly,
        "Hangfire.PostgreSql.Tests.Scripts.Clean.sql").Replace("'hangfire'", $"'{ConnectionUtils.GetSchemaName()}'");

      using NpgsqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);
      using NpgsqlCommand command = new(script, connection, transaction);
      command.CommandTimeout = 120;
      command.ExecuteNonQuery();
      transaction.Commit();
    }

    private static string GetStringResource(Assembly assembly, string resourceName)
    {
      using Stream stream = assembly.GetManifestResourceStream(resourceName);
      if (stream == null)
      {
        throw new InvalidOperationException($"Requested resource '{resourceName}' was not found in the assembly '{assembly}'.");
      }

      using StreamReader reader = new(stream);
      return reader.ReadToEnd();
    }
  }
}
