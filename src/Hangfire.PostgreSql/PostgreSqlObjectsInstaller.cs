﻿// This file is part of Hangfire.PostgreSql.
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
using System.IO;
using System.Reflection;
using System.Resources;
using Hangfire.Logging;
using Hangfire.PostgreSql.Extensions;
using Hangfire.PostgreSql.Utils;
using Npgsql;

namespace Hangfire.PostgreSql
{
  public static class PostgreSqlObjectsInstaller
  {
    private static readonly ILog _logger = LogProvider.GetLogger(typeof(PostgreSqlStorage));

    public static void Install(NpgsqlConnection connection, string schemaName = "hangfire")
    {
      if (connection == null)
        throw new ArgumentNullException(nameof(connection));

      if (DbQueryHelper.IsUpperCase)
        schemaName = schemaName?.ToUpperInvariant();

      _logger.Info("Start installing Hangfire SQL objects...");

      // starts with version 3 to keep in check with Hangfire SqlServer, but I couldn't keep up with that idea after all;
      int version = 3;
      int previousVersion = 1;
      do
      {
        try
        {
          string script;
          try
          {
            var scriptPostfix = DbQueryHelper.IsUpperCase ? "_UpperCase.sql" : ".sql";
            script = GetStringResource(typeof(PostgreSqlObjectsInstaller).GetTypeInfo().Assembly,
              $"Hangfire.PostgreSql.Scripts.Install.v{version.ToString(CultureInfo.InvariantCulture)}{scriptPostfix}");
          }
          catch (MissingManifestResourceException)
          {
            break;
          }

          if (schemaName != "hangfire")
          {
            script = script.Replace("'hangfire'", $"'{schemaName.GetProperDbObjectName()}'").Replace(@"""hangfire""", $@"""{schemaName.GetProperDbObjectName()}""");
          }

          if (!VersionAlreadyApplied(connection, schemaName, version))
          {
            using (NpgsqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable))
            {
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
              using (NpgsqlCommand command = new NpgsqlCommand(script, connection, transaction))
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
              {
                command.CommandTimeout = 120;
                try
                {
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                  command.CommandText += $@"; UPDATE ""{schemaName.GetProperDbObjectName()}"".""{"schema".GetProperDbObjectName()}"" SET ""{"version".GetProperDbObjectName()}"" = @Version WHERE ""{"version".GetProperDbObjectName()}"" = @PreviousVersion";
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
                  command.Parameters.AddWithValue("Version", version);
                  command.Parameters.AddWithValue("PreviousVersion", previousVersion);

                  command.ExecuteNonQuery();

                  transaction.Commit();
                }
                catch (PostgresException ex)
                {
                  if ((ex.MessageText ?? "") != "version-already-applied")
                  {
                    throw;
                  }
                }
              }
            }
          }
        }
        catch (Exception ex)
        {
          if (ex.Source.Equals("Npgsql"))
          {
            _logger.ErrorException("Error while executing install/upgrade", ex);
          }
        }

        previousVersion = version;
        version++;
      } while (true);

      _logger.Info("Hangfire SQL objects installed.");
    }

    private static bool VersionAlreadyApplied(NpgsqlConnection connection, string schemaName, int version)
    {
      try
      {
        using (NpgsqlCommand command =
          new NpgsqlCommand($@"SELECT true :: boolean ""VersionAlreadyApplied"" FROM ""{schemaName.GetProperDbObjectName()}"".""{"schema".GetProperDbObjectName()}"" WHERE ""{"version".GetProperDbObjectName()}""::integer >= @Version",
            connection))
        {
          command.Parameters.AddWithValue("Version", version);
          object result = command.ExecuteScalar();
          if (true.Equals(result)) return true;
        }
      }
      catch (PostgresException ex)
      {
        if (ex.SqlState.Equals(PostgresErrorCodes.UndefinedTable)) //42P01: Relation (table) does not exist. So no schema table yet.
          return false;

        throw;
      }

      return false;
    }

    private static string GetStringResource(Assembly assembly, string resourceName)
    {
      using (Stream stream = assembly.GetManifestResourceStream(resourceName))
      {
        if (stream == null)
          throw new MissingManifestResourceException($"Requested resource `{resourceName}` was not found in the assembly `{assembly}`.");

        using (StreamReader reader = new StreamReader(stream))
        {
          return reader.ReadToEnd();
        }
      }
    }
  }
}
