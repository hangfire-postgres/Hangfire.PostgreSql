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
using System.IO;
using System.Reflection;
using System.Resources;
using Hangfire.Logging;
using Npgsql;

namespace Hangfire.PostgreSql
{
  public static class PostgreSqlObjectsInstaller
  {
    private static readonly ILog _logger = LogProvider.GetLogger(typeof(PostgreSqlStorage));

    public static void Install(NpgsqlConnection connection, string schemaName = "hangfire")
    {
      if (connection == null)
      {
        throw new ArgumentNullException(nameof(connection));
      }

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
            script = GetStringResource(typeof(PostgreSqlObjectsInstaller).GetTypeInfo().Assembly,
              $"Hangfire.PostgreSql.Scripts.Install.v{version.ToString(CultureInfo.InvariantCulture)}.sql");
          }
          catch (MissingManifestResourceException)
          {
            break;
          }

          if (schemaName != "hangfire")
          {
            script = script.Replace("'hangfire'", $"'{schemaName}'").Replace(@"""hangfire""", $@"""{schemaName}""");
          }

          if (!VersionAlreadyApplied(connection, schemaName, version))
          {
            string commandText = $@"{script}; UPDATE ""{schemaName}"".""schema"" SET ""version"" = @Version WHERE ""version"" = @PreviousVersion";
            using NpgsqlTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);
            using NpgsqlCommand command = new(commandText, connection, transaction);
            command.CommandTimeout = 120;
            command.Parameters.Add(new NpgsqlParameter("Version", version));
            command.Parameters.Add(new NpgsqlParameter("PreviousVersion", previousVersion));
            try
            {
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
        catch (Exception ex)
        {
          if (ex.Source.Equals("Npgsql"))
          {
            _logger.ErrorException("Error while executing install/upgrade", ex);
          }

          throw;
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
        using NpgsqlCommand command = new($@"SELECT true ""VersionAlreadyApplied"" FROM ""{schemaName}"".""schema"" WHERE ""version"" >= $1", connection);
        command.Parameters.Add(new NpgsqlParameter { Value = version });
        object result = command.ExecuteScalar();
        if (true.Equals(result))
        {
          return true;
        }
      }
      catch (PostgresException ex)
      {
        if (ex.SqlState.Equals(PostgresErrorCodes.UndefinedTable)) //42P01: Relation (table) does not exist. So no schema table yet.
        {
          return false;
        }

        throw;
      }

      return false;
    }

    private static string GetStringResource(Assembly assembly, string resourceName)
    {
      using Stream stream = assembly.GetManifestResourceStream(resourceName);
      if (stream == null)
      {
        throw new MissingManifestResourceException($"Requested resource `{resourceName}` was not found in the assembly `{assembly}`.");
      }

      using StreamReader reader = new(stream);
      return reader.ReadToEnd();
    }
  }
}
