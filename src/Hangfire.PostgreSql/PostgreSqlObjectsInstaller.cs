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
using System.Collections.Generic;
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

    public static void Install(NpgsqlConnection connection, string schemaName = "hangfire", bool useTablePrefix = false, string tablePrefixName = "hangfire_")
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

          if (!VersionAlreadyApplied(connection, schemaName, version, useTablePrefix, tablePrefixName))
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

      if(!useTablePrefix)
      {
        _logger.Warn("Hangfire SQL objects installed.");
        return;
      }

      _logger.Info($"Try to change table name with prefix {tablePrefixName}");

      try
      {
        _logger.Info("Check if prefix change has already been recorded..");
        string prefixChangeKey = "hangfire:table_prefix_changed";
        string tableNameToCheck = tablePrefixName + "set";
        bool shouldInsertTrackingRecord = false;

        // Verifica se la tabella con prefisso esiste già
        bool tableWithPrefixExists = false;
        using (var checkTableCmd = new NpgsqlCommand(
            $@"SELECT 1 FROM information_schema.tables 
       WHERE table_schema = @SchemaName 
       AND table_name = @TableName 
       LIMIT 1;", connection))
        {
          checkTableCmd.Parameters.AddWithValue("SchemaName", schemaName);
          checkTableCmd.Parameters.AddWithValue("TableName", tableNameToCheck);
          var tableExists = checkTableCmd.ExecuteScalar();
          tableWithPrefixExists = (tableExists != null && tableExists != DBNull.Value);
        }

        if (tableWithPrefixExists)
        {
          // La tabella con prefisso esiste già, controlla se il prefisso è già stato applicato
          using (var checkCmd = new NpgsqlCommand(
              $@"SELECT 1 FROM ""{schemaName}"".""{tableNameToCheck}"" WHERE ""key"" = @Key LIMIT 1;", connection))
          {
            checkCmd.Parameters.AddWithValue("Key", prefixChangeKey);
            var exists = checkCmd.ExecuteScalar();
            if (exists != null && exists != DBNull.Value)
            {
              _logger.Warn("Table prefix has already been changed. If you want to change it again, you must drop all Hangfire tables and reinstall them.");
              return;
            }
          }
        }
        else
        {
          // La tabella con prefisso non esiste, bisognerà inserire il record dopo le rename
          shouldInsertTrackingRecord = true;
        }

        var hangfireTableNames = new[]
        {
            "job", "state", "counter", "queue", "schema", "server", "set", "list", "hash", "lock", "aggregatedcounter", "jobparameter", "jobqueue"
        };

        List<string> tablesToRename = new List<string>();
        using (var cmd = new NpgsqlCommand($@"
                  SELECT table_name
                  FROM information_schema.tables
                  WHERE table_schema = @SchemaName
                    AND table_name = ANY(@TableNames)", connection))
        {
          cmd.Parameters.AddWithValue("SchemaName", schemaName);
          cmd.Parameters.AddWithValue("TableNames", hangfireTableNames);
          using (var reader = cmd.ExecuteReader())
          {
            while (reader.Read())
            {
              tablesToRename.Add(reader.GetString(0));
            }
          }
        }

        foreach (string tableName in tablesToRename)
        {
          string newTableName = tablePrefixName + tableName;
          using (var renameCmd = new NpgsqlCommand(
              $@"ALTER TABLE ""{schemaName}"".""{tableName}"" RENAME TO ""{newTableName}"";", connection))
          {
            renameCmd.ExecuteNonQuery();
            _logger.Info($"Table {tableName} renamed to {newTableName}");
          }
        }

        if (shouldInsertTrackingRecord)
        {
          using (var insertCmd = new NpgsqlCommand(
              $@"INSERT INTO ""{schemaName}"".""{tablePrefixName + "set"}"" (""key"", ""value"", ""score"") VALUES (@Key, @Value, @Score);", connection))
          {
            insertCmd.Parameters.AddWithValue("Key", prefixChangeKey);
            insertCmd.Parameters.AddWithValue("Value", tablePrefixName);
            insertCmd.Parameters.AddWithValue("Score", 0);
            insertCmd.ExecuteNonQuery();
            _logger.Info($"Table prefix change persisted in set table with key '{prefixChangeKey}' and value '{tablePrefixName}'");
          }
        }
      }
      catch (Exception ex)
      {
        _logger.ErrorException("Error while changing table name with prefix", ex);
        throw;
      }

      _logger.Info("Hangfire SQL objects installed.");
    }

    private static bool VersionAlreadyApplied(NpgsqlConnection connection, string schemaName, int version, bool useTablePrefix, string tablePrefixName)
    {
      try
      {
        string tableName  = useTablePrefix ? (tablePrefixName + "schema") : "schema";
        using NpgsqlCommand command = new($@"SELECT true ""VersionAlreadyApplied"" FROM ""{schemaName}"".""{tableName}"" WHERE ""version"" >= $1", connection);
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
