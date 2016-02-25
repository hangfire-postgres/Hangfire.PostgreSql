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
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Reflection;
using Dapper;
using Hangfire.Logging;
using Npgsql;

namespace Hangfire.PostgreSql
{
    [ExcludeFromCodeCoverage]
    internal static class PostgreSqlObjectsInstaller
    {
        private static readonly ILog Log = LogProvider.GetLogger(typeof(PostgreSqlStorage));

        public static void Install(NpgsqlConnection connection, string schemaName = "hangfire")
        {
            if (connection == null) throw new ArgumentNullException("connection");

            Log.Info("Start installing Hangfire SQL objects...");

            // version 3 to keep in check with Hangfire SqlServer, but I couldn't keep up with that idea after all;
            int version = 3;
            int previousVersion = 1;
            bool scriptFound = true;

            do
            {
                try
                {
                    var script = GetStringResource(
                        typeof (PostgreSqlObjectsInstaller).Assembly,
                        string.Format("Hangfire.PostgreSql.Install.v{0}.sql",
                            version.ToString(CultureInfo.InvariantCulture)));
                    if (schemaName != "hangfire")
                    {
                        script = script.Replace("'hangfire'", string.Format("'{0}'", schemaName))
                            .Replace(@"""hangfire""", string.Format(@"""{0}""", schemaName));
                    }

                    using (var transaction = connection.BeginTransaction(IsolationLevel.Serializable))
                    using (var command = new NpgsqlCommand(script, connection, transaction))
                    {
                        command.CommandTimeout = 120;
                        try
                        {
                            command.ExecuteNonQuery();

                            // Due to https://github.com/npgsql/npgsql/issues/641 , it's not possible to send
                            // CREATE objects and use the same object in the same command
                            // So bump the version in another command
                            var bumpVersionSql = string.Format(
                                "INSERT INTO \"{0}\".\"schema\"(\"version\") " +
                                "SELECT @version \"version\" WHERE NOT EXISTS (SELECT @previousVersion FROM \"{0}\".\"schema\")", schemaName);
                            using (var versionCommand = new NpgsqlCommand(bumpVersionSql, connection, transaction))
                            {
                                versionCommand.Parameters.AddWithValue("version", version);
                                versionCommand.Parameters.AddWithValue("previousVersion", version);
                                versionCommand.ExecuteNonQuery();
                            }
                            transaction.Commit();
                        }
                        catch (NpgsqlException ex)
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
					if(ex.Source.Equals("Npgsql"))
					{
						Log.ErrorException("Error while executing install/upgrade", ex);
					}

                    scriptFound = false;
                }

                previousVersion = version;
                version++;
            } while (scriptFound);

            Log.Info("Hangfire SQL objects installed.");
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
