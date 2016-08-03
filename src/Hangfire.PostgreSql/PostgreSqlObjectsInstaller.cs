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
using Hangfire.Logging;
using Npgsql;

namespace Hangfire.PostgreSql
{
	[ExcludeFromCodeCoverage]
	internal class PostgreSqlObjectsInstaller
	{
		private readonly ILog _log = LogProvider.GetLogger(typeof(PostgreSqlStorage));
		private NpgsqlConnection _connection = null;
		private string _schemaName = null;

		public PostgreSqlObjectsInstaller(NpgsqlConnection connection, string schemaName = "hangfire")
		{
			_connection = connection;
			_schemaName = schemaName;
		}

		public void Install()
		{
			if (_connection == null) throw new ArgumentNullException(nameof(_connection));

			_log.Info("Start installing Hangfire SQL objects...");

			// version 3 to keep in check with Hangfire SqlServer, but I couldn't keep up with that idea after all;
			int version = 3;
			int previousVersion = 1;
			bool scriptFound = true;

			do
			{
				try
				{
					var script = GetStringResource(
						typeof(PostgreSqlObjectsInstaller).Assembly, 
						$"Hangfire.PostgreSql.Install.v{version.ToString(CultureInfo.InvariantCulture)}.sql");
					if (_schemaName != "hangfire")
					{
						script = script.Replace("'hangfire'", $"'{_schemaName}'").Replace(@"""hangfire""", $@"""{_schemaName}""");
					}

					using (var transaction = _connection.BeginTransaction(IsolationLevel.Serializable))
					using (var command = new NpgsqlCommand(script, _connection, transaction))
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
								"SELECT @version \"version\" WHERE NOT EXISTS (SELECT @previousVersion FROM \"{0}\".\"schema\")", _schemaName);
							using (var versionCommand = new NpgsqlCommand(bumpVersionSql, _connection, transaction))
							{
								versionCommand.Parameters.AddWithValue("version", version);
								versionCommand.Parameters.AddWithValue("previousVersion", version);
								versionCommand.ExecuteNonQuery();
							}
							transaction.Commit();
						}
						catch (PostgresException ex)
						{
							if (!(ex.MessageText ?? "").Equals("version-already-applied"))
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
						_log.ErrorException("Error while executing install/upgrade", ex);
					}

					scriptFound = false;
				}

				previousVersion = version;
				version++;
			} while (scriptFound);

			_log.Info("Hangfire SQL objects installed.");
		}

		private string GetStringResource(Assembly assembly, string resourceName)
		{
			using (var stream = assembly.GetManifestResourceStream(resourceName))
			{
				if (stream == null)
				{
					throw new InvalidOperationException($"Requested resource `{resourceName}` was not found in the assembly `{assembly}`.");
				}

				using (var reader = new StreamReader(stream))
				{
					return reader.ReadToEnd();
				}
			}
		}
	}
}