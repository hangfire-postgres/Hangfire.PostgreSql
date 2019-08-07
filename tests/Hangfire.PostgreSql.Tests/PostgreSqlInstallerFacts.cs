using System;
using Dapper;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
	public class PostgreSqlInstallerFacts
	{
        [Fact, CleanDatabase]
		public void InstallingSchemaShouldNotThrowAnException()
		{
			var ex = Record.Exception(() =>
			{
				UseConnection(connection =>
				{
                    //Remove default schema to avoid false green test
                    connection.Execute($@"DROP SCHEMA ""hangfire"" CASCADE;");

                    string schemaName = "hangfire_tests_" + Guid.NewGuid().ToString().Replace("-", "_").ToLower();

					PostgreSqlObjectsInstaller.Install(connection, schemaName);

					connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
				});
			});

			Assert.Null(ex);
		}


		private static void UseConnection(Action<NpgsqlConnection> action)
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				action(connection);
			}
		}
	}
}