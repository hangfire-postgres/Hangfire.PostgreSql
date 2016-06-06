using System;
using Dapper;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
	public class PostgreSqlInstallerFacts
	{
		[Fact]
		public void InstallingSchemaShouldNotThrowAnException()
		{
			var ex = Record.Exception(() =>
			{
				UseConnection(connection =>
				{
					string schemaName = "hangfire_tests_" + System.Guid.NewGuid().ToString().Replace("-", "_").ToLower();

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