using System;
using Dapper;
using Hangfire.Logging;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
	public class PostgreSqlInstallerFacts
	{
		[Fact]
		public void InstallingSchema_ShouldNotThrowAnException()
		{
			var ex = Record.Exception(() =>
			{
				UseConnection(connection =>
				{
					string schemaName = "hangfire_tests_" + System.Guid.NewGuid().ToString().Replace("-", "_").ToLower();

					var installer = new PostgreSqlObjectsInstaller(connection, schemaName);
					installer.Install();

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