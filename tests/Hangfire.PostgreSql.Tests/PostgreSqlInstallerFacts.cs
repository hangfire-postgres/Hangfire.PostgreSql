using System;
using System.Linq;
using Dapper;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
	public class PostgreSqlInstallerFacts
	{
		[Fact]
		public void InstallingSchemaUpdatesVersionAndShouldNotThrowAnException()
		{
			var ex = Record.Exception(() =>
			{
				UseConnection(connection =>
				{
					string schemaName = "hangfire_tests_" + Guid.NewGuid().ToString().Replace("-", "_").ToLower();

					PostgreSqlObjectsInstaller.Install(connection, schemaName);

					var lastVersion = connection.Query<int>(@"select version from """ + schemaName + @""".""schema""").Single();
					Assert.Equal(12, lastVersion);

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