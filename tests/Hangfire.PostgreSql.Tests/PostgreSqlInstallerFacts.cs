using System;
using System.Linq;
using Dapper;
using Hangfire.PostgreSql.Tests.Utils;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
  public class PostgreSqlInstallerFacts
  {
    [Fact]
    public void InstallingSchemaUpdatesVersionAndShouldNotThrowAnException()
    {
      Exception ex = Record.Exception(() => {
        UseConnection(connection => {
          string schemaName = "hangfire_tests_" + Guid.NewGuid().ToString().Replace("-", "_").ToLower();

          PostgreSqlObjectsInstaller.Install(connection, schemaName);

          int lastVersion = connection.Query<int>($@"SELECT version FROM ""{schemaName}"".""schema""").Single();
          Assert.Equal(18, lastVersion);

          connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
        });
      });

      Assert.Null(ex);
    }

    [Fact]
    public void InstallingSchemaWithCapitalsUpdatesVersionAndShouldNotThrowAnException()
    {
      Exception ex = Record.Exception(() => {
        UseConnection(connection => {
          string schemaName = "Hangfire_Tests_" + Guid.NewGuid().ToString().Replace("-", "_").ToLower();

          PostgreSqlObjectsInstaller.Install(connection, schemaName);

          int lastVersion = connection.Query<int>($@"SELECT version FROM ""{schemaName}"".""schema""").Single();
          Assert.Equal(18, lastVersion);

          connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
        });
      });

      Assert.Null(ex);
    }

    private static void UseConnection(Action<NpgsqlConnection> action)
    {
      using (NpgsqlConnection connection = ConnectionUtils.CreateConnection())
      {
        action(connection);
      }
    }
  }
}
