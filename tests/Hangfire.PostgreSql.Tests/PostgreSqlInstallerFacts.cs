using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Hangfire.PostgreSql.Tests.Utils;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
  public class PostgreSqlInstallerFacts
  {
    [Fact]
    public void InstallingSchemaConcurrentlyShouldNotThrowAnException()
    {
      const int installerCount = 8;
      string schemaName = "hangfire_tests_" + Guid.NewGuid().ToString().Replace("-", "_").ToLower(CultureInfo.InvariantCulture);
      using Barrier barrier = new(installerCount);
      Task<Exception>[] installers = new Task<Exception>[installerCount];

      try
      {
        for (int i = 0; i < installerCount; i++)
        {
          installers[i] = Task.Factory.StartNew(() => Record.Exception(() => {
            using NpgsqlConnection connection = ConnectionUtils.CreateConnection();
            Assert.True(barrier.SignalAndWait(TimeSpan.FromSeconds(30)));
            PostgreSqlObjectsInstaller.Install(connection, schemaName);
          }), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        Task.WaitAll(installers);
        Assert.All(installers, installer => Assert.Null(installer.Result));

        UseConnection(connection => {
          int lastVersion = connection.QuerySingle<int>($@"SELECT version FROM ""{schemaName}"".""schema""");
          Assert.Equal(23, lastVersion);
        });
      }
      finally
      {
        UseConnection(connection => connection.Execute($@"DROP SCHEMA IF EXISTS ""{schemaName}"" CASCADE;"));
      }
    }

    [Fact]
    public void InstallingSchemaUpdatesVersionAndShouldNotThrowAnException()
    {
      Exception ex = Record.Exception(() => {
        UseConnection(connection => {
          string schemaName = "hangfire_tests_" + Guid.NewGuid().ToString().Replace("-", "_").ToLower(CultureInfo.InvariantCulture);

          PostgreSqlObjectsInstaller.Install(connection, schemaName);

          int lastVersion = connection.QuerySingle<int>($@"SELECT version FROM ""{schemaName}"".""schema""");
          Assert.Equal(23, lastVersion);

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
          string schemaName = "Hangfire_Tests_" + Guid.NewGuid().ToString().Replace("-", "_").ToLower(CultureInfo.InvariantCulture);

          PostgreSqlObjectsInstaller.Install(connection, schemaName);

          int lastVersion = connection.QuerySingle<int>($@"SELECT version FROM ""{schemaName}"".""schema""");
          Assert.Equal(23, lastVersion);

          connection.Execute($@"DROP SCHEMA ""{schemaName}"" CASCADE;");
        });
      });

      Assert.Null(ex);
    }

    private static void UseConnection(Action<NpgsqlConnection> action)
    {
      using NpgsqlConnection connection = ConnectionUtils.CreateConnection();
      action(connection);
    }
  }
}
