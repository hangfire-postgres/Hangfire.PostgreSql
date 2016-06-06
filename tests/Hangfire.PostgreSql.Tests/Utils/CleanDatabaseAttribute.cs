using System;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading;
using Dapper;
using Npgsql;
using Xunit.Sdk;

namespace Hangfire.PostgreSql.Tests
{
	public class CleanDatabaseAttribute : BeforeAfterTestAttribute
	{
		private static readonly object GlobalLock = new object();
		private static bool _sqlObjectInstalled;

		public CleanDatabaseAttribute()
		{
		}

		public override void Before(MethodInfo methodUnderTest)
		{
			Monitor.Enter(GlobalLock);

			if (!_sqlObjectInstalled)
			{
				RecreateSchemaAndInstallObjects();
				_sqlObjectInstalled = true;
			}
			CleanTables();
		}

		public override void After(MethodInfo methodUnderTest)
		{
			try
			{
			}
			finally
			{
				Monitor.Exit(GlobalLock);
			}
		}

		private static void RecreateSchemaAndInstallObjects()
		{
			using (var connection = new NpgsqlConnection(
				ConnectionUtils.GetMasterConnectionString()))
			{
				bool databaseExists = connection.Query<bool?>(
					@"select true :: boolean from pg_database where datname = @databaseName;",
					new
					{
						databaseName = ConnectionUtils.GetDatabaseName()
					}
					).SingleOrDefault() ?? false;

				if (!databaseExists)
				{
					connection.Execute($@"CREATE DATABASE ""{ConnectionUtils.GetDatabaseName()}""");
				}
			}

			using (var connection = new NpgsqlConnection(ConnectionUtils.GetConnectionString()))
			{
				if (connection.State == ConnectionState.Closed)
				{
					connection.Open();
				}

				PostgreSqlObjectsInstaller.Install(connection);
				PostgreSqlTestObjectsInitializer.CleanTables(connection);
			}
		}

		private static void CleanTables()
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				PostgreSqlTestObjectsInitializer.CleanTables(connection);
			}
		}
	}
}