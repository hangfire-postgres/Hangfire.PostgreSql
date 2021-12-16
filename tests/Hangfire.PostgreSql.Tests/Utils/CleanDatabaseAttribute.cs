﻿using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading;
using Dapper;
using Npgsql;
using Xunit.Sdk;

namespace Hangfire.PostgreSql.Tests.Utils
{
  public class CleanDatabaseAttribute : BeforeAfterTestAttribute
  {
    private static readonly object _globalLock = new();
    private static bool _sqlObjectInstalled;
   
    // Change here if you want to upper case object test !!!
    public CleanDatabaseAttribute() => DbQueryHelper.IsUpperCase = true;
   
    public override void Before(MethodInfo methodUnderTest)
    {
      Monitor.Enter(_globalLock);

      if (!_sqlObjectInstalled)
      {
        RecreateSchemaAndInstallObjects();
        _sqlObjectInstalled = true;
      }

      CleanTables();
    }

    public override void After(MethodInfo methodUnderTest)
    {
      try { }
      finally
      {
        Monitor.Exit(_globalLock);
      }
    }

    private static void RecreateSchemaAndInstallObjects()
    {
      using NpgsqlConnection masterConnection = ConnectionUtils.CreateMasterConnection();
      bool databaseExists = masterConnection.QuerySingleOrDefault<bool?>($@"SELECT true :: boolean FROM pg_database WHERE datname = @DatabaseName;",
        new
        {
          DatabaseName = ConnectionUtils.GetDatabaseName(),
        }) ?? false;

      if (!databaseExists)
      {
        masterConnection.Execute($@"CREATE DATABASE ""{ConnectionUtils.GetDatabaseName()}""");
      }

      using NpgsqlConnection connection = ConnectionUtils.CreateConnection();
      if (connection.State == ConnectionState.Closed)
      {
        connection.Open();
      }

      PostgreSql.Utils.DbQueryHelper.IsUpperCase = DbQueryHelper.IsUpperCase;
      PostgreSqlObjectsInstaller.Install(connection);
      PostgreSqlTestObjectsInitializer.CleanTables(connection, DbQueryHelper.IsUpperCase);
    }

    private static void CleanTables()
    {
      using NpgsqlConnection connection = ConnectionUtils.CreateConnection();
      PostgreSqlTestObjectsInitializer.CleanTables(connection, DbQueryHelper.IsUpperCase);
    }
  }
}
