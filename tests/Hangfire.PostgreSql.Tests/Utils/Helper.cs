using System;
using System.Data;
using System.Globalization;
using Dapper;
using Hangfire.PostgreSql.Tests.Entities;

namespace Hangfire.PostgreSql.Tests.Utils
{
  public static class Helper
  {
    public static TestJob GetTestJob(IDbConnection connection, string schemaName, string jobId, bool usePrefix = false, string prefix = "")
    {
      string tableName = usePrefix ? prefix + "job" : "job";
      return connection
        .QuerySingle<TestJob>($@"SELECT ""id"" ""Id"", ""invocationdata"" ""InvocationData"", ""arguments"" ""Arguments"", ""expireat"" ""ExpireAt"", ""statename"" ""StateName"", ""stateid"" ""StateId"", ""createdat"" ""CreatedAt"" FROM ""{schemaName}"".""{tableName}"" WHERE ""id"" = @Id OR @Id = -1",
          new { Id = Convert.ToInt64(jobId, CultureInfo.InvariantCulture) });
    }

    public static void DoTableRename(IDbConnection connection, string schemaName, bool useTablePrefix, string newPrefix)
    {
      if (!useTablePrefix)
      {
        return;
      }

      var hangfireTableNames = new[]
        {
            "job", "state", "counter", "queue", "schema", "server", "set", "list", "hash", "lock", "aggregatedcounter", "jobparameter", "jobqueue"
        };
      foreach (string tableName in hangfireTableNames)
      {
        string oldTableName = tableName;
        string newTableName = newPrefix + tableName;
        connection.Execute($@"ALTER TABLE ""{schemaName}"".""{oldTableName}"" RENAME TO ""{newTableName}"";");
      }
    }

  }
}
