using System;
using System.Data;
using System.Globalization;
using Dapper;

namespace Hangfire.PostgreSql.Tests.Utils
{
  public static class Helper
  {
    public static TestJob GetTestJob(IDbConnection connection, string schemaName, string jobId, bool isUpperCase = false)
    {
      if (isUpperCase)
      {
        schemaName = schemaName?.ToUpper();
        return connection
          .QuerySingle<TestJob>($@"SELECT ""ID"" ""ID"", ""INVOCATIONDATA"" ""INVOCATIONDATA"", 
                                          ""ARGUMENTS"" ""ARGUMENTS"", ""EXPIREAT"" ""EXPIREAT"", 
                                          ""STATENAME"" ""STATENAME"", ""STATEID"" ""STATEID"", 
                                          ""CREATEDAT"" ""CREATEDAT"" 
                              FROM ""{schemaName}"".""JOB"" WHERE ""ID"" = @Id OR @Id = -1", new { Id = Convert.ToInt64(jobId, CultureInfo.InvariantCulture) });
      }
      else
      {


        return connection
          .QuerySingle<TestJob>($@"SELECT ""id"" ""Id"", ""invocationdata"" ""InvocationData"", 
                                          ""arguments"" ""Arguments"", ""expireat"" ""ExpireAt"", 
                                          ""statename"" ""StateName"", ""stateid"" ""StateId"", 
                                          ""createdat"" ""CreatedAt"" 
                              FROM ""{schemaName}"".""job"" WHERE ""id"" = @Id OR @Id = -1", new { Id = Convert.ToInt64(jobId, CultureInfo.InvariantCulture) });
      }
    }
  }
}
