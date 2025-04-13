namespace Hangfire.PostgreSql.Tests.Entities
{
  public record TestJob(long Id, string InvocationData, string Arguments, DateTime? ExpireAt, string StateName, long? StateId, DateTime CreatedAt);

  public class TestJobs
  {
    public void Run(string logMessage)
    {
      Console.WriteLine("Running test job: {0}", logMessage);
    }
  }
}
