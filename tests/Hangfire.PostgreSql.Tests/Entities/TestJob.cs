using System;

namespace Hangfire.PostgreSql.Tests
{
  public record TestJob(long Id, string InvocationData, string Arguments, DateTime? ExpireAt, string StateName, long? StateId, DateTime CreatedAt);
}
