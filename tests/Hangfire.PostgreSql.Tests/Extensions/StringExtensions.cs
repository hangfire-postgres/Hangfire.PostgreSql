using Hangfire.PostgreSql.Tests.Utils;

namespace Hangfire.PostgreSql.Tests.Extensions
{
  public static class StringExtensions
  {
    public static string GetProperDbObjectName(this string val)
    {
      return DbQueryHelper.IsUpperCase ? val?.ToUpperInvariant() : val;
    }
  }
}
