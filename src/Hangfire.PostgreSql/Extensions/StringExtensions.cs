using Hangfire.PostgreSql.Utils;

namespace Hangfire.PostgreSql.Extensions
{
  public static class StringExtensions
  {
    public static string GetProperDbObjectName(this string val)
    {
      return DbQueryHelper.IsUpperCase ? val?.ToUpperInvariant() : val;
    }
  }
}
