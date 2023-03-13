using System;
using Microsoft.Extensions.Caching.Memory;

namespace Hangfire.PostgreSql;

internal sealed class QueryProvider : IDisposable
{
  private readonly IMemoryCache _cache;

  public QueryProvider()
  {
    _cache = new MemoryCache(new MemoryCacheOptions());
  }

  public string GetQuery(string name, Func<string> queryFactory)
  {
    return _cache.GetOrCreate($"postgresql-query:{name}", entry => {
      entry.SlidingExpiration = TimeSpan.FromMinutes(30);

      string query = queryFactory();
      entry.Value = query;
      return query;
    });
  }

  public void Dispose()
  {
    _cache?.Dispose();
    Instance = null;
  }

  public static QueryProvider Instance { get; private set; } = new();
}
