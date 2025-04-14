// This file is part of Hangfire.PostgreSql.
// Copyright © 2014 Frank Hommers <http://hmm.rs/Hangfire.PostgreSql>.
// 
// Hangfire.PostgreSql is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.PostgreSql  is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.PostgreSql. If not, see <http://www.gnu.org/licenses/>.
//
// This work is based on the work of Sergey Odinokov, author of 
// Hangfire. <http://hangfire.io/>
//   
//    Special thanks goes to him.

using System.Reflection;

namespace Hangfire.PostgreSql;

public class SqlQueryProvider(string schemaName)
{
  private readonly string _schemaName = ProcessSchemaName(schemaName);
  private readonly Dictionary<string, string> _queryCache = new();
  private readonly Dictionary<string, MethodInfo> _methodCache = new();

  public string GetQuery(string query)
  {
    if (_queryCache.TryGetValue(query, out string result))
    {
        return result;
    }

    Func<string, string, string>? queryFunc = (Func<string, string, string>)GetQueryMethod("GetQuery").Invoke(null, null);
    return _queryCache[query] = queryFunc(_schemaName, query);
  }

  public string GetQuery(string query, params object[] args)
  {
    // No cache for queries with arguments
    Func<string, string, object[], string>? queryFunc = (Func<string, string, object[], string>)GetQueryMethod("GetQueryWithArgs").Invoke(null, null);
    return queryFunc(_schemaName, query, args);
  }

  public string GetQuery(Func<string, string> query)
  {
    return query(_schemaName);
  }

  private MethodInfo GetQueryMethod(string methodName)
  {
    if (_methodCache.TryGetValue(methodName, out MethodInfo method))
    {
      return method;
    }

    return _methodCache[methodName] = typeof(GeneratedQueries.QueryContainer).GetMethod(methodName, BindingFlags.Public | BindingFlags.Static)
      ?? throw new InvalidOperationException($"Method '{methodName}' not found in the query container.");
  }

  private static string ProcessSchemaName(string schemaName)
  {
    return string.IsNullOrEmpty(schemaName)
      ? throw new ArgumentException("Missing schema name", nameof(schemaName))
      : schemaName.Any(char.IsUpper)
        ? $"\"{schemaName}\""
        : schemaName;
  }
}

