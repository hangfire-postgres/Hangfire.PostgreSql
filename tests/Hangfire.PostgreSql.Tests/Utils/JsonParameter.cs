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
#nullable enable
using System.Data;
using System.Text.Json;
using Dapper;
using Npgsql;
using NpgsqlTypes;

namespace Hangfire.PostgreSql.Tests.Utils;

internal class JsonParameter : SqlMapper.ICustomQueryParameter
{
  private readonly object? _value;
  private readonly ValueType _type;

  public JsonParameter(object? value) : this(value, ValueType.Object)
  {
  }

  public JsonParameter(object? value, ValueType type)
  {
    _value = value;
    _type = type;
  }

  public void AddParameter(IDbCommand command, string name)
  {
    string value = _value switch {
      string { Length: > 0 } stringValue => stringValue,
      string { Length: 0 } or null => GetDefaultValue(),
      var _ => JsonSerializer.Serialize(_value),
    };
    command.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Jsonb) { Value = value });
  }

  private string GetDefaultValue()
  {
    return _type switch
    {
      ValueType.Object => "{}",
      ValueType.Array => "[]",
      var _ => throw new ArgumentOutOfRangeException(),
    };
  }

  public enum ValueType
  {
    Object,
    Array,
  }
}
