using System;
using System.Data;
using System.Text.Json;
using Dapper;
using Hangfire.Annotations;
using Npgsql;
using NpgsqlTypes;

namespace Hangfire.PostgreSql;

internal class JsonParameter : SqlMapper.ICustomQueryParameter
{
  [CanBeNull] private readonly object _value;
  private readonly ValueType _type;

  public JsonParameter([CanBeNull] object value) : this(value, ValueType.Object)
  {
  }

  public JsonParameter([CanBeNull] object value, ValueType type)
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
