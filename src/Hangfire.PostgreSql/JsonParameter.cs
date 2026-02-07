using System;
using System.Text.Json;
using Hangfire.Annotations;

namespace Hangfire.PostgreSql;

internal static class JsonParameter
{
  public static string GetParameterValue([CanBeNull] object value)
  {
    return GetParameterValue(value, ValueType.Object);
  }

  public static string GetParameterValue([CanBeNull] object value, ValueType type)
  {
    return value switch {
      string { Length: > 0 } stringValue => stringValue,
      string { Length: 0 } or null => GetDefaultValue(type),
      var _ => JsonSerializer.Serialize(value),
    };
  }

  private static string GetDefaultValue(ValueType type)
  {
    return type switch {
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
