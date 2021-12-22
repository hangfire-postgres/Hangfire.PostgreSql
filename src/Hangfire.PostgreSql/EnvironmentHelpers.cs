using System;
using System.Collections.Generic;
using System.Text;

namespace Hangfire.PostgreSql
{
  internal class EnvironmentHelpers
  {
    private static bool? _isMono;

    public static bool IsMono() =>
      _isMono ??= Type.GetType("Mono.Runtime") != null;
  }
}
