// This file is part of Hangfire. Copyright © 2022 Hangfire OÜ.
// 
// Hangfire is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire. If not, see <http://www.gnu.org/licenses/>.

// Borrowed from Hangfire

using System;
using System.Collections.Generic;

namespace Hangfire.PostgreSql.Utils;

internal static class ExceptionTypeHelper
{
  private static readonly HashSet<Type> _nonCatchableExceptionTypes = [typeof(StackOverflowException), typeof(OutOfMemoryException)];
 
  internal static bool IsCatchableExceptionType(this Exception e)
  {
    Type? type = e.GetType();
    return !_nonCatchableExceptionTypes.Contains(type);
  }
}