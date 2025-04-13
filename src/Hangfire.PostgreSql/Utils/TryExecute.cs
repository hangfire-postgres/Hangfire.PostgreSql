﻿// This file is part of Hangfire.PostgreSql.
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

using System;

namespace Hangfire.PostgreSql.Utils;

public static class Utils
{
  public static bool TryExecute<T>(
    Func<T> func,
    out T? result,
    Func<Exception, bool>? swallowException = null,
    int? tryCount = null)
  {
    while (tryCount == null || tryCount-- > 0)
    {
      try
      {
        result = func();
        return true;
      }
      catch (Exception ex)
      {
        if (swallowException != null && !swallowException(ex))
        {
          throw;
        }
      }
    }
    result = default(T);
    return false;
  }
}