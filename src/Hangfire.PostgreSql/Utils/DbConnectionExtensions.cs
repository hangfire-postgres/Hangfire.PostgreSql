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

using System.Data;
using Npgsql;

namespace Hangfire.PostgreSql.Utils;

internal static class DbConnectionExtensions
{
  private static bool? _supportsNotifications;

  internal static bool SupportsNotifications(this IDbConnection connection)
  {
    if (_supportsNotifications.HasValue)
    {
      return _supportsNotifications.Value;
    }

    if (connection is not NpgsqlConnection npgsqlConnection)
    {
      _supportsNotifications = false;
      return false;
    }

    if (npgsqlConnection.State != ConnectionState.Open)
    {
      npgsqlConnection.Open();
    }

    _supportsNotifications = npgsqlConnection.PostgreSqlVersion.Major >= 11;
    return _supportsNotifications.Value;
  }
}
