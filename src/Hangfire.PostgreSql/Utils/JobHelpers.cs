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

namespace Hangfire.PostgreSql.Utils;

public static class JobHelpers
{
  public static long ParseJobId(this string jobId)
  {
    if (string.IsNullOrEmpty(jobId))
    {
      throw new ArgumentNullException(nameof(jobId));
    }

    if (!long.TryParse(jobId, out long id))
    {
      throw new ArgumentException($"Invalid job ID: {jobId}", nameof(jobId));
    }

    return id;
  }
}
