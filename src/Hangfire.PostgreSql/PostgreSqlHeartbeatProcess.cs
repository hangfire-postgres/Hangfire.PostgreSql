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

using System;
using System.Collections.Concurrent;
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;

namespace Hangfire.PostgreSql
{
#pragma warning disable CS0618
  internal sealed class PostgreSqlHeartbeatProcess : IServerComponent, IBackgroundProcess
#pragma warning restore CS0618
  {
    private readonly ConcurrentDictionary<PostgreSqlFetchedJob, object> _items = new();

    public void Track(PostgreSqlFetchedJob item)
    {
      _items.TryAdd(item, null);
    }

    public void Untrack(PostgreSqlFetchedJob item)
    {
      _items.TryRemove(item, out var _);
    }
    
    public void Execute(CancellationToken cancellationToken)
    {
      foreach (var item in _items)
      {
        item.Key.ExecuteKeepAliveQueryIfRequired();
      }

      cancellationToken.Wait(TimeSpan.FromSeconds(1));
    }

    public void Execute(BackgroundProcessContext context)
    {
      Execute(context.StoppingToken);
    }
  }
}