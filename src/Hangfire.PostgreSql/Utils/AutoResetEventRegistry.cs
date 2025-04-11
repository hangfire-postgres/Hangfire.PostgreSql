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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Hangfire.PostgreSql.Utils
{
  /// <summary>
  /// Represents a registry for managing AutoResetEvent instances using event keys.
  /// </summary>
  public class AutoResetEventRegistry
  {
    private readonly ConcurrentDictionary<string, AutoResetEvent> _events = new();

    /// <summary>
    /// Retrieves the wait handles associated with the specified event keys.
    /// </summary>
    /// <param name="eventKeys">The event keys.</param>
    /// <returns>An enumerable of wait handles.</returns>
    public IEnumerable<WaitHandle> GetWaitHandles(IEnumerable<string> eventKeys)
    {
      foreach (string eventKey in eventKeys)
      {
          AutoResetEvent newHandle = _events.GetOrAdd(eventKey, _ => new AutoResetEvent(false));
          yield return newHandle;
      }
    }

    /// <summary>
    /// Sets the specified event.
    /// </summary>
    /// <param name="eventKey">The event key.</param>
    public void Set(string eventKey)
    {
      if (_events.TryGetValue(eventKey, out AutoResetEvent handle))
      {
        handle.Set();
      }
    }
  }
}