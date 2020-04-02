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
using System.Collections;
using System.Collections.Generic;

namespace Hangfire.PostgreSql
{
    public class PersistentJobQueueProviderCollection : IEnumerable<IPersistentJobQueueProvider>
    {
        private readonly List<IPersistentJobQueueProvider> _providers
            = new List<IPersistentJobQueueProvider>(); 
        private readonly Dictionary<string, IPersistentJobQueueProvider> _providersByQueue
            = new Dictionary<string, IPersistentJobQueueProvider>(StringComparer.OrdinalIgnoreCase);

        private readonly IPersistentJobQueueProvider _defaultProvider;

        public PersistentJobQueueProviderCollection(IPersistentJobQueueProvider defaultProvider)
        {
	        _defaultProvider = defaultProvider ?? throw new ArgumentNullException(nameof(defaultProvider));
            _providers.Add(_defaultProvider);
        }

        public void Add(IPersistentJobQueueProvider provider, IEnumerable<string> queues)
        {
            if (provider == null) throw new ArgumentNullException(nameof(provider));
            if (queues == null) throw new ArgumentNullException(nameof(queues));

            _providers.Add(provider);

            foreach (var queue in queues)
            {
                _providersByQueue.Add(queue, provider);
            }
        }

        public IPersistentJobQueueProvider GetProvider(string queue)
        {
            return _providersByQueue.ContainsKey(queue) 
                ? _providersByQueue[queue]
                : _defaultProvider;
        }

        public IEnumerator<IPersistentJobQueueProvider> GetEnumerator()
        {
            return _providers.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}