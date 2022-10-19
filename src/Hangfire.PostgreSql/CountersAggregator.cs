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
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.PostgreSql
{
#pragma warning disable 618
  internal class CountersAggregator : IServerComponent
#pragma warning restore 618
  {
    // This number should be high enough to aggregate counters efficiently,
    // but low enough to not to cause large amount of row locks to be taken.
    // Lock escalation to page locks may pause the background processing.
    private const int NumberOfRecordsInSinglePass = 1000;
    private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);

    private readonly ILog _logger = LogProvider.For<CountersAggregator>();
    private readonly TimeSpan _interval;
    private readonly PostgreSqlStorage _storage;

    public CountersAggregator(PostgreSqlStorage storage, TimeSpan interval)
    {
      if (storage == null) throw new ArgumentNullException(nameof(storage));

      _storage = storage;
      _interval = interval;
    }

    public void Execute(CancellationToken cancellationToken)
    {
      _logger.Debug("Aggregating records in 'Counter' table...");

      int removedCount = 0;
      
      do
      {
        _storage.UseConnection(null, connection => {
          removedCount = connection.Execute(GetAggregationQuery(_storage),
            new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass },
            commandTimeout: 0);
        });

        if (removedCount >= NumberOfRecordsInSinglePass)
        {
          cancellationToken.Wait(DelayBetweenPasses);
          cancellationToken.ThrowIfCancellationRequested();
        }
        // ReSharper disable once LoopVariableIsNeverChangedInsideLoop
      } while (removedCount >= NumberOfRecordsInSinglePass);

      _logger.Trace("Records from the 'Counter' table aggregated.");

      cancellationToken.Wait(_interval);
    }

    public override string ToString()
    {
      return GetType().ToString();
    }

    private string GetAggregationQuery(PostgreSqlStorage storage)
    {
      // Starting from SQL Server 2014 it's possible to get a query with
      // much lower cost by adding a clustered index on [Key] column.
      // However extended support for SQL Server 2012 SP4 ends only on
      // July 12, 2022.
      return
        $@"begin;

insert into ""{_storage.Options.SchemaName}"".""aggregatedcounter"" (""key"", ""value"", ""expireat"")	
      select
      ""key"",
      sum(""value""),
      max(""expireat"")
      from ""hangfire"".""counter""
      group by
      ""key""
      on conflict(""key"") do
        update
          set
      ""value"" = ""aggregatedcounter"".""value"" + excluded.""value"",
      ""expireat"" = excluded.""expireat"";

      delete from ""{_storage.Options.SchemaName}"".""counter""
        where
      ""key"" in (select ""key"" from ""{_storage.Options.SchemaName}"".""aggregatedcounter"" );

      commit;
      ";
    }
  }
}
