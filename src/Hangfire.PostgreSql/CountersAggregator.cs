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

    private static readonly TimeSpan _delayBetweenPasses = TimeSpan.FromMilliseconds(500);

    private readonly ILog _logger = LogProvider.For<CountersAggregator>();
    private readonly TimeSpan _interval;
    private readonly PostgreSqlStorage _storage;

    public CountersAggregator(PostgreSqlStorage storage, TimeSpan interval)
    {
      _storage = storage ?? throw new ArgumentNullException(nameof(storage));
      _interval = interval;
    }

    public void Execute(CancellationToken cancellationToken)
    {
      _logger.Debug("Aggregating records in 'Counter' table...");

      int removedCount = 0;
      
      do
      {
        _storage.UseConnection(null, connection => {
          removedCount = connection.Execute(GetAggregationQuery(),
            new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass },
            commandTimeout: 0);
        });

        if (removedCount < NumberOfRecordsInSinglePass)
        {
          continue;
        }

        cancellationToken.Wait(_delayBetweenPasses);
        cancellationToken.ThrowIfCancellationRequested();
        // ReSharper disable once LoopVariableIsNeverChangedInsideLoop
      } while (removedCount >= NumberOfRecordsInSinglePass);

      _logger.Trace("Records from the 'Counter' table aggregated.");

      cancellationToken.Wait(_interval);
    }

    private string GetAggregationQuery()
    {
      string schemaName = _storage.Options.SchemaName;
      return
        $"""
        BEGIN;

        INSERT INTO "{schemaName}"."aggregatedcounter" ("key", "value", "expireat")	
        SELECT
          "key",
          SUM("value"),
          MAX("expireat")
        FROM "{schemaName}"."counter"
        GROUP BY "key"
        ON CONFLICT("key") DO UPDATE
        SET "value" = "aggregatedcounter"."value" + EXCLUDED."value", "expireat" = EXCLUDED."expireat";
  
        DELETE FROM "{schemaName}"."counter"
        WHERE "key" IN (
          SELECT "key" FROM "{schemaName}"."aggregatedcounter"
        );

        COMMIT;
        """;
    }
  }
}
