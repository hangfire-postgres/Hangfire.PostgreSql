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
using System.Data;
using System.Diagnostics;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Npgsql;

namespace Hangfire.PostgreSql
{
  public sealed class PostgreSqlDistributedLock
  {
    private static readonly ILog _logger = LogProvider.GetCurrentClassLogger();

    private static void Log(string resource, string message, Exception ex)
    {
      bool isConcurrencyError = ex is PostgresException { SqlState: PostgresErrorCodes.SerializationFailure };
      _logger.Log(isConcurrencyError ? LogLevel.Debug : LogLevel.Warn, () => $"{resource}: {message}", ex);
    }

    internal static void Acquire(IDbConnection connection, string resource, TimeSpan timeout, PostgreSqlStorageOptions options)
    {
      if (connection == null)
      {
        throw new ArgumentNullException(nameof(connection));
      }

      if (string.IsNullOrEmpty(resource))
      {
        throw new ArgumentNullException(nameof(resource));
      }

      if (options == null)
      {
        throw new ArgumentNullException(nameof(options));
      }

      if (connection.State != ConnectionState.Open)
      {
        // When we are passing a closed connection to Dapper's Execute method,
        // it kindly opens it for us, but after command execution, it will be closed
        // automatically, and our just-acquired application lock will immediately
        // be released. This is not behavior we want to achieve, so let's throw an
        // exception instead.
        throw new InvalidOperationException("Connection must be open before acquiring a distributed lock.");
      }

      if (options.UseNativeDatabaseTransactions)
      {
        TransactionLockHandler.Lock(resource, timeout, connection, options);
      }
      else
      {
        UpdateCountLockHandler.Lock(resource, timeout, connection, options);
      }
    }

    internal static void Release(IDbConnection connection, string resource, PostgreSqlStorageOptions options)
    {
      if (connection == null)
      {
        throw new ArgumentNullException(nameof(connection));
      }

      if (resource == null)
      {
        throw new ArgumentNullException(nameof(resource));
      }

      if (options == null)
      {
        throw new ArgumentNullException(nameof(options));
      }

      int rowsAffected = connection.Execute($@"DELETE FROM ""{options.SchemaName}"".""lock"" WHERE ""resource"" = @Resource;",
        new { Resource = resource });

      if (rowsAffected <= 0)
      {
        throw new PostgreSqlDistributedLockException($"Could not release a lock on the resource '{resource}'. Lock does not exists.");
      }
    }

    private static class TransactionLockHandler
    {
      public static void Lock(string resource, TimeSpan timeout, IDbConnection connection, PostgreSqlStorageOptions options)
      {
        Stopwatch lockAcquiringTime = Stopwatch.StartNew();

        bool tryAcquireLock = true;

        while (tryAcquireLock)
        {
          if (connection.State != ConnectionState.Open)
          {
            connection.Open();
          }

          TryRemoveLock(resource, connection, options);

          try
          {
            int rowsAffected;
            using (IDbTransaction trx = connection.BeginTransaction(IsolationLevel.RepeatableRead))
            {
              rowsAffected = connection.Execute($@"
                INSERT INTO ""{options.SchemaName}"".""lock""(""resource"", ""acquired"") 
                SELECT @Resource, @Acquired
                WHERE NOT EXISTS (
                    SELECT 1 FROM ""{options.SchemaName}"".""lock"" 
                    WHERE ""resource"" = @Resource
                )
                ON CONFLICT DO NOTHING;
              ",
                new {
                  Resource = resource,
                  Acquired = DateTime.UtcNow,
                }, trx);
              trx.Commit();
            }

            if (rowsAffected > 0)
            {
              return;
            }
          }
          catch (Exception ex)
          {
            Log(resource, "Failed to lock with transaction", ex);
          }

          if (lockAcquiringTime.ElapsedMilliseconds > timeout.TotalMilliseconds)
          {
            tryAcquireLock = false;
          }
          else
          {
            int sleepDuration = (int)(timeout.TotalMilliseconds - lockAcquiringTime.ElapsedMilliseconds);
            if (sleepDuration > 1000)
            {
              sleepDuration = 1000;
            }

            if (sleepDuration > 0)
            {
              Thread.Sleep(sleepDuration);
            }
            else
            {
              tryAcquireLock = false;
            }
          }
        }

        throw new PostgreSqlDistributedLockException($@"Could not place a lock on the resource '{resource}': Lock timeout.");
      }

      private static void TryRemoveLock(string resource, IDbConnection connection, PostgreSqlStorageOptions options)
      {
        try
        {
          using IDbTransaction transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead);
          connection.Execute($@"DELETE FROM ""{options.SchemaName}"".""lock"" WHERE ""resource"" = @Resource AND ""acquired"" < @Timeout",
            new {
              Resource = resource,
              Timeout = DateTime.UtcNow - options.DistributedLockTimeout,
            }, transaction: transaction);

          transaction.Commit();
        }
        catch (Exception ex)
        {
          Log(resource, "Failed to remove lock", ex);
        }
      }
    }

    private static class UpdateCountLockHandler
    {
      public static void Lock(string resource, TimeSpan timeout, IDbConnection connection, PostgreSqlStorageOptions options)
      {
        Stopwatch lockAcquiringStopwatch = Stopwatch.StartNew();

        bool tryAcquireLock = true;

        while (tryAcquireLock)
        {
          try
          {
            connection.Execute($@"
              INSERT INTO ""{options.SchemaName}"".""lock""(""resource"", ""updatecount"", ""acquired"") 
              SELECT @Resource, 0, @Acquired
              WHERE NOT EXISTS (
                  SELECT 1 FROM ""{options.SchemaName}"".""lock"" 
                  WHERE ""resource"" = @Resource
              )
              ON CONFLICT DO NOTHING;
            ", new {
              Resource = resource,
              Acquired = DateTime.UtcNow,
            });
          }
          catch (Exception ex)
          {
            Log(resource, "Failed to lock with update count", ex);
          }

          int rowsAffected = connection.Execute(
            $@"UPDATE ""{options.SchemaName}"".""lock"" SET ""updatecount"" = 1 WHERE ""updatecount"" = 0 AND ""resource"" = @Resource",
            new { Resource = resource });

          if (rowsAffected > 0)
          {
            return;
          }

          if (lockAcquiringStopwatch.ElapsedMilliseconds > timeout.TotalMilliseconds)
          {
            tryAcquireLock = false;
          }
          else
          {
            int sleepDuration = (int)(timeout.TotalMilliseconds - lockAcquiringStopwatch.ElapsedMilliseconds);
            if (sleepDuration > 1000)
            {
              sleepDuration = 1000;
            }

            if (sleepDuration > 0)
            {
              Thread.Sleep(sleepDuration);
            }
            else
            {
              tryAcquireLock = false;
            }
          }
        }

        throw new PostgreSqlDistributedLockException($"Could not place a lock on the resource '{resource}': Lock timeout.");
      }
    }
  }
}
