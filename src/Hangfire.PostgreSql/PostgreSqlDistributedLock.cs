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
using System.Diagnostics;
using System.Transactions;
using Hangfire.Logging;
using Npgsql;
using IsolationLevel = System.Data.IsolationLevel;

namespace Hangfire.PostgreSql;

public static class PostgreSqlDistributedLock
{
  private static readonly ILog _logger = LogProvider.GetCurrentClassLogger();

  private static void Log(string resource, string message, Exception ex)
  {
    bool isConcurrencyError = ex is PostgresException { SqlState: PostgresErrorCodes.SerializationFailure };
    _logger.Log(isConcurrencyError ? LogLevel.Trace : LogLevel.Warn, () => $"{resource}: {message}", ex);
  }

  internal static void Acquire(IDbConnection connection, string resource, TimeSpan timeout, PostgreSqlStorageContext context)
  {
    if (connection == null)
    {
      throw new ArgumentNullException(nameof(connection));
    }

    if (string.IsNullOrEmpty(resource))
    {
      throw new ArgumentNullException(nameof(resource));
    }

    if (context == null)
    {
      throw new ArgumentNullException(nameof(context));
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

    LockHandler.Lock(resource, timeout, connection, context);
  }

  internal static void Release(IDbConnection connection, string resource, PostgreSqlStorageContext context)
  {
    if (connection == null)
    {
      throw new ArgumentNullException(nameof(connection));
    }

    if (resource == null)
    {
      throw new ArgumentNullException(nameof(resource));
    }

    if (context == null)
    {
      throw new ArgumentNullException(nameof(context));
    }

    if (!LockHandler.TryRemoveLock(resource, connection, context, false))
    {
      throw new PostgreSqlDistributedLockException(resource);
    }
  }

  private static class LockHandler
  {
    public static void Lock(string resource, TimeSpan timeout, IDbConnection connection, PostgreSqlStorageContext context)
    {
      Stopwatch lockAcquiringTime = Stopwatch.StartNew();

      bool tryAcquireLock = true;
      Func<IDbConnection, PostgreSqlStorageContext, string, bool> tryLock = context.Options.UseNativeDatabaseTransactions
        ? TransactionLockHandler.TryLock
        : UpdateCountLockHandler.TryLock;

      while (tryAcquireLock)
      {
        if (connection.State != ConnectionState.Open)
        {
          connection.Open();
        }

        TryRemoveLock(resource, connection, context, true);

        try
        {
          if (tryLock(connection, context, resource))
          {
            return;
          }
        }
        catch (Exception ex)
        {
          Log(resource, "Failed to acquire lock", ex);
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

      throw new PostgreSqlDistributedLockException(resource);
    }

    public static bool TryRemoveLock(string resource, IDbConnection connection, PostgreSqlStorageContext context, bool onlyExpired)
    {
      IDbTransaction? trx = null;
      try
      {
        // Non-expired locks are removed only when releasing them. Transaction is not needed in that case.
        if (onlyExpired && context.Options.UseNativeDatabaseTransactions)
        {
          trx = TransactionLockHandler.BeginTransactionIfNotPresent(connection);
        }

        DateTime timeout = onlyExpired ? DateTime.UtcNow - context.Options.DistributedLockTimeout : DateTime.MaxValue;

        string query = context.QueryProvider.GetQuery("DELETE FROM hangfire.lock WHERE resource = $1 AND acquired < $2");
        int rowsAffected = connection.Process(query, trx).WithParameters(resource, timeout).Execute();

        trx?.Commit();

        return rowsAffected >= 0;
      }
      catch (Exception ex)
      {
        Log(resource, "Failed to remove lock", ex);
        return false;
      }
      finally
      {
        trx?.Dispose();
      }
    }
  }

  private static class TransactionLockHandler
  {
    public static bool TryLock(IDbConnection connection, PostgreSqlStorageContext context, string resource)
    {
      IDbTransaction? trx = null;
      try
      {
        trx = BeginTransactionIfNotPresent(connection);

        string query = context.QueryProvider.GetQuery(
          """
          INSERT INTO hangfire.lock (resource, acquired) 
          SELECT $1, $2
          WHERE NOT EXISTS (
              SELECT 1 FROM hangfire.lock
              WHERE resource = $1
          )
          ON CONFLICT DO NOTHING
          """);
        int rowsAffected = connection.Process(query, trx).WithParameters(resource, DateTime.UtcNow).Execute();
        trx?.Commit();

        return rowsAffected > 0;
      }
      finally
      {
        trx?.Dispose();
      }
    }

    public static IDbTransaction? BeginTransactionIfNotPresent(IDbConnection connection)
    {
      // If transaction scope was created outside of hangfire, the newly-opened connection is automatically enlisted into the transaction.
      // Starting a new transaction throws "A transaction is already in progress; nested/concurrent transactions aren't supported." in that case.
      return Transaction.Current == null ? connection.BeginTransaction(IsolationLevel.ReadCommitted) : null;
    }
  }

  private static class UpdateCountLockHandler
  {
    public static bool TryLock(IDbConnection connection, PostgreSqlStorageContext context, string resource)
    {
      string query = context.QueryProvider.GetQuery(
        """
        INSERT INTO hangfire.lock (resource, updatecount, acquired) 
        SELECT $1, 0, $2
        WHERE NOT EXISTS (
            SELECT 1 FROM hangfire.lock
            WHERE resource = $1
        )
        ON CONFLICT DO NOTHING
        """);
      connection.Process(query).WithParameters(resource, DateTime.UtcNow).Execute();

      // The lock is acquired if the updatecount is 0. If it is 1, it means that the lock was already acquired by another process.
      // In that case, we need to update the updatecount to 1 to indicate that the lock is now held by this process.
      string updateQuery = context.QueryProvider.GetQuery("UPDATE hangfire.lock SET updatecount = 1 WHERE updatecount = 0 AND resource = $1");
      int rowsAffected = connection.Process(updateQuery).WithParameter(resource).Execute();

      return rowsAffected > 0;
    }
  }
}