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

namespace Hangfire.PostgreSql
{
  public class PostgreSqlStorageOptions
  {
    private static readonly TimeSpan _minimumQueuePollInterval = TimeSpan.FromMilliseconds(50);

    private int _deleteExpiredBatchSize;
    private TimeSpan _distributedLockTimeout;
    private TimeSpan _invisibilityTimeout;
    private TimeSpan _jobExpirationCheckInterval;
    private TimeSpan _queuePollInterval;
    private TimeSpan _transactionSerializationTimeout;
    private TimeSpan _countersAggregateInterval;

    public PostgreSqlStorageOptions()
    {
      QueuePollInterval = TimeSpan.FromSeconds(15);
      InvisibilityTimeout = TimeSpan.FromMinutes(30);
      DistributedLockTimeout = TimeSpan.FromMinutes(10);
      TransactionSynchronisationTimeout = TimeSpan.FromMilliseconds(500);
      JobExpirationCheckInterval = TimeSpan.FromHours(1);
      CountersAggregateInterval = TimeSpan.FromMinutes(5);
      SchemaName = "hangfire";
      AllowUnsafeValues = false;
      UseNativeDatabaseTransactions = true;
      PrepareSchemaIfNecessary = true;
      EnableTransactionScopeEnlistment = true;
      DeleteExpiredBatchSize = 1000;
      UseSlidingInvisibilityTimeout = false;
    }

    public TimeSpan QueuePollInterval
    {
      get => _queuePollInterval;
      set {
        ThrowIfValueIsLowerThan(_minimumQueuePollInterval, value, nameof(QueuePollInterval));
        _queuePollInterval = value;
      }
    }

    public TimeSpan InvisibilityTimeout
    {
      get => _invisibilityTimeout;
      set {
        ThrowIfValueIsNotPositive(value, nameof(InvisibilityTimeout));
        _invisibilityTimeout = value;
      }
    }
    
    public TimeSpan DistributedLockTimeout
    {
      get => _distributedLockTimeout;
      set {
        ThrowIfValueIsNotPositive(value, nameof(DistributedLockTimeout));
        _distributedLockTimeout = value;
      }
    }

    // ReSharper disable once IdentifierTypo
    public TimeSpan TransactionSynchronisationTimeout
    {
      get => _transactionSerializationTimeout;
      set {
        ThrowIfValueIsNotPositive(value, nameof(TransactionSynchronisationTimeout));
        _transactionSerializationTimeout = value;
      }
    }

    public TimeSpan JobExpirationCheckInterval
    {
      get => _jobExpirationCheckInterval;
      set {
        ThrowIfValueIsNotPositive(value, nameof(JobExpirationCheckInterval));
        _jobExpirationCheckInterval = value;
      }
    }

    public TimeSpan CountersAggregateInterval
    {
      get => _countersAggregateInterval;
      set {
        ThrowIfValueIsNotPositive(value, nameof(CountersAggregateInterval));
        _countersAggregateInterval = value;
      }
    }

    /// <summary>
    ///   Gets or sets the number of records deleted in a single batch in expiration manager
    /// </summary>
    public int DeleteExpiredBatchSize
    {
      get => _deleteExpiredBatchSize;
      set {
        ThrowIfValueIsNotPositive(value, nameof(DeleteExpiredBatchSize));
        _deleteExpiredBatchSize = value;
      }
    }

    public bool AllowUnsafeValues { get; set; }
    public bool UseNativeDatabaseTransactions { get; set; }
    public bool PrepareSchemaIfNecessary { get; set; }
    public string SchemaName { get; set; }
    public bool EnableTransactionScopeEnlistment { get; set; }
    public bool EnableLongPolling { get; set; }

    /// <summary>
    ///   Apply a sliding invisibility timeout where the last fetched time is continually updated in the background.
    ///   This allows a lower invisibility timeout to be used with longer running jobs
    ///   IMPORTANT: If <see cref="BackgroundJobServerOptions.IsLightweightServer" /> option is used, then sliding invisiblity timeouts will not work
    ///   since the background storage processes are not run (which is used to update the invisibility timeouts)
    /// </summary>
    public bool UseSlidingInvisibilityTimeout { get; set; }

    private static void ThrowIfValueIsNotPositive(TimeSpan value, string fieldName)
    {
      string message = $"The {fieldName} property value should be positive. Given: {value}.";

      if (value == TimeSpan.Zero)
      {
        throw new ArgumentException(message, nameof(value));
      }

      if (value != value.Duration())
      {
        throw new ArgumentException(message, nameof(value));
      }
    }

    private void ThrowIfValueIsLowerThan(TimeSpan minValue, TimeSpan value, string fieldName)
    {
      if (!AllowUnsafeValues)
      {
        string message = $"The {fieldName} property value seems to be too low ({value}, lower than suggested minimum of {minValue}). Consider increasing it. If you really need to have such a low value, please set {nameof(PostgreSqlStorageOptions)}.{nameof(AllowUnsafeValues)} to true.";

        if (value < minValue)
        {
          throw new ArgumentException(message, nameof(value));
        }
      }

      ThrowIfValueIsNotPositive(value, fieldName);
    }

    private static void ThrowIfValueIsNotPositive(int value, string fieldName)
    {
      if (value <= 0)
      {
        throw new ArgumentException($"The {fieldName} property value should be positive. Given: {value}.");
      }
    }
  }
}
