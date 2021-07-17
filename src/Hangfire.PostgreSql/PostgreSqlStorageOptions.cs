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
        private TimeSpan _queuePollInterval;
        private TimeSpan _invisibilityTimeout;
        private TimeSpan _distributedLockTimeout;
        private TimeSpan _transactionSerializationTimeout;
        private TimeSpan _jobExpirationCheckInterval;
        private int _deleteExpiredBatchSize;

        public PostgreSqlStorageOptions()
        {
            QueuePollInterval = TimeSpan.FromSeconds(15);
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            DistributedLockTimeout = TimeSpan.FromMinutes(10);
	        TransactionSynchronisationTimeout = TimeSpan.FromMilliseconds(500);
	        JobExpirationCheckInterval = TimeSpan.FromHours(1);
            SchemaName = "hangfire";
            UseNativeDatabaseTransactions = true;
            PrepareSchemaIfNecessary = true;
            DeleteExpiredBatchSize = 1000;
        }

        public TimeSpan QueuePollInterval
        {
            get => _queuePollInterval;
	        set
            {
                ThrowIfValueIsNotPositive(value, nameof(QueuePollInterval));
                _queuePollInterval = value;
            }
        }

        public TimeSpan InvisibilityTimeout
        {
            get => _invisibilityTimeout;
	        set
            {
                ThrowIfValueIsNotPositive(value, nameof(InvisibilityTimeout));
                _invisibilityTimeout = value;
            }
        }

        public TimeSpan DistributedLockTimeout
        {
            get => _distributedLockTimeout;
	        set
            {
                ThrowIfValueIsNotPositive(value, nameof(DistributedLockTimeout));
                _distributedLockTimeout = value;
            }
        }

	    public TimeSpan TransactionSynchronisationTimeout
	    {
			get => _transactionSerializationTimeout;
		    set
		    {
			    ThrowIfValueIsNotPositive(value, nameof(TransactionSynchronisationTimeout));
			    _transactionSerializationTimeout = value;
		    }
		}

		public TimeSpan JobExpirationCheckInterval
		{
			get => _jobExpirationCheckInterval;
			set
			{
				ThrowIfValueIsNotPositive(value, nameof(JobExpirationCheckInterval));
				_jobExpirationCheckInterval = value;
			}
		}

		/// <summary>
		/// Gets or sets the number of records deleted in a single batch in expiration manager
		/// </summary>
		public int DeleteExpiredBatchSize
		{
			get => _deleteExpiredBatchSize;
			set 
			{
				ThrowIfValueIsNotPositive(value, nameof(DeleteExpiredBatchSize));
                _deleteExpiredBatchSize = value;
			}
		}

		public bool UseNativeDatabaseTransactions { get; set; }
        public bool PrepareSchemaIfNecessary { get; set; }
        public string SchemaName { get; set; }
        public bool EnableTransactionScopeEnlistment { get; set; }

        private static void ThrowIfValueIsNotPositive(TimeSpan value, string fieldName)
        {
            var message = $"The {fieldName} property value should be positive. Given: {value}.";

            if (value == TimeSpan.Zero)
            {
                throw new ArgumentException(message, nameof(value));
            }
            if (value != value.Duration())
            {
                throw new ArgumentException(message, nameof(value));
            }
        }

        private static void ThrowIfValueIsNotPositive(int value, string fieldName)
        {
            if (value < 0)
		        throw new ArgumentException($"The {fieldName} property value should be positive. Given: {value}.");
        }
    }
}
