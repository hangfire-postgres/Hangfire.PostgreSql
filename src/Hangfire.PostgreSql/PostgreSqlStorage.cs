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

using Hangfire.Logging;
using Hangfire.PostgreSql.Components;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.PostgreSql;

public class PostgreSqlStorage : JobStorage
{
  private readonly Dictionary<string, bool> _features =
    new(StringComparer.OrdinalIgnoreCase)
    {
      { JobStorageFeatures.JobQueueProperty, true },
      { JobStorageFeatures.Connection.BatchedGetFirstByLowest, true },
    };

  public PostgreSqlStorage(IConnectionFactory connectionFactory) : this(connectionFactory, new PostgreSqlStorageOptions()) { }

  public PostgreSqlStorage(IConnectionFactory connectionFactory, PostgreSqlStorageOptions options, PersistentJobQueueProviderCollection? persistentJobQueueProviderCollection = null)
  {
    Context = new PostgreSqlStorageContext(options, new PostgreSqlDbConnectionManager(connectionFactory, options));
    PostgreSqlObjectsInstaller.Install(Context);

    if (persistentJobQueueProviderCollection == null)
    {
      PostgreSqlJobQueueProvider defaultQueueProvider = new(Context);
      persistentJobQueueProviderCollection = new PersistentJobQueueProviderCollection(defaultQueueProvider);
    }
    Context.QueueProviders = persistentJobQueueProviderCollection;
  }
  
  internal PostgreSqlStorageContext Context { get; }

  public override IMonitoringApi GetMonitoringApi()
  {
    return new PostgreSqlMonitoringApi(Context);
  }

  public override IStorageConnection GetConnection()
  {
    return new PostgreSqlConnection(Context);
  }

#pragma warning disable CS0618
  public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore CS0618
  {
    yield return new ExpirationManager(Context);
    yield return new CountersAggregator(Context);
    if (Context.HeartbeatProcess != null)
    {
      // This is only used to update the sliding invisibility timeouts, so if not enabled then do not use it
      yield return Context.HeartbeatProcess;
    }
  }

  public override void WriteOptionsToLog(ILog logger)
  {
    PostgreSqlStorageOptions options = Context.Options;
    logger.Info("Using the following options for PostgreSQL job storage:");
    logger.InfoFormat("    Queue poll interval: {0}.", options.QueuePollInterval);
    logger.InfoFormat("    Invisibility timeout: {0}.", options.InvisibilityTimeout);
    logger.InfoFormat("    Use sliding invisibility timeout: {0}.", options.UseSlidingInvisibilityTimeout);
  }

  public override string ToString()
  {
    return Context.ConnectionManager.ToString();
  }

  public override bool HasFeature(string featureId)
  {
    if (featureId == null)
    {
      throw new ArgumentNullException(nameof(featureId));
    }

    return _features.TryGetValue(featureId, out bool isSupported) 
      ? isSupported
      : base.HasFeature(featureId);
  }
}