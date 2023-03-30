using System.Threading;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Xunit;

namespace Hangfire.PostgreSql.Tests;

public class FirstClassQueueFeatureSupportTests
{
  public FirstClassQueueFeatureSupportTests()
  {
    JobStorage.Current = new PostgreSqlStorage(ConnectionUtils.GetConnectionString());
  }

  [Fact]
  public void HasFlag_ShouldReturnTrue_ForJobQueueProperty()
  {
    bool supportJobQueueProperty = JobStorage.Current.HasFeature(JobStorageFeatures.JobQueueProperty);
    Assert.True(supportJobQueueProperty);
  }

  [Fact]
  [CleanDatabase]
  public void EnqueueJobWithSpecificQueue_ShouldEnqueueCorrectlyAndJobMustBeProcessedInThatQueue()
  {
    BackgroundJob.Enqueue<TestJobs>("critical", job => job.Run("critical"));
    BackgroundJob.Enqueue<TestJobs>("offline", job => job.Run("offline"));

    BackgroundJobServer server = new(new BackgroundJobServerOptions() {
      Queues = new[] { "critical" },
    });

    Thread.Sleep(200);

    IMonitoringApi monitoringApi = JobStorage.Current.GetMonitoringApi();

    JobList<EnqueuedJobDto> jobsInCriticalQueue = monitoringApi.EnqueuedJobs("critical", 0, 10);
    JobList<EnqueuedJobDto> jobsInOfflineQueue = monitoringApi.EnqueuedJobs("offline", 0, 10);

    Assert.Empty(jobsInCriticalQueue);   //Job from 'critical' queue must be processed by the server 
    Assert.NotEmpty(jobsInOfflineQueue); //Job from 'offline' queue must be left untouched because no server is processing it
  }
}
