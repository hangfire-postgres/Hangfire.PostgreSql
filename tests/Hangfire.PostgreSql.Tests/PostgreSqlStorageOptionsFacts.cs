using System;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
  public class PostgreSqlStorageOptionsFacts
  {
    [Fact]
    public void Ctor_SetsTheDefaultOptions()
    {
      PostgreSqlStorageOptions options = new();

      Assert.True(options.QueuePollInterval > TimeSpan.Zero);
      Assert.True(options.InvisibilityTimeout > TimeSpan.Zero);
      Assert.True(options.DistributedLockTimeout > TimeSpan.Zero);
      Assert.True(options.PrepareSchemaIfNecessary);
    }

    [Fact]
    public void Set_QueuePollInterval_ShouldThrowAnException_WhenGivenIntervalIsTooLow()
    {
      PostgreSqlStorageOptions options = new();
      Assert.Throws<ArgumentException>(() => options.QueuePollInterval = TimeSpan.FromMilliseconds(10));
    }

    [Fact]
    public void Set_QueuePollInterval_SetsTheValue_WhenGivenIntervalIsTooLow_ButIgnored()
    {
      PostgreSqlStorageOptions options = new() {
        AllowUnsafeValues = true,
        QueuePollInterval = TimeSpan.FromMilliseconds(10),
      };
      Assert.Equal(TimeSpan.FromMilliseconds(10), options.QueuePollInterval);
    }

    [Fact]
    public void Set_QueuePollInterval_ShouldThrowAnException_WhenGivenIntervalIsEqualToZero_EvenIfIgnored()
    {
      PostgreSqlStorageOptions options = new() { AllowUnsafeValues = true };
      Assert.Throws<ArgumentException>(() => options.QueuePollInterval = TimeSpan.Zero);
    }

    [Fact]
    public void Set_QueuePollInterval_SetsTheValue()
    {
      PostgreSqlStorageOptions options = new();
      options.QueuePollInterval = TimeSpan.FromSeconds(1);
      Assert.Equal(TimeSpan.FromSeconds(1), options.QueuePollInterval);
    }

    [Fact]
    public void Set_InvisibilityTimeout_ShouldThrowAnException_WhenGivenIntervalIsEqualToZero()
    {
      PostgreSqlStorageOptions options = new();
      Assert.Throws<ArgumentException>(() => options.InvisibilityTimeout = TimeSpan.Zero);
    }

    [Fact]
    public void Set_InvisibilityTimeout_ShouldThrowAnException_WhenGivenIntervalIsNegative()
    {
      PostgreSqlStorageOptions options = new();
      Assert.Throws<ArgumentException>(() => options.InvisibilityTimeout = TimeSpan.FromSeconds(-1));
    }

    [Fact]
    public void Set_InvisibilityTimeout_SetsTheValue()
    {
      PostgreSqlStorageOptions options = new();
      options.InvisibilityTimeout = TimeSpan.FromSeconds(1);
      Assert.Equal(TimeSpan.FromSeconds(1), options.InvisibilityTimeout);
    }

    [Fact]
    public void Set_DistributedLockTimeout_ShouldThrowAnException_WhenGivenIntervalIsEqualToZero()
    {
      PostgreSqlStorageOptions options = new();
      Assert.Throws<ArgumentException>(() => options.DistributedLockTimeout = TimeSpan.Zero);
    }

    [Fact]
    public void Set_DistributedLockTimeout_ShouldThrowAnException_WhenGivenIntervalIsNegative()
    {
      PostgreSqlStorageOptions options = new();
      Assert.Throws<ArgumentException>(() => options.DistributedLockTimeout = TimeSpan.FromSeconds(-1));
    }

    [Fact]
    public void Set_DistributedLockTimeout_SetsTheValue()
    {
      PostgreSqlStorageOptions options = new();
      options.DistributedLockTimeout = TimeSpan.FromSeconds(1);
      Assert.Equal(TimeSpan.FromSeconds(1), options.DistributedLockTimeout);
    }
  }
}
