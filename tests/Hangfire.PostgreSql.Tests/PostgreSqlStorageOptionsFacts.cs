using System;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlStorageOptionsFacts
    {
        [Fact]
        public void Ctor_SetsTheDefaultOptions()
        {
            var options = new PostgreSqlStorageOptions();

            Assert.True(options.QueuePollInterval > TimeSpan.Zero);
            Assert.True(options.InvisibilityTimeout > TimeSpan.Zero);
            Assert.True(options.DistributedLockTimeout > TimeSpan.Zero);
            Assert.True(options.PrepareSchemaIfNecessary);
        }

        [Fact]
        public void Set_QueuePollInterval_ShouldThrowAnException_WhenGivenIntervalIsEqualToZero()
        {
            var options = new PostgreSqlStorageOptions();
            Assert.Throws<ArgumentException>(
                () => options.QueuePollInterval = TimeSpan.Zero);
        }

        [Fact]
        public void Set_QueuePollInterval_ShouldThrowAnException_WhenGivenIntervalIsNegative()
        {
            var options = new PostgreSqlStorageOptions();
            Assert.Throws<ArgumentException>(
                () => options.QueuePollInterval = TimeSpan.FromSeconds(-1));
        }

        [Fact]
        public void Set_QueuePollInterval_SetsTheValue()
        {
            var options = new PostgreSqlStorageOptions();
            options.QueuePollInterval = TimeSpan.FromSeconds(1);
            Assert.Equal(TimeSpan.FromSeconds(1), options.QueuePollInterval);
        }

        [Fact]
        public void Set_InvisibilityTimeout_ShouldThrowAnException_WhenGivenIntervalIsEqualToZero()
        {
            var options = new PostgreSqlStorageOptions();
            Assert.Throws<ArgumentException>(
                () => options.InvisibilityTimeout = TimeSpan.Zero);
        }

        [Fact]
        public void Set_InvisibilityTimeout_ShouldThrowAnException_WhenGivenIntervalIsNegative()
        {
            var options = new PostgreSqlStorageOptions();
            Assert.Throws<ArgumentException>(
                () => options.InvisibilityTimeout = TimeSpan.FromSeconds(-1));
        }

        [Fact]
        public void Set_InvisibilityTimeout_SetsTheValue()
        {
            var options = new PostgreSqlStorageOptions();
            options.InvisibilityTimeout = TimeSpan.FromSeconds(1);
            Assert.Equal(TimeSpan.FromSeconds(1), options.InvisibilityTimeout);
        }

        [Fact]
        public void Set_DistributedLockTimeout_ShouldThrowAnException_WhenGivenIntervalIsEqualToZero()
        {
            var options = new PostgreSqlStorageOptions();
            Assert.Throws<ArgumentException>(
                () => options.DistributedLockTimeout = TimeSpan.Zero);
        }

        [Fact]
        public void Set_DistributedLockTimeout_ShouldThrowAnException_WhenGivenIntervalIsNegative()
        {
            var options = new PostgreSqlStorageOptions();
            Assert.Throws<ArgumentException>(
                () => options.DistributedLockTimeout = TimeSpan.FromSeconds(-1));
        }

        [Fact]
        public void Set_DistributedLockTimeout_SetsTheValue()
        {
            var options = new PostgreSqlStorageOptions();
            options.DistributedLockTimeout = TimeSpan.FromSeconds(1);
            Assert.Equal(TimeSpan.FromSeconds(1), options.DistributedLockTimeout);
        }
    }
}