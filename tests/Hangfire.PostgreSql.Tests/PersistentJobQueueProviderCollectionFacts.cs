using System;
using System.Linq;
using Moq;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
  public class PersistentJobQueueProviderCollectionFacts
  {
    private static readonly string[] _queues = { "default", "critical" };
    private readonly Mock<IPersistentJobQueueProvider> _defaultProvider;
    private readonly Mock<IPersistentJobQueueProvider> _provider;

    public PersistentJobQueueProviderCollectionFacts()
    {
      _defaultProvider = new Mock<IPersistentJobQueueProvider>();
      _provider = new Mock<IPersistentJobQueueProvider>();
    }

    [Fact]
    public void Enumeration_IncludesTheDefaultProvider()
    {
      PersistentJobQueueProviderCollection collection = CreateCollection();

      IPersistentJobQueueProvider[] result = collection.ToArray();

      Assert.Single(result);
      Assert.Same(_defaultProvider.Object, result[0]);
    }

    [Fact]
    public void GetProvider_ReturnsTheDefaultProvider_WhenProviderCanNotBeResolvedByQueue()
    {
      PersistentJobQueueProviderCollection collection = CreateCollection();

      IPersistentJobQueueProvider provider = collection.GetProvider("queue");

      Assert.Same(_defaultProvider.Object, provider);
    }

    [Fact]
    public void Add_ThrowsAnException_WhenProviderIsNull()
    {
      PersistentJobQueueProviderCollection collection = CreateCollection();

      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => collection.Add(null, _queues));

      Assert.Equal("provider", exception.ParamName);
    }

    [Fact]
    public void Add_ThrowsAnException_WhenQueuesCollectionIsNull()
    {
      PersistentJobQueueProviderCollection collection = CreateCollection();

      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => collection.Add(_provider.Object, null));

      Assert.Equal("queues", exception.ParamName);
    }

    [Fact]
    public void Enumeration_ContainsAddedProvider()
    {
      PersistentJobQueueProviderCollection collection = CreateCollection();

      collection.Add(_provider.Object, _queues);

      Assert.Contains(_provider.Object, collection);
    }

    [Fact]
    public void GetProvider_CanBeResolved_ByAnyQueue()
    {
      PersistentJobQueueProviderCollection collection = CreateCollection();
      collection.Add(_provider.Object, _queues);

      IPersistentJobQueueProvider provider1 = collection.GetProvider("default");
      IPersistentJobQueueProvider provider2 = collection.GetProvider("critical");

      Assert.NotSame(_defaultProvider.Object, provider1);
      Assert.Same(provider1, provider2);
    }

    private PersistentJobQueueProviderCollection CreateCollection()
    {
      return new PersistentJobQueueProviderCollection(_defaultProvider.Object);
    }
  }
}
