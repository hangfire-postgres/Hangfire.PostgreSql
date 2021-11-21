using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
  public class PostgreSqlStorageFacts
  {
    private readonly PostgreSqlStorageOptions _options;

    public PostgreSqlStorageFacts()
    {
      _options = new PostgreSqlStorageOptions { PrepareSchemaIfNecessary = false, EnableTransactionScopeEnlistment = true };
    }


    [Fact]
    public void Ctor_ThrowsAnException_WhenConnectionStringIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new PostgreSqlStorage(nameOrConnectionString: null));

      Assert.Equal("nameOrConnectionString", exception.ParamName);
    }

    [Fact]
    public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new PostgreSqlStorage("hello", null));

      Assert.Equal("options", exception.ParamName);
    }

    [Fact]
    [CleanDatabase]
    public void Ctor_CanCreateSqlServerStorage_WithExistingConnection()
    {
      NpgsqlConnection connection = ConnectionUtils.CreateConnection();
      PostgreSqlStorage storage = new PostgreSqlStorage(connection, _options);

      Assert.NotNull(storage);
    }

    [Fact]
    [CleanDatabase]
    public void Ctor_InitializesDefaultJobQueueProvider_AndPassesCorrectOptions()
    {
      PostgreSqlStorage storage = CreateStorage();
      PersistentJobQueueProviderCollection providers = storage.QueueProviders;

      PostgreSqlJobQueueProvider provider = (PostgreSqlJobQueueProvider)providers.GetProvider("default");

      Assert.Same(_options, provider.Options);
    }

    [Fact]
    [CleanDatabase]
    public void GetMonitoringApi_ReturnsNonNullInstance()
    {
      PostgreSqlStorage storage = CreateStorage();
      IMonitoringApi api = storage.GetMonitoringApi();
      Assert.NotNull(api);
    }

    [Fact]
    [CleanDatabase]
    public void GetComponents_ReturnsAllNeededComponents()
    {
      PostgreSqlStorage storage = CreateStorage();

      IEnumerable<IServerComponent> components = storage.GetComponents();

      Type[] componentTypes = components.Select(x => x.GetType()).ToArray();
      Assert.Contains(typeof(ExpirationManager), componentTypes);
    }

    private PostgreSqlStorage CreateStorage()
    {
      return new PostgreSqlStorage(ConnectionUtils.GetConnectionString(),
        _options);
    }
  }
}
