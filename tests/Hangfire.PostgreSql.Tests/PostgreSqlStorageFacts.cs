﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Transactions;
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
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new PostgreSqlStorage(connectionString: null));

      Assert.Equal("connectionString", exception.ParamName);
    }

    [Fact]
    public void Ctor_ThrowsAnException_WhenConnectionStringIsInvalid()
    {
      ArgumentException exception = Assert.Throws<ArgumentException>(() => new PostgreSqlStorage("hello", new PostgreSqlStorageOptions()));

      Assert.Equal("connectionString", exception.ParamName);
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
      PostgreSqlStorage storage = new(connection, _options);

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

    [Fact]
    public void Ctor_ThrowsAnException_WhenConnectionFactoryIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new PostgreSqlStorage(connectionFactory: null, new PostgreSqlStorageOptions()));
      Assert.Equal("connectionFactory", exception.ParamName);
    }

    [Fact]
    [CleanDatabase]
    public void Ctor_CanCreateSqlServerStorage_WithExistingConnectionFactory()
    {
      PostgreSqlStorage storage = new(new DefaultConnectionFactory(), _options);
      Assert.NotNull(storage);
    }

    [Fact]
    [CleanDatabase]
    public void CanCreateAndOpenConnection_WithExistingConnectionFactory()
    {
      PostgreSqlStorage storage = new(new DefaultConnectionFactory(), _options);
      NpgsqlConnection connection = storage.CreateAndOpenConnection();
      Assert.NotNull(connection);
    }

    [Fact]
    public void CreateAndOpenConnection_ThrowsAnException_WithExistingConnectionFactoryAndInvalidOptions()
    {
      PostgreSqlStorageOptions option = new() {
        EnableTransactionScopeEnlistment = false,
        PrepareSchemaIfNecessary = false,
      };
      PostgreSqlStorage storage = new(new DefaultConnectionFactory(), option);
      Assert.Throws<ArgumentException>(() => storage.CreateAndOpenConnection());
    }

    [Fact]
    public void CanUseTransaction_WithDifferentTransactionIsolationLevel()
    {
      using TransactionScope scope = new(TransactionScopeOption.Required,
        new TransactionOptions() { IsolationLevel = IsolationLevel.Serializable });
      
      PostgreSqlStorage storage = new(new DefaultConnectionFactory(), _options);
      NpgsqlConnection connection = storage.CreateAndOpenConnection();
      
      bool success = storage.UseTransaction(connection, (_, _) => true);
      
      Assert.True(success);
    }

    private PostgreSqlStorage CreateStorage()
    {
      return new PostgreSqlStorage(ConnectionUtils.GetConnectionString(), _options);
    }
  }
}
