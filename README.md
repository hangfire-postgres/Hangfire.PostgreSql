# Hangfire.PostgreSql

[![Build status](https://github.com/hangfire-postgres/Hangfire.PostgreSql/actions/workflows/pack.yml/badge.svg)](https://github.com/hangfire-postgres/Hangfire.PostgreSql/actions/workflows/pack.yml) [![GitHub release (latest by date)](https://img.shields.io/github/v/release/hangfire-postgres/Hangfire.PostgreSql?label=Release)](https://github.com/hangfire-postgres/Hangfire.PostgreSql/releases/latest) [![Nuget](https://img.shields.io/nuget/v/Hangfire.PostgreSql?label=NuGet)](https://www.nuget.org/packages/Hangfire.PostgreSql)

This is an plugin to the Hangfire to enable PostgreSQL as a storage system.
Read about hangfire here: https://github.com/HangfireIO/Hangfire#overview
and here: http://hangfire.io/

## Instructions

### For .NET

Install Hangfire, see https://github.com/HangfireIO/Hangfire#installation

Download all files from this repository, add the Hangfire.PostgreSql.csproj to your solution.
Reference it in your project, and you are ready to go by using:

```csharp
app.UseHangfireServer(new BackgroundJobServerOptions(),
  new PostgreSqlStorage("<connection string or its name>"));
app.UseHangfireDashboard();
```

### For ASP.NET Core

First, NuGet package needs installation.

- Hangfire.AspNetCore
- Hangfire.PostgreSql (Uses Npgsql 6)
- Hangfire.PostgreSql.Npgsql5 (Uses Npgsql 5)

Historically both packages were functionally the same up until the package version 1.9.11, the only difference was the underlying Npgsql dependency version. Afterwards, the support for Npgsql v5 has been dropped and now minimum required version is 6.0.0.

In `Startup.cs` _ConfigureServices(IServiceCollection services)_ method add the following line:

```csharp
services.AddHangfire(config =>
    config.UsePostgreSqlStorage(c =>
        c.UseNpgsqlConnection(Configuration.GetConnectionString("HangfireConnection"))));
```

In Configure method, add these two lines:

```csharp
app.UseHangfireServer();
app.UseHangfireDashboard();
```

And... That's it. You are ready to go.

If you encounter any issues/bugs or have idea of a feature regarding Hangfire.Postgresql, [create us an issue](https://github.com/hangfire-postgres/Hangfire.PostgreSql/issues/new). Thanks!

### Enabling SSL support

SSL support can be enabled for Hangfire.PostgreSql library using the following mechanism:

```csharp
config.UsePostgreSqlStorage(c =>
    c.UseNpgsqlConnection(
        Configuration.GetConnectionString("HangfireConnection"), // connection string,
        connection => // connection setup - gets called after instantiating the connection and before any calls to DB are made
        {
            connection.ProvideClientCertificatesCallback += clientCerts =>
            {
                clientCerts.Add(X509Certificate.CreateFromCertFile("[CERT_FILENAME]"));
            };
        }
    )
);
```
### Queue processing

Similar to `Hangfire.SqlServer`, queues are processed in alphabetical order. Given the following example

```csharp
var options = new BackgroundJobServerOptions
{
    Queues = new[] { "general-queue", "very-fast-queue", "a-long-running-queue" }
};
app.UseHangfireServer(options);
```

this provider would first process jobs in `a-long-running-queue`, then `general-queue` and lastly `very-fast-queue`.

### Startup resilience and transient database outages

Starting from version 1.x (where `PostgreSqlStorageOptions` gained startup resilience options),
the storage tries to be more tolerant to *transient* PostgreSQL outages during application startup.

#### Default behavior

By default, when you use the new-style configuration:

```csharp
services.AddHangfire((provider, config) =>
{
    config.UsePostgreSqlStorage(opts =>
        opts.UseNpgsqlConnection(Configuration.GetConnectionString("HangfireConnection")));
});

app.UseHangfireServer();
app.UseHangfireDashboard();
```

`PostgreSqlStorageOptions` uses the following defaults for startup resilience:

- `PrepareSchemaIfNecessary = true`
- `StartupConnectionMaxRetries = 5`
- `StartupConnectionBaseDelay = 1 second`
- `StartupConnectionMaxDelay = 1 minute`
- `AllowDegradedModeWithoutStorage = true`

With these defaults:

1. On application startup, when schema preparation is required, the storage will try to open a connection and install/upgrade the schema.
2. If the database is temporarily unavailable, it will retry the operation up to 6 (initial + retries) times with exponential backoff between attempts, capped by `StartupConnectionMaxDelay`.
3. If all attempts fail **during startup**, the storage enters a *degraded* state instead of crashing the whole process. Your ASP.NET Core application can still start and serve other endpoints that do not depend on Hangfire.
4. On the *first actual use* of the storage (e.g. dashboard, background job server), Hangfire will try to initialize again. If the database is available by then, initialization succeeds and everything works as usual. If it is still unavailable, an `InvalidOperationException` with the original database exception as `InnerException` is thrown at that call site.

This behavior is designed to make applications more robust in scenarios where the database may briefly lag behind the application during deployments or orchestrated restarts.

#### Opting out of resilient startup (fail fast)

If you prefer to fail the whole process immediately if the database is not reachable during startup – you can disable retries by setting `StartupConnectionMaxRetries` to `0`:

```csharp
var storageOptions = new PostgreSqlStorageOptions
{
    PrepareSchemaIfNecessary = true,
    StartupConnectionMaxRetries = 0,          // disables resilient startup
    AllowDegradedModeWithoutStorage = false,  // fail fast if DB is down at startup
};

services.AddHangfire((provider, config) =>
{
    config.UsePostgreSqlStorage(opts =>
        opts.UseNpgsqlConnection(Configuration.GetConnectionString("HangfireConnection")),
        storageOptions);
});
```

With this configuration:

- A single attempt is made to open a connection and prepare the schema.
- If that attempt fails, the storage constructor throws and application startup fails.

#### Controlling degraded mode

Degraded mode is controlled via `AllowDegradedModeWithoutStorage`:

- `true` (default): if all startup attempts fail, the storage constructor will *not* throw. Instead, it records the last failure and leaves storage uninitialized. The first use of the storage will re-run initialization and either succeed (once the DB is up) or throw.
- `false`: if all startup attempts fail, the storage constructor will throw an `InvalidOperationException("Failed to initialize Hangfire PostgreSQL storage.", innerException)`.

For example, to keep retries but still fail startup if the DB never becomes available:

```csharp
var storageOptions = new PostgreSqlStorageOptions
{
    PrepareSchemaIfNecessary = true,
    StartupConnectionMaxRetries = 10,         // more aggressive retry policy
    StartupConnectionBaseDelay = TimeSpan.FromSeconds(2),
    StartupConnectionMaxDelay = TimeSpan.FromMinutes(2),
    AllowDegradedModeWithoutStorage = false,  // do not start the app without storage
};
```

#### Turning off schema preparation entirely

If you manage the Hangfire schema yourself (for example via migrations or a dedicated deployment step) and do not want the storage to touch the database during startup or on first use, set `PrepareSchemaIfNecessary = false`:

```csharp
var storageOptions = new PostgreSqlStorageOptions
{
    PrepareSchemaIfNecessary = false, // no schema installation/upgrade
};
```

In this case:

- No schema initialization is performed by `PostgreSqlStorage`.
- The first query that actually needs the database will fail if the schema is missing or mismatched, so you must ensure it is created/updated out of band.

> Note: startup resilience settings (`StartupConnectionMaxRetries`, `AllowDegradedModeWithoutStorage`, etc.) only apply when `PrepareSchemaIfNecessary` is `true`.

### License

Copyright © 2014-2024 Frank Hommers https://github.com/hangfire-postgres/Hangfire.PostgreSql.

Collaborators:
Frank Hommers (frankhommers), Vytautas Kasparavičius (vytautask), Žygimantas Arūna (azygis) 

Contributors:
Andrew Armstrong (Plasma), Burhan Irmikci (barhun), Zachary Sims(zsims), kgamecarter, Stafford Williams (staff0rd), briangweber, Viktor Svyatokha (ahydrax), Christopher Dresel (Dresel), Vincent Vrijburg, David Roth (davidroth) and Ivan Tiniakov (Tinyakov).

Hangfire.PostgreSql is an Open Source project licensed under the terms of the LGPLv3 license. Please see http://www.gnu.org/licenses/lgpl-3.0.html for license text or COPYING.LESSER file distributed with the source code.

This work is based on the work of Sergey Odinokov, author of Hangfire. <http://hangfire.io/>

### Related Projects

- [Hangfire.Core](https://github.com/HangfireIO/Hangfire)
