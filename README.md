﻿# Hangfire.PostgreSql

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

### Connecting to Azure Postgres Flexible Servers

To connect to Azure PostgreSQL Flexible Servers, use need to use

```csharp
services.AddHangfire(config =>
    config.UseAzurePostgreSqlStorage(c => Configuration.GetConnectionString("HangfireConnection"))
    );
```

This factory generates a data source builder which behind the scenes configured a periodic password provider. 
This provider will use DefaultAzureCredential to fetch a token depending on the environment.
If you need to customize the behavior, use the dataSourceBuilderSetup override. That one is called after the internal configuration.  

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

### Running Unit Tests

In order to run unit tests you need to setup a postgresql database. Simples way to do that is to use docker;

Environment configurations:
          POSTGRES_PASSWORD: postgres
          POSTGRES_HOST_AUTH_METHOD: trust
ports:
          - 5432:5432


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
