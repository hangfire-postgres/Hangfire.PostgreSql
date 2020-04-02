
# Hangfire.PostgreSql
[![Build status](https://ci.appveyor.com/api/projects/status/a01vpyliv5mh9xac/branch/master?svg=true)](https://ci.appveyor.com/project/vytautask/hangfire-postgresql-lel5h/branch/master)

This is an plugin to the Hangfire to enable PostgreSQL as a storage system.
Read about hangfire here: https://github.com/HangfireIO/Hangfire#hangfire-
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
First, additional NuGet packages needs installation: 
* Hangfire
* Hangfire.AspNetCore
* Hangfire.PostgreSql


In `Startup.cs` _ConfigureServices(IServiceCollection services)_ method add the following line:
```csharp
services.AddHangfire(config =>
		        config.UsePostgreSqlStorage(Configuration.GetConnectionString("HangfireConnection")));
```

In Configure method, add these two lines:
```csharp
app.UseHangfireServer();
app.UseHangfireDashboard();
```
And... That's it. You are ready to go. Also there exists sample application [here](https://github.com/frankhommers/Hangfire.PostgreSql/releases/download/1.4.8.1/aspnetcore_hangfire_sample.zip).

If you encounter any issues/bugs or have idea of a feature regarding Hangfire.Postgresql, [create us an issue](https://github.com/frankhommers/Hangfire.PostgreSql/issues/new). Thanks! 


### Enabling SSL support
SSL support can be enabled for Hangfire.PostgreSql library using the following mechanism:
```csharp
config.UsePostgreSqlStorage(new DefaultConnectionBuilder(
    options.HangfireDatabaseConnectionString,
    connection =>
    {
        connection.ProvideClientCertificatesCallback += clientCerts =>
        {
            clientCerts.Add(X509Certificate.CreateFromCertFile("[CERT_FILENAME]"));
        };
    }));
```

### License
Copyright © 2014-2020 Frank Hommers http://hmm.rs/Hangfire.PostgreSql and others (Burhan Irmikci (barhun), Zachary Sims(zsims), kgamecarter, Stafford Williams (staff0rd), briangweber, Viktor Svyatokha (ahydrax), Christopher Dresel (Dresel), Vytautas Kasparavičius (vytautask), Vincent Vrijburg, David Roth (davidroth).

Hangfire.PostgreSql is an Open Source project licensed under the terms of the LGPLv3 license. Please see http://www.gnu.org/licenses/lgpl-3.0.html for license text or COPYING.LESSER file distributed with the source code.

This work is based on the work of Sergey Odinokov, author of Hangfire. <http://hangfire.io/>

### Related Projects
* [Hangfire.Core](https://github.com/HangfireIO/Hangfire)
