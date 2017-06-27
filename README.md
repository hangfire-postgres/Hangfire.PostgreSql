
Hangfire.PostgreSql
===================
[![Build status](https://ci.appveyor.com/api/projects/status/a01vpyliv5mh9xac/branch/master?svg=true)](https://ci.appveyor.com/project/vytautask/hangfire-postgresql-lel5h/branch/master)

This is an plugin to the Hangfire to enable PostgreSQL as a storage system.
Read about hangfire here: https://github.com/HangfireIO/Hangfire#hangfire-
and here: http://hangfire.io/

Instructions
------------
Install Hangfire, see https://github.com/HangfireIO/Hangfire#installation

Download all files from this repository, add the Hangfire.PostgreSql.csproj to your solution.
Reference it in your project, and you are ready to go by using:

```csharp
app.UseHangfireServer(new BackgroundJobServerOptions(), 
  new PostgreSqlStorage("<connection string or its name>"));
app.UseHangfireDashboard();
```


Related Projects
-----------------

* [Hangfire.Core](https://github.com/HangfireIO/Hangfire)

License
========

Copyright © 2014-2017 Frank Hommers http://hmm.rs/Hangfire.PostgreSql and others (Burhan Irmikci (barhun), Zachary Sims(zsims), kgamecarter, Stafford Williams (staff0rd), briangweber, Viktor Svyatokha (ahydrax), Christopher Dresel (Dresel), Vytautas Kasparavičius (vytautask).

Hangfire.PostgreSql is an Open Source project licensed under the terms of the LGPLv3 license. Please see http://www.gnu.org/licenses/lgpl-3.0.html for license text or COPYING.LESSER file distributed with the source code.

This work is based on the work of Sergey Odinokov, author of Hangfire. <http://hangfire.io/>

