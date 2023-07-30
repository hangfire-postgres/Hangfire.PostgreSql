using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Hangfire.PostgreSql.Utils;
using Hangfire.Server;
using Npgsql;

namespace Hangfire.PostgreSql;

#pragma warning disable CS0618
public class NotificationManager : IBackgroundProcess, IServerComponent
#pragma warning restore CS0618
{
  private const string JobNotificationChannel = "new_job";

  private readonly PostgreSqlStorage _storage;

  public AutoResetEvent NewJob { get; }

  public NotificationManager(PostgreSqlStorage storage)
  {
    _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    NewJob = new AutoResetEvent(false);
  }

  public void Execute(BackgroundProcessContext context)
  {
    Execute(context.StoppingToken);
  }

  public void Execute(CancellationToken cancellationToken)
  {
    if (_storage.Options.EnableLongPolling)
    {
      ListenForNotificationsAsync(cancellationToken).Wait(cancellationToken);
    }
  }

  public override string ToString()
  {
    return "PostgreSQL Notifications Manager";
  }

  private Task ListenForNotificationsAsync(CancellationToken cancellationToken)
  {
    NpgsqlConnection connection = _storage.CreateAndOpenConnection();

    try
    {
      if (!connection.SupportsNotifications())
      {
        return Task.CompletedTask;
      }

      // CreateAnOpenConnection can return the same connection over and over if an existing connection
      //  is passed in the constructor of PostgreSqlStorage. We must use a separate dedicated
      //  connection to listen for notifications.
      NpgsqlConnection clonedConnection = connection.CloneWith(connection.ConnectionString);

      return Task.Run(async () => {
        try
        {
          if (clonedConnection.State != ConnectionState.Open)
          {
            await clonedConnection.OpenAsync(cancellationToken); // Open so that Dapper doesn't auto-close.
          }

          while (!cancellationToken.IsCancellationRequested)
          {
            await clonedConnection.ExecuteAsync($"LISTEN {JobNotificationChannel}");
            await clonedConnection.WaitAsync(cancellationToken);
            NewJob.Set();
          }
        }
        catch (TaskCanceledException)
        {
          // Do nothing, cancellation requested so just end.
        }
        finally
        {
          _storage.ReleaseConnection(clonedConnection);
        }

      }, cancellationToken);

    }
    finally
    {
      _storage.ReleaseConnection(connection);
    }
  }

  public void NotifyNewJob(IDbConnection connection)
  {
    if (_storage.Options.EnableLongPolling)
    {
      connection.Execute($"NOTIFY {JobNotificationChannel}");
    }
  }
}