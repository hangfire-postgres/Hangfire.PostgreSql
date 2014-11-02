using System;
using Npgsql;

namespace Hangfire.PostgreSql.Tests
{
    public static class ConnectionUtils
    {
        private const string DatabaseVariable = "Hangfire_PostgreSql_DatabaseName";
        private const string ConnectionStringTemplateVariable 
            = "Hangfire_PostgreSql_ConnectionStringTemplate";

        private const string MasterDatabaseName = "postgres";
        private const string DefaultDatabaseName = @"hangfire_tests";
        private const string DefaultConnectionStringTemplate
            = @"Server=127.0.0.1;Port=5432;Database={0};Integrated Security=true;";

        public static string GetDatabaseName()
        {
            return Environment.GetEnvironmentVariable(DatabaseVariable) ?? DefaultDatabaseName;
        }

        public static string GetMasterConnectionString()
        {
            return String.Format(GetConnectionStringTemplate(), MasterDatabaseName);
        }

        public static string GetConnectionString()
        {
            return String.Format(GetConnectionStringTemplate(), GetDatabaseName());
        }

        private static string GetConnectionStringTemplate()
        {
            return Environment.GetEnvironmentVariable(ConnectionStringTemplateVariable)
                   ?? DefaultConnectionStringTemplate;
        }

        public static NpgsqlConnection CreateConnection()
        {
            NpgsqlConnectionStringBuilder csb = new NpgsqlConnectionStringBuilder(GetConnectionString());
            csb.Enlist = false;
            var connection = new NpgsqlConnection(csb.ToString());
            connection.Open();

            return connection;
        }
    }
}
