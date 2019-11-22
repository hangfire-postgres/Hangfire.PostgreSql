using System;
using Npgsql;

namespace Hangfire.PostgreSql.Tests
{
	public static class ConnectionUtils
	{
		private const string DatabaseVariable = "Hangfire_PostgreSql_DatabaseName";
		private const string SchemaVariable = "Hangfire_PostgreSql_SchemaName";

		private const string ConnectionStringTemplateVariable = "Hangfire_PostgreSql_ConnectionStringTemplate";

		private const string MasterDatabaseName = "postgres";
		private const string DefaultDatabaseName = @"hangfire_tests";
		private const string DefaultSchemaName = @"hangfire";

        private const string DefaultConnectionStringTemplate = @"Server=127.0.0.1;Port=5432;Database=postgres;User Id=postgres;Password=password;";

        public static string GetDatabaseName()
		{
			return Environment.GetEnvironmentVariable(DatabaseVariable) ?? DefaultDatabaseName;
		}

		public static string GetSchemaName()
		{
			return Environment.GetEnvironmentVariable(SchemaVariable) ?? DefaultSchemaName;
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
            var csb = new NpgsqlConnectionStringBuilder(GetConnectionString());
			var connection = new NpgsqlConnection
			{
                ConnectionString = csb.ToString()
            };
			connection.Open();

			return connection;
		}

	    public static NpgsqlConnection CreateMasterConnection()
	    {
            var csb = new NpgsqlConnectionStringBuilder(GetMasterConnectionString());
	        var connection = new NpgsqlConnection
	        {
	            ConnectionString = csb.ToString()
	        };
	        connection.Open();

	        return connection;
	    }
    }
}