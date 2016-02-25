using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlInstallerFacts
    {

        [Fact]
        public void InstallingSchemaShouldNotThrowAnException()
        {
	        var ex = Record.Exception(() =>
	        {
				UseConnection(connection =>
				{
					string schemaName = "hangfire_tests_" + System.Guid.NewGuid().ToString().Replace("-", "_").ToLower();

					PostgreSqlObjectsInstaller.Install(connection, schemaName);

					connection.Execute(string.Format(@"DROP SCHEMA ""{0}"" CASCADE;", schemaName));
				});
	        });

            Assert.Null(ex);
        }


        private static void UseConnection(Action<NpgsqlConnection> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
}
