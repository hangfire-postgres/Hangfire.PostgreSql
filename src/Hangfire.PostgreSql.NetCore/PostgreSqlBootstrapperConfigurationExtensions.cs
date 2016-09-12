// This file is part of Hangfire.PostgreSql.
// Copyright © 2014 Frank Hommers <http://hmm.rs/Hangfire.PostgreSql>.
// 
// Hangfire.PostgreSql is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.PostgreSql  is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.PostgreSql. If not, see <http://www.gnu.org/licenses/>.
//
// This work is based on the work of Sergey Odinokov, author of 
// Hangfire. <http://hangfire.io/>
//   
//    Special thanks goes to him.

namespace Hangfire.PostgreSql
{
	public static class PostgreSqlBootstrapperConfigurationExtensions
	{
		/// <summary>
		/// Tells the bootstrapper to use PostgreSQL as a job storage,
		/// that can be accessed using the given connection string or 
		/// its name.
		/// </summary>
		/// <param name="configuration">Configuration</param>
		/// <param name="nameOrConnectionString">Connection string or its name</param>
		public static PostgreSqlStorage UsePostgreSqlStorage(
			this GlobalConfiguration configuration,
			string nameOrConnectionString)
		{
			var storage = new PostgreSqlStorage(nameOrConnectionString);
			configuration.UseStorage(storage);

			return storage;
		}

		/// <summary>
		/// Tells the bootstrapper to use PostgreSQL as a job storage
		/// with the given options, that can be accessed using the specified
		/// connection string or its name.
		/// </summary>
		/// <param name="configuration">Configuration</param>
		/// <param name="nameOrConnectionString">Connection string or its name</param>
		/// <param name="options">Advanced options</param>
		public static PostgreSqlStorage UsePostgreSqlStorage(
			this GlobalConfiguration configuration,
			string nameOrConnectionString,
			PostgreSqlStorageOptions options)
		{
			var storage = new PostgreSqlStorage(nameOrConnectionString, options);
			configuration.UseStorage(storage);

			return storage;
		}
	}
}