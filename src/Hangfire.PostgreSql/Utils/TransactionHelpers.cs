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

using System;
using System.Transactions;

namespace Hangfire.PostgreSql.Utils;

public static class TransactionHelpers
{
  internal static TransactionScope CreateTransactionScope(IsolationLevel? isolationLevel = IsolationLevel.ReadCommitted, bool enlist = true, TimeSpan? timeout = null)
  {
    TransactionScopeOption scopeOption = TransactionScopeOption.RequiresNew;
    if (enlist)
    {
      Transaction currentTransaction = Transaction.Current;
      if (currentTransaction != null)
      {
        isolationLevel = currentTransaction.IsolationLevel;
        scopeOption = TransactionScopeOption.Required;
      }
    }

    return new TransactionScope(
      scopeOption,
      new TransactionOptions {
        IsolationLevel = isolationLevel.GetValueOrDefault(IsolationLevel.ReadCommitted),
        Timeout = timeout.GetValueOrDefault(TransactionManager.DefaultTimeout),
      });
  }
}