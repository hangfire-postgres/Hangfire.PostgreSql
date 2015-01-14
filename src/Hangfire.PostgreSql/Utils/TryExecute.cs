using System;
using Npgsql;

namespace Hangfire.PostgreSql
{
    public static partial class Utils
    {
        public static bool TryExecute(
            Action action,
            Func<Exception, bool> smoothExValidator = default(Func<Exception, bool>),
            int? tryCount = default(int?))
        {
            object futile;
            return TryExecute(() => { action(); return null; }, out futile, smoothExValidator, tryCount);
        }

        public static bool TryExecute<T>(
            Func<T> func,
            out T result,
            Func<Exception, bool> smoothExValidator = default(Func<Exception, bool>),
            int? tryCount = default(int?))
        {
            while (tryCount == default(int?) || tryCount-- > 0)
            {
                try
                {
                    result = func();
                    return true;
                }
                catch (Exception ex)
                {
                    if (smoothExValidator != null && !smoothExValidator(ex))
                    {
                        throw;
                    }
                }
            }

            result = default(T);
            return false;
        }
    }
}
