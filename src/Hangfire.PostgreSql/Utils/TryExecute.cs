using System;
using Npgsql;

namespace Hangfire.PostgreSql
{
    public static partial class Utils
    {
        public static bool TryExecute(Action action, int tryCount = 1, Func<Exception, bool> smoothExValidator = default(Func<Exception, bool>))
        {
            object futile;
            return TryExecute(() => { action(); return null; }, out futile, tryCount, smoothExValidator);
        }

        public static bool TryExecute<T>(Func<T> func, out T result, int tryCount = 1, Func<Exception, bool> smoothExValidator = default(Func<Exception, bool>))
        {
            while (tryCount-- > 0)
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
