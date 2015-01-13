using System;

namespace Hangfire.PostgreSql
{
    public static partial class Utils
    {
        public static bool TryExecute(Action action, int tryCount = 1, Action<Exception> onException = default(Action<Exception>))
        {
            object futile;
            return TryExecute(() => { action(); return null; }, out futile, tryCount, onException);
        }

        public static bool TryExecute<T>(Func<T> func, out T result, int tryCount = 1, Action<Exception> onException = default(Action<Exception>))
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
                    if (onException != null)
                    {
                        onException(ex);
                    }
                }
            }

            result = default(T);
            return false;
        }
    }
}
