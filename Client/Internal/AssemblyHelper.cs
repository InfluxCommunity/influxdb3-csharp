using System.Reflection;

namespace InfluxDB3.Client.Internal
{
    internal static class AssemblyHelper
    {
        internal static string GetVersion()
        {
            return typeof(InfluxDBClient)
                .GetTypeInfo()
                .Assembly
                .GetCustomAttribute<AssemblyFileVersionAttribute>()
                .Version;
        }
    }
}