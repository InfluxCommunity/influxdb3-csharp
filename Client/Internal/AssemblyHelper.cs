using System;
using System.Reflection;

namespace InfluxDB3.Client.Internal
{
    internal static class AssemblyHelper
    {
        internal static string GetVersion()
        {
            try
            {
                return typeof(InfluxDBClient)
                    .GetTypeInfo()
                    .Assembly
                    .GetCustomAttribute<AssemblyFileVersionAttribute>()
                    .Version;
            }
            catch (Exception)
            {
                return "unknown";
            }
        }
    }
}