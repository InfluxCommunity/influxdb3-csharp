using System;
using System.Collections.Generic;

namespace InfluxDB3.Client.Test.Utils;

public static class TestUtils
{
    public static void SetEnv(IDictionary<String, String> dict)
    {
        foreach (var entry in dict)
        {
            Environment.SetEnvironmentVariable(entry.Key, entry.Value, EnvironmentVariableTarget.Process);
        }
    }

    public static void CleanupEnv()
    {
        var env = Environment.GetEnvironmentVariables(EnvironmentVariableTarget.Process);
        foreach (var key in env.Keys)
        {
            if (((string)key).StartsWith("INFLUX_"))
            {
                Environment.SetEnvironmentVariable((string)key, null, EnvironmentVariableTarget.Process);
            }
        }
    }
}