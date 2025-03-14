using InfluxDB3.Examples.Downsampling;
using InfluxDB3.Examples.IOx;
using InfluxDB3.Examples.CustomSslCerts;

namespace InfluxDB3.Examples.General;

public class Runner
{

    public static string Host { get; private set; } = Environment.GetEnvironmentVariable("INFLUXDB_URL") ??
                                                  "http://localhost:8086";
    public static string Token { get; private set; } = Environment.GetEnvironmentVariable("INFLUXDB_TOKEN") ??
                                                   "my-token";

    public static string Database { get; private set; } = Environment.GetEnvironmentVariable("INFLUXDB_DATABASE") ??
                                                      "my-database";

    private static readonly Dictionary<string, Func<Task>> Functions = new Dictionary<string, Func<Task>>()
    {
        {"DownSampling", DownsamplingExample.Run},
        {"HttpErrorHandled", HttpErrorHandled.Run},
        {"IOx", IOxExample.Run},
        {"CustomSslCerts", CustomSslCertsExample.Run}
    };
    private static void Help()
    {
        Console.WriteLine("Usage:");
        Console.WriteLine("  General.exe <ExampleToRun>");
        Console.WriteLine("    Available examples:");
        foreach (var key in Functions.Keys)
        {
            Console.WriteLine($"      {key}");
        }
    }

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Starting runner... args {0}", args.Length);
        if (args.Length != 1)
        {
            Console.WriteLine("InfluxDB Example Runner requires a single argument.");
            Help();
            Environment.Exit(1);
        }

        try
        {
            await Functions[args[0]]();
        }
        catch (KeyNotFoundException)
        {
            Console.WriteLine($"Unknown example: {args[0]}");
            Help();
            Environment.Exit(1);
        }
    }
}