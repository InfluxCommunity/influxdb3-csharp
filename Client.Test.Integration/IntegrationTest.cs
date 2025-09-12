

using System;
using System.Diagnostics;
using NUnit.Framework;

namespace InfluxDB3.Client.Test.Integration;

public abstract class IntegrationTest
{
    private static readonly TraceListener ConsoleOutListener = new TextWriterTraceListener(Console.Out);

    protected string Host { get; private set; } = Environment.GetEnvironmentVariable("TESTING_INFLUXDB_URL") ??
                                                throw new InvalidOperationException(
                                                    "TESTING_INFLUXDB_URL environment variable is not set.");

    protected string Token { get; private set; } = Environment.GetEnvironmentVariable("TESTING_INFLUXDB_TOKEN") ??
                                                   throw new InvalidOperationException(
                                                       "TESTING_INFLUXDB_TOKEN environment variable is not set.");

    protected string Database { get; private set; } = Environment.GetEnvironmentVariable("TESTING_INFLUXDB_DATABASE") ??
                                                      throw new InvalidOperationException(
                                                          "TESTING_INFLUXDB_DATABASE environment variable is not set.");

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        if (!Trace.Listeners.Contains(ConsoleOutListener))
        {
            Console.SetOut(TestContext.Progress);
            Trace.Listeners.Add(ConsoleOutListener);
        }
    }

    [OneTimeTearDownAttribute]
    public void OneTimeTearDownAttribute()
    {
        ConsoleOutListener.Dispose();
    }
}