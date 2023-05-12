using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Grpc.Core;
using InfluxDB3.Client.Config;
using Microsoft.Data.Analysis;
using NUnit.Framework;

namespace InfluxDB3.Client.Test.Integration;

public class QueryTest
{
    private static readonly TraceListener ConsoleOutListener = new TextWriterTraceListener(Console.Out);

    private readonly IContainer? _flightSqlContainer = Environment.GetEnvironmentVariable("FLIGHT_SQL_URL") is null ? new ContainerBuilder()
        .WithImage("voltrondata/flight-sql:latest")
        .WithAutoRemove(true)
        .WithPortBinding(31337, 31337)
        .WithEnvironment(new Dictionary<string, string>()
        {
            { "FLIGHT_PASSWORD", "flight_password" },
            { "PRINT_QUERIES", "1" }
        })
        .Build() : null;


    [OneTimeSetUp]
    public async Task StartContainer()
    {
        if (!Trace.Listeners.Contains(ConsoleOutListener))
        {
            Console.SetOut(TestContext.Progress);
            Trace.Listeners.Add(ConsoleOutListener);
        }

        if (_flightSqlContainer is not null)
        {
            await _flightSqlContainer.StartAsync();
            // wait to start
            await Task.Delay(TimeSpan.FromSeconds(5));
        }
    }

    [OneTimeTearDown]
    public async Task StopContainer()
    {
        if (_flightSqlContainer is not null)
        {
            await _flightSqlContainer.StopAsync();
        }
    }

    [Test]
    public async Task SimpleQuery()
    {
        var headers = new Metadata
        {
            {
                "Authorization",
                "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("flight_username:flight_password"))
            }
        };

        using var client = new InfluxDBClient(new InfluxDBClientConfigs
        {
            Host = Environment.GetEnvironmentVariable("FLIGHT_SQL_URL") ?? "https://localhost:31337",
            Database = "database",
            DisableServerCertificateValidation = true,
            Headers = headers
        });

        await foreach (var recordBatch in client.Query("SELECT * FROM nation"))
        {
            var dataFrame = DataFrame.FromArrowRecordBatch(recordBatch);

            var nation = dataFrame.Rows.First()[1];
            Assert.That(nation, Is.EqualTo("ALGERIA"));

            Trace.Write(dataFrame.ToString());
        }
    }
}