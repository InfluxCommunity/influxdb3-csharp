using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Grpc.Core;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Write;
using NUnit.Framework;

namespace InfluxDB3.Client.Test.Integration;

public class QueryWriteTest
{
    private static readonly TraceListener ConsoleOutListener = new TextWriterTraceListener(Console.Out);

    private readonly IContainer?[] _dockerContainers =
    {
        Environment.GetEnvironmentVariable("FLIGHT_SQL_URL") is null
            ? new ContainerBuilder()
                .WithImage("voltrondata/flight-sql:latest")
                .WithAutoRemove(true)
                .WithPortBinding(31337, 31337)
                .WithEnvironment(new Dictionary<string, string>()
                {
                    { "FLIGHT_PASSWORD", "flight_password" },
                    { "PRINT_QUERIES", "1" }
                })
                .Build()
            : null,
        Environment.GetEnvironmentVariable("INFLUXDB_URL") is null
            ? new ContainerBuilder()
                .WithImage("influxdb:latest")
                .WithAutoRemove(true)
                .WithPortBinding(8086, 8086)
                .WithEnvironment(new Dictionary<string, string>()
                {
                    { "DOCKER_INFLUXDB_INIT_MODE", "setup" },
                    { "DOCKER_INFLUXDB_INIT_USERNAME", "my-user" },
                    { "DOCKER_INFLUXDB_INIT_PASSWORD", "my-password" },
                    { "DOCKER_INFLUXDB_INIT_ORG", "my-org" },
                    { "DOCKER_INFLUXDB_INIT_BUCKET", "my-bucket" },
                    { "DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", "my-token" },
                })
                .Build()
            : null
    };

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        if (!Trace.Listeners.Contains(ConsoleOutListener))
        {
            Console.SetOut(TestContext.Progress);
            Trace.Listeners.Add(ConsoleOutListener);
        }

        foreach (var dockerContainer in _dockerContainers)
        {
            if (dockerContainer is not null)
            {
                await dockerContainer.StartAsync();
            }

            // wait to start
            await Task.Delay(TimeSpan.FromSeconds(5));
        }
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        foreach (var dockerContainer in _dockerContainers)
        {
            if (dockerContainer is not null)
            {
                await dockerContainer.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task Query()
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
            HostUrl = Environment.GetEnvironmentVariable("FLIGHT_SQL_URL") ?? "https://localhost:31337",
            Database = "database",
            DisableServerCertificateValidation = true,
            Headers = headers
        });

        var index = 0;
        await foreach (var row in client.Query("SELECT * FROM nation"))
        {
            var nation = row[1];
            if (index == 0)
            {
                Assert.That(nation, Is.EqualTo("ALGERIA"));
            }

            Trace.WriteLine(nation);
            index++;
        }
    }

    [Test]
    public void QueryNotAuthorized()
    {
        using var client = new InfluxDBClient(new InfluxDBClientConfigs
        {
            HostUrl = Environment.GetEnvironmentVariable("FLIGHT_SQL_URL") ?? "https://localhost:31337",
            Database = "database",
            DisableServerCertificateValidation = true,
            AuthToken = "my-token"
        });

        var ae = Assert.ThrowsAsync<RpcException>(async () =>
        {
            await foreach (var _ in client.Query("SELECT 1"))
            {
            }
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae?.Message, Contains.Substring("Invalid bearer token"));
    }

    [Test]
    public async Task Write()
    {
        using var client = new InfluxDBClient(new InfluxDBClientConfigs
        {
            HostUrl = Environment.GetEnvironmentVariable("INFLUXDB_URL") ?? "http://localhost:8086",
            Database = "my-bucket",
            Organization = "my-org",
            AuthToken = "my-token",
            DisableServerCertificateValidation = true
        });

        await client.WriteRecordAsync("mem,type=used value=1.0");
    }

    [Test]
    public async Task WriteDontFailForEmptyData()
    {
        using var client = new InfluxDBClient(new InfluxDBClientConfigs
        {
            HostUrl = Environment.GetEnvironmentVariable("INFLUXDB_URL") ?? "http://localhost:8086",
            Database = "my-bucket",
            Organization = "my-org",
            AuthToken = "my-token",
            DisableServerCertificateValidation = true
        });

        await client.WritePointAsync(PointData.Measurement("cpu").AddTag("tag", "c"));
    }
}