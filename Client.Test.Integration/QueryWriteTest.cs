using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Query;
using InfluxDB3.Client.Write;
using NUnit.Framework;

using WriteOptions = InfluxDB3.Client.Config.WriteOptions;

namespace InfluxDB3.Client.Test.Integration;

public class QueryWriteTest
{
    private static readonly TraceListener ConsoleOutListener = new TextWriterTraceListener(Console.Out);

    private readonly string _host = Environment.GetEnvironmentVariable("TESTING_INFLUXDB_URL") ??
                                       throw new InvalidOperationException("TESTING_INFLUXDB_URL environment variable is not set.");
    private readonly string _token = Environment.GetEnvironmentVariable("TESTING_INFLUXDB_TOKEN") ??
                                         throw new InvalidOperationException("TESTING_INFLUXDB_TOKEN environment variable is not set.");
    private readonly string _database = Environment.GetEnvironmentVariable("TESTING_INFLUXDB_DATABASE") ??
                                        throw new InvalidOperationException("TESTING_INFLUXDB_DATABASE environment variable is not set.");

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

    [Test]
    public async Task QueryWrite()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = _host,
            Token = _token,
            Database = _database
        });

        const string measurement = "integration_test";
        var testId = DateTime.UtcNow.Millisecond;
        await client.WriteRecordAsync($"{measurement},type=used value=123.0,testId={testId}");

        var sql = $"SELECT value FROM {measurement} where \"testId\" = {testId}";
        await foreach (var row in client.Query(sql))
        {
            Assert.That(row, Has.Length.EqualTo(1));
            Assert.That(row[0], Is.EqualTo(123.0));
        }

        var results = await client.Query(sql).ToListAsync();
        Assert.That(results, Has.Count.EqualTo(1));

        var influxQL = $"select MEAN(value) from {measurement} where \"testId\" = {testId} group by time(1s) fill(none) order by time desc limit 1";
        results = await client.Query(influxQL, queryType: QueryType.InfluxQL).ToListAsync();
        Assert.That(results, Has.Count.EqualTo(1));
    }

    [Test]
    public void QueryNotAuthorized()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = _host,
            Database = _database
        });

        var ae = Assert.ThrowsAsync<RpcException>(async () =>
        {
            await foreach (var _ in client.Query("SELECT 1"))
            {
            }
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae?.Message, Contains.Substring("Unauthenticated"));
    }

    [Test]
    public async Task WriteDontFailForEmptyData()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = _host,
            Database = _database,
            Token = _token
        });

        await client.WritePointAsync(PointData.Measurement("cpu").SetTag("tag", "c"));
    }

    [Test]
    public async Task CanDisableCertificateValidation()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = _host,
            Database = _database,
            Token = _token,
            DisableServerCertificateValidation = true
        });

        await client.WritePointAsync(PointData.Measurement("cpu").SetTag("tag", "c"));
    }


    [Test]
    public async Task WriteDataGzipped()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = _host,
            Database = _database,
            Token = _token,
            WriteOptions = new WriteOptions
            {
                GzipThreshold = 1
            }
        });

        await client.WritePointAsync(PointData.Measurement("cpu").SetTag("tag", "c").SetField("user", 14.34));
    }
}