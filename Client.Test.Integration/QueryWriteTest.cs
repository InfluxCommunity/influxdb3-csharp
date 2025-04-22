using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Query;
using InfluxDB3.Client.Write;
using NUnit.Framework;
using WriteOptions = InfluxDB3.Client.Config.WriteOptions;

namespace InfluxDB3.Client.Test.Integration;

public class QueryWriteTest : IntegrationTest
{

    [Test]
    public async Task QueryWrite()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database
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

        var points = await client.QueryPoints(sql).ToListAsync();
        Assert.That(points, Has.Count.EqualTo(1));
        Assert.That(points.First().GetField("value"), Is.EqualTo(123.0));

        points = await client.QueryPoints($"SELECT * FROM {measurement} where \"testId\" = {testId}").ToListAsync();
        Assert.That(points, Has.Count.EqualTo(1));
        Assert.That(points.First().GetField("value"), Is.EqualTo(123.0));
        Assert.That(points.First().GetTag("type"), Is.EqualTo("used"));
    }

    [Test]
    public void QueryNotAuthorized()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Database = Database
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
            Host = Host,
            Database = Database,
            Token = Token
        });

        await client.WritePointAsync(PointData.Measurement("cpu").SetTag("tag", "c"));
    }

    [Test]
    public async Task CanDisableCertificateValidation()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Database = Database,
            Token = Token,
            DisableServerCertificateValidation = true
        });

        await client.WritePointAsync(PointData.Measurement("cpu").SetTag("tag", "c"));
    }

    [Test]
    public async Task WriteDataGzipped()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Database = Database,
            Token = Token,
            WriteOptions = new WriteOptions
            {
                GzipThreshold = 1
            }
        });

        await client.WritePointAsync(PointData.Measurement("cpu").SetTag("tag", "c").SetField("user", 14.34));
    }

    [Test]
    public async Task QueryWriteParameters()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database
        });

        var testId = DateTime.UtcNow.Millisecond;
        await client.WriteRecordAsync($"integration_test,type=used value=1234.0,testId={testId}");

        const string sql = "SELECT value FROM integration_test where \"testId\" = $testId";
        await foreach (var row in client.Query(sql, namedParameters: new Dictionary<string, object>
                       {
                           { "testId", testId },
                       }))
        {
            Assert.That(row, Has.Length.EqualTo(1));
            Assert.That(row[0], Is.EqualTo(1234.0));
        }
    }

    [Test]
    public async Task MaxReceiveMessageSize()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database,
            QueryOptions = new QueryOptions()
            {
                MaxReceiveMessageSize = 100
            }
        });

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            await foreach (var _ in client.Query("SELECT value FROM integration_test"))
            {
            }
        });
        Assert.That(ex?.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));;
    }

    [Test]
    public async Task MaxSendMessageSize()
    {
        //todo write test, should I test this

        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database,
            QueryOptions = new QueryOptions()
            {
                MaxSendMessageSize = 0
            }
        });

        await client.WritePointAsync(PointData.Measurement("cpu").SetTag("tag", "c"));
        // var ex = Assert.ThrowsAsync<RpcException>(async () =>
        // {
        //     await client.WritePointAsync(PointData.Measurement("cpu").SetTag("tag", "c"));
        // });
        // Assert.That(ex?.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));;
    }

    [Test]
    public async Task GrpcDeadline()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database,
            QueryOptions = new QueryOptions()
            {
                Deadline = DateTime.UtcNow.AddMicroseconds(1)
            }
        });

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            await foreach (var _ in client.Query("SELECT value FROM stat"))
            {
            }
        });
        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.DeadlineExceeded));;;
    }
}