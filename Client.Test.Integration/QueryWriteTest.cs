using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
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
    [TestCase(null, "identity,gzip,deflate", "gzip", TestName = "QueryWithDisableGrpcCompression_NotSet")]
    [TestCase(false, "identity,gzip,deflate", "gzip", TestName = "QueryWithDisableGrpcCompression_False")]
    [TestCase(true, "identity", null, TestName = "QueryWithDisableGrpcCompression_True")]
    public async Task QueryWithDisableGrpcCompression(bool? disableGrpcCompression, string expectedReqEncoding,
        string? expectedRespEncoding)
    {
        // Custom handler to intercept and capture grpc encoding headers
        var headerInterceptor = new GrpcEncodingInterceptorHandler();
        var httpClient = new HttpClient(headerInterceptor);

        var queryOptions = new QueryOptions();
        if (disableGrpcCompression.HasValue)
        {
            queryOptions.DisableGrpcCompression = disableGrpcCompression.Value;
        }

        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database,
            DisableServerCertificateValidation = true,
            QueryOptions = queryOptions,
            HttpClient = httpClient,
        });

        const string measurement = "integration_test_compression";
        var testId = DateTime.UtcNow.Millisecond;
        await client.WriteRecordAsync($"{measurement},type=test value=42.0,testId={testId}");

        // Wait for data to be available (max 10 seconds)
        var sql = $"SELECT value FROM {measurement} where \"testId\" = {testId}";
        List<object?[]> results;
        for (var i = 0; i < 100 && (results = await client.Query(sql).ToListAsync()).Count == 0; i++)
            await Task.Delay(100);
        results = await client.Query(sql).ToListAsync();

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0][0], Is.EqualTo(42.0));

        // Verify the request grpc-accept-encoding header
        Assert.That(headerInterceptor.LastGrpcAcceptEncoding, Is.EqualTo(expectedReqEncoding),
            $"Request grpc-accept-encoding should be '{expectedReqEncoding}'");

        // Verify the response grpc-encoding header
        // Note: InfluxDB 3 Core may not compress responses even when client advertises gzip support.
        // Per gRPC spec, servers may choose not to compress regardless of client settings.
        // InfluxDB Cloud typically compresses, but Core may not. We warn instead of failing.
        // See: https://grpc.io/docs/guides/compression/
        var actualRespEncoding = headerInterceptor.LastGrpcEncoding;
        if (expectedRespEncoding != null)
        {
            if (actualRespEncoding != expectedRespEncoding)
            {
                Assert.Warn(
                    $"Server returned '{actualRespEncoding}' instead of '{expectedRespEncoding}'. " +
                    "This is normal for InfluxDB 3 Core which may not compress responses.");
            }
        }
        else
        {
            Assert.That(actualRespEncoding, Is.Null.Or.EqualTo("identity"),
                $"Expected no compression, got: {actualRespEncoding}");
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

        // Make sure the measurement exists
        var testId = DateTime.UtcNow.Millisecond;
        await client.WriteRecordAsync($"integration_test,type=used value=1234.0,testId={testId}");


        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            await foreach (var _ in client.Query("SELECT value FROM integration_test"))
            {
            }
        });
        Assert.That(ex?.StatusCode, Is.EqualTo(StatusCode.ResourceExhausted));
    }

    [Test]
    public async Task TimeoutExceededByDeadline()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database,
            WriteTimeout = TimeSpan.FromSeconds(11),
            QueryTimeout = TimeSpan.FromSeconds(11),
            QueryOptions = new QueryOptions()
            {
                Deadline = DateTime.UtcNow.AddTicks(1) // Deadline will have a higher priority than QueryTimeout
            }
        });
        await client.WriteRecordAsync("mem,tag=a field=1");
        TestQuery(client);
        TestQueryBatches(client);
        TestQueryPoints(client);
    }

    [Test]
    public async Task TimeoutExceededByQueryTimeout()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database,
            WriteTimeout = TimeSpan.FromSeconds(11),
            QueryTimeout = TimeSpan.FromTicks(1)
        });
        await client.WriteRecordAsync("mem,tag=a field=1");
        TestQuery(client);
        TestQueryBatches(client);
        TestQueryPoints(client);
    }

    [Test]
    public async Task TimeoutExceeded()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database,
            WriteTimeout = TimeSpan.FromSeconds(11),
            QueryTimeout = TimeSpan.FromSeconds(11),
            QueryOptions =
            {
                Deadline = DateTime.UtcNow.AddSeconds(11),
            }
        });
        await client.WriteRecordAsync("mem,tag=a field=1");
        var timeout = TimeSpan.FromTicks(1);
        TestQuery(client, timeout);
        TestQueryBatches(client, timeout);
        TestQueryPoints(client, timeout);
    }

    private static void TestQuery(InfluxDBClient client, TimeSpan? timeout = null)
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            await foreach (var _ in client.Query("SELECT * FROM mem", timeout: timeout))
            {
            }
        });
        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.DeadlineExceeded));
    }

    private static void TestQueryBatches(InfluxDBClient client, TimeSpan? timeout = null)
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            await foreach (var _ in client.QueryBatches("SELECT * FROM mem", timeout: timeout))
            {
            }
        });
        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.DeadlineExceeded));
    }

    private static void TestQueryPoints(InfluxDBClient client, TimeSpan? timeout = null)
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            await foreach (var _ in client.QueryPoints("SELECT * FROM mem", timeout: timeout))
            {
            }
        });
        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.DeadlineExceeded));
    }
}