using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Write;
using WireMock.Matchers;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using WriteOptions = InfluxDB3.Client.Config.WriteOptions;

namespace InfluxDB3.Client.Test;

public class InfluxDBClientWriteTest : MockServerTest
{
    private InfluxDBClient _client;

    [TearDown]
    public new void TearDown()
    {
        _client?.Dispose();
    }

    [Test]
    public async Task Body()
    {
        _client = new InfluxDBClient(MockServerUrl, token: "my-token", organization: "my-org", database: "my-database");
        await WriteData();

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("mem,tag=a field=1"));
    }

    [Test]
    public async Task BodyConcat()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        _client = new InfluxDBClient(MockServerUrl, token: "my-token", organization: "my-org", database: "my-database");

        await _client.WriteRecordsAsync(new[] { "mem,tag=a field=1", "mem,tag=b field=2" });

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString,
            Is.EqualTo("mem,tag=a field=1\nmem,tag=b field=2"));
    }

    [Test]
    public async Task BodyPoint()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        _client = new InfluxDBClient(MockServerUrl, token: "my-token", organization: "my-org", database: "my-database");

        await _client.WritePointAsync(PointData.Measurement("cpu").SetTag("tag", "c").SetField("field", 1));

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("cpu,tag=c field=1i"));
    }

    [Test]
    public async Task BodyNull()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        _client = new InfluxDBClient(MockServerUrl, token: "my-token", organization: "my-org", database: "my-database");

        await _client.WriteRecordsAsync(new[] { "mem,tag=a field=1", null });

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("mem,tag=a field=1"));
    }

    [Test]
    public async Task BodyNonDefaultGzipped()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").WithHeader("Content-Encoding", "gzip").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            Organization = "my-org",
            Database = "my-database",
            WriteOptions = new WriteOptions
            {
                GzipThreshold = 1
            }
        });

        await _client.WriteRecordAsync("mem,tag=a field=1");
        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("mem,tag=a field=1"));
    }

    [Test]
    public async Task BodyDefaultNotGzipped()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").WithHeader("Content-Encoding", ".*", MatchBehaviour.RejectOnMatch).UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        _client = new InfluxDBClient(MockServerUrl, token: "my-token", organization: "my-org", database: "my-database");

        await _client.WriteRecordAsync("mem,tag=a field=1");
        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("mem,tag=a field=1"));
    }

    [Test]
    public void AlreadyDisposed()
    {
        _client = new InfluxDBClient(MockServerUrl, token: "my-token");
        _client.Dispose();
        var ae = Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await _client.WriteRecordAsync("mem,tag=a field=1");
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo($"Cannot access a disposed object.{Environment.NewLine}Object name: 'InfluxDBClient'."));
    }

    [Test]
    public async Task NotSpecifiedOrg()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        _client = new InfluxDBClient(MockServerUrl, token: "my-token", organization: null, database: "my-database");
        await _client.WriteRecordAsync("mem,tag=a field=1");

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query, Does.Not.ContainKey("org"));
    }

    [Test]
    public async Task DefaultTags()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            Organization = "my-org",
            Database = "my-database",
            WriteOptions = new WriteOptions
            {
                DefaultTags = new Dictionary<string, string>()
                {
                    { "tag1", "default" },
                    { "tag2", "default" },
                }
            }
        });

        await _client.WritePointAsync(PointData
            .Measurement("cpu")
            .SetTag("tag", "c")
            .SetTag("tag2", "c")
            .SetField("field", 1)
        );

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("cpu,tag=c,tag1=default,tag2=c field=1i"));
    }

    [Test]
    public async Task DatabaseCustom()
    {
        _client = new InfluxDBClient(MockServerUrl, token: "my-token", organization: "my-org", database: "my-database");
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1", database: "x-database");

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query?["bucket"].First(), Is.EqualTo("x-database"));
    }

    [Test]
    public void NotSpecifiedDatabase()
    {
        _client = new InfluxDBClient(MockServerUrl, token: "my-token", organization: "my-org");
        var ae = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await _client.WriteRecordAsync("mem,tag=a field=1");
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message,
            Is.EqualTo(
                "Please specify the 'database' as a method parameter or use default configuration at 'ClientConfig.Database'."));
    }

    [Test]
    public async Task PrecisionDefault()
    {
        _client = new InfluxDBClient(MockServerUrl, token: "my-token", organization: "my-org", database: "my-database");
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1", database: "my-database");

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query?["precision"].First(), Is.EqualTo("ns"));
    }

    [Test]
    public async Task PrecisionOptions()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            Organization = "my-org",
            Database = "my-database",
            WriteOptions = new WriteOptions
            {
                Precision = WritePrecision.Ms
            }
        });
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1");

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query?["precision"].First(), Is.EqualTo("ms"));
    }

    [Test]
    public async Task PrecisionCustom()
    {
        _client = new InfluxDBClient(MockServerUrl, token: "my-token", organization: "my-org", database: "my-database");
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1", precision: WritePrecision.S);

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query?["precision"].First(), Is.EqualTo("s"));
    }

    [Test]
    public async Task PrecisionBody()
    {
        _client = new InfluxDBClient(MockServerUrl, token: "my-token", organization: "my-org", database: "my-database");
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        var point = PointData.Measurement("h2o")
            .SetTag("location", "europe")
            .SetField("level", 2)
            .SetTimestamp(123_000_000_000L);

        await _client.WritePointAsync(point, precision: WritePrecision.S);

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("h2o,location=europe level=2i 123"));
    }

    [Test]
    public async Task Proxy()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            Organization = "my-org",
            Database = "my-database",
            Proxy = new WebProxy
            {
                Address = new Uri(MockProxyUrl),
                BypassProxyOnLocal = false
            }
        });
        MockProxy
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        var point = PointData.Measurement("h2o")
            .SetTag("location", "europe")
            .SetField("level", 2)
            .SetTimestamp(123_000_000_000L);

        await _client.WritePointAsync(point);

        var requests = MockProxy.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("h2o,location=europe level=2i 123000000000"));
    }

    [Test]
    public async Task CustomHeader()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            Organization = "my-org",
            Database = "my-database",
            Headers = new Dictionary<string, string>
            {
                { "X-device", "ab-01" },
            }
        });
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").WithHeader("X-device", "ab-01").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        var point = PointData.Measurement("h2o")
            .SetTag("location", "europe")
            .SetField("level", 2)
            .SetTimestamp(123_000_000_000L);

        await _client.WritePointAsync(point);

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("h2o,location=europe level=2i 123000000000"));
            Assert.That(requests[0].RequestMessage.Headers?["X-device"].First(), Is.EqualTo("ab-01"));
        });
    }

    [Test]
    public async Task CustomHeaderFromRequest()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            Organization = "my-org",
            Database = "my-database"
        });
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").WithHeader("X-Tracing-ID", "123").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        var point = PointData.Measurement("h2o")
            .SetTag("location", "europe")
            .SetField("level", 2)
            .SetTimestamp(123_000_000_000L);

        await _client.WritePointAsync(point, headers: new Dictionary<string, string>
        {
            { "X-Tracing-ID", "123" },
        });

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests, Has.Count.EqualTo(1));
        Assert.That(requests[0].RequestMessage.Headers?["X-Tracing-ID"].First(), Is.EqualTo("123"));
    }

    [Test]
    public async Task CustomHeaderFromRequestArePreferred()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            Organization = "my-org",
            Database = "my-database",
            Headers = new Dictionary<string, string>
            {
                { "X-Client-ID", "123" },
            }
        });
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").WithHeader("X-Client-ID", "456").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        var point = PointData.Measurement("h2o")
            .SetTag("location", "europe")
            .SetField("level", 2)
            .SetTimestamp(123_000_000_000L);

        await _client.WritePointAsync(point, headers: new Dictionary<string, string>
        {
            { "X-Client-ID", "456" },
        });

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests, Has.Count.EqualTo(1));
        Assert.That(requests[0].RequestMessage.Headers?["X-Client-ID"].First(), Is.EqualTo("456"));
    }

    private async Task WriteData()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1");
    }

    [Test]
    public async Task WriteNoSyncFalse()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            Database = "my-database",
            WriteOptions = new WriteOptions
            {
                NoSync = false
            }
        });
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1");

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Path, Is.EqualTo("/api/v2/write"));
    }

    [Test]
    public async Task WriteNoSyncTrueSupported()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            Database = "my-database",
            WriteOptions = new WriteOptions
            {
                NoSync = true
            }
        });
        MockServer
            .Given(Request.Create().WithPath("/api/v3/write_lp").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1");

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query?["no_sync"].First(), Is.EqualTo("true"));
    }

    [Test]
    public void WriteNoSyncTrueNotSupported()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            Database = "my-database",
            WriteOptions = new WriteOptions
            {
                NoSync = true
            }
        });
        MockServer
            .Given(Request.Create().WithPath("/api/v3/write_lp").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(HttpStatusCode.MethodNotAllowed));


        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.WriteRecordAsync("mem,tag=a field=1");
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.HttpResponseMessage, Is.Not.Null);
            Assert.That(ae.Message,
                Does.Contain(
                    "Server doesn't support write with NoSync=true (supported by InfluxDB 3 Core/Enterprise servers only)."));
        });
    }

    [Test]
    public void TimeoutExceededByTimeout()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204).WithDelay(TimeSpan.FromSeconds(2)));

        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            Database = "my-database",
            Timeout = TimeSpan.FromSeconds(1)
        });

        //fixme should be TaskCanceledException or TimeoutException
        var ae = Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await _client.WriteRecordAsync("mem,tag=a field=1");
        });

        Assert.That(ae, Is.Not.Null);
    }

    [Test]
    public Task TimeoutExceededByWriteTimeout()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204).WithDelay(TimeSpan.FromSeconds(2)));

        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            Database = "my-database",
            QueryTimeout = TimeSpan.FromSeconds(11),
            Timeout = TimeSpan.FromSeconds(11),
            WriteTimeout = TimeSpan.FromSeconds(1) // WriteTimeout has a higher priority than Timeout
        });
        var ae = Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await _client.WriteRecordAsync("mem,tag=a field=1");
        });

        Assert.That(ae, Is.Not.Null);
        return Task.CompletedTask;
    }

    [Test]
    public void TimeoutExceededByToken()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204).WithDelay(TimeSpan.FromSeconds(2)));

        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            Database = "my-database",
            QueryTimeout = TimeSpan.FromSeconds(11),
            Timeout = TimeSpan.FromSeconds(11),
            WriteTimeout = TimeSpan.FromSeconds(11)
        });
        var ae = Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await _client.WriteRecordAsync("mem,tag=a field=1", cancellationToken: new CancellationTokenSource(TimeSpan.FromSeconds(1)).Token);
        });

        Assert.That(ae, Is.Not.Null);
    }
}