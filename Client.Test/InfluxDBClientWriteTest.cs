using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Write;
using WireMock.Matchers;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;

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
        _client = new InfluxDBClient(MockServerUrl, organization: "org", database: "database");
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

        _client = new InfluxDBClient(MockServerUrl, organization: "org", database: "database");

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

        _client = new InfluxDBClient(MockServerUrl, organization: "org", database: "database");

        await _client.WritePointAsync(PointData.Measurement("cpu").AddTag("tag", "c").AddField("field", 1));

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("cpu,tag=c field=1i"));
    }

    [Test]
    public async Task BodyNull()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        _client = new InfluxDBClient(MockServerUrl, organization: "org", database: "database");

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

        _client = new InfluxDBClient(new InfluxDBClientConfigs
        {
            HostUrl = MockServerUrl,
            Organization = "org",
            Database = "database",
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

        _client = new InfluxDBClient(MockServerUrl, null, "org", "database");

        await _client.WriteRecordAsync("mem,tag=a field=1");
        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("mem,tag=a field=1"));
    }

    [Test]
    public void AlreadyDisposed()
    {
        _client = new InfluxDBClient(MockServerUrl);
        _client.Dispose();
        var ae = Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await _client.WriteRecordAsync("mem,tag=a field=1");
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo("Cannot access a disposed object.\nObject name: 'InfluxDBClient'."));
    }

    [Test]
    public async Task OrgCustom()
    {
        _client = new InfluxDBClient(MockServerUrl, organization: "org", database: "database");
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1", organization: "my-org");

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query?["org"].First(), Is.EqualTo("my-org"));
    }

    [Test]
    public async Task NotSpecifiedOrg()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        _client = new InfluxDBClient(MockServerUrl, database: "database");
        await _client.WriteRecordAsync("mem,tag=a field=1");

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query, Does.Not.ContainKey("org"));
    }

    [Test]
    public async Task DatabaseCustom()
    {
        _client = new InfluxDBClient(MockServerUrl, organization: "org", database: "database");
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1", database: "my-database");

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query?["bucket"].First(), Is.EqualTo("my-database"));
    }

    [Test]
    public void NotSpecifiedDatabase()
    {
        _client = new InfluxDBClient(MockServerUrl, organization: "org");
        var ae = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await _client.WriteRecordAsync("mem,tag=a field=1");
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message,
            Is.EqualTo(
                "Please specify the 'database' as a method parameter or use default configuration at 'InfluxDBClientConfigs.Database'."));
    }

    [Test]
    public async Task PrecisionDefault()
    {
        _client = new InfluxDBClient(MockServerUrl, organization: "org", database: "database");
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
        _client = new InfluxDBClient(new InfluxDBClientConfigs
        {
            HostUrl = MockServerUrl,
            Organization = "org",
            Database = "database",
            WriteOptions = new WriteOptions
            {
                Precision = WritePrecision.Ms
            }
        });
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1", database: "my-database");

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query?["precision"].First(), Is.EqualTo("ms"));
    }

    [Test]
    public async Task PrecisionCustom()
    {
        _client = new InfluxDBClient(MockServerUrl, organization: "org", database: "database");
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1", database: "my-database", precision: WritePrecision.S);

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query?["precision"].First(), Is.EqualTo("s"));
    }

    [Test]
    public async Task PrecisionBody()
    {
        _client = new InfluxDBClient(MockServerUrl, organization: "org", database: "database");
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        var point = PointData.Measurement("h2o")
            .AddTag("location", "europe")
            .AddField("level", 2)
            .SetTimestamp(123_000_000_000L);

        await _client.WritePointAsync(point, precision: WritePrecision.S);

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("h2o,location=europe level=2i 123"));
    }

    [Test]
    public async Task Proxy()
    {
        _client = new InfluxDBClient(new InfluxDBClientConfigs
        {
            HostUrl = MockServerUrl,
            Organization = "org",
            Database = "database",
            Proxy = new System.Net.WebProxy
            {
                Address = new Uri(MockProxyUrl),
                BypassProxyOnLocal = false
            }
        });
        MockProxy
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        var point = PointData.Measurement("h2o")
            .AddTag("location", "europe")
            .AddField("level", 2)
            .SetTimestamp(123_000_000_000L);

        await _client.WritePointAsync(point);

        var requests = MockProxy.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("h2o,location=europe level=2i 123000000000"));
    }

    [Test]
    public async Task CustomHeader()
    {
        _client = new InfluxDBClient(new InfluxDBClientConfigs
        {
            HostUrl = MockServerUrl,
            Organization = "org",
            Database = "database",
            Headers = new List<KeyValuePair<String, String>>
            {
                new KeyValuePair<string, string>("X-device", "ab-01"),
            }
        });
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").WithHeader("X-device", "ab-01").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        var point = PointData.Measurement("h2o")
            .AddTag("location", "europe")
            .AddField("level", 2)
            .SetTimestamp(123_000_000_000L);

        await _client.WritePointAsync(point);

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("h2o,location=europe level=2i 123000000000"));
    }

    private async Task WriteData()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1");
    }
}