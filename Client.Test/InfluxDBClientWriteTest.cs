using System.Linq;
using System.Threading.Tasks;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Write;
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
        _client = new InfluxDBClient(MockServerUrl, org: "org", database: "database");
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

        _client = new InfluxDBClient(MockServerUrl, org: "org", database: "database");

        await _client.WriteRecordsAsync(new[] { "mem,tag=a field=1", "mem,tag=b field=2" });

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString,
            Is.EqualTo("mem,tag=a field=1\nmem,tag=b field=2"));
    }

    [Test]
    public async Task OrgCustom()
    {
        _client = new InfluxDBClient(MockServerUrl, org: "org", database: "database");
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1", org: "my-org");

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query?["org"].First(), Is.EqualTo("my-org"));
    }

    [Test]
    public async Task DatabaseCustom()
    {
        _client = new InfluxDBClient(MockServerUrl, org: "org", database: "database");
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1", database: "my-database");

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query?["bucket"].First(), Is.EqualTo("my-database"));
    }

    [Test]
    public async Task PrecisionDefault()
    {
        _client = new InfluxDBClient(MockServerUrl, org: "org", database: "database");
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
            Host = MockServerUrl,
            Org = "org",
            Database = "database",
            WritePrecision = WritePrecision.Ms
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
        _client = new InfluxDBClient(MockServerUrl, org: "org", database: "database");
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1", database: "my-database", precision: WritePrecision.S);

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Query?["precision"].First(), Is.EqualTo("s"));
    }

    private async Task WriteData()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1");
    }
}