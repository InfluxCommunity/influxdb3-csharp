using System.Linq;
using System.Threading.Tasks;
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
        _client = new InfluxDBClient(MockServerUrl);
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

        _client = new InfluxDBClient(MockServerUrl);

        await _client.WriteRecordsAsync(new[] { "mem,tag=a field=1", "mem,tag=b field=2" });

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString,
            Is.EqualTo("mem,tag=a field=1\nmem,tag=b field=2"));
    }

    private async Task WriteData()
    {
        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1");
    }
}