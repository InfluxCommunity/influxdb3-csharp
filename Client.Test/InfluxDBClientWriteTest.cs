using System;
using System.Linq;
using System.Threading.Tasks;
using InfluxDB3.Client.Config;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using WireMock.Server;
using WireMock.Settings;

namespace InfluxDB3.Client.Test;

public class InfluxDBClientWriteTest
{
    private WireMockServer _mockServer;
    private string _mockServerUrl;

    private InfluxDBClient _client;

    [SetUp]
    public void SetUp()
    {
        if (_mockServer is { IsStarted: true })
        {
            return;
        }

        _mockServer = WireMockServer.Start(new WireMockServerSettings
        {
            UseSSL = false
        });

        _mockServerUrl = _mockServer.Urls[0];
    }

    [TearDown]
    public void ResetServer()
    {
        _mockServer.Reset();
        _client?.Dispose();
    }

    [OneTimeTearDown]
    public void ShutdownServer()
    {
        _mockServer?.Stop();
    }

    [Test]
    public async Task Authorization()
    {
        _client = new InfluxDBClient(_mockServerUrl, token: "my-token");
        await WriteData();

        var requests = _mockServer.LogEntries.ToList();

        Assert.That(requests[0].RequestMessage.Headers?["Authorization"][0], Is.EqualTo("Token my-token"));
    }

    [Test]
    public async Task UserAgent()
    {
        _client = new InfluxDBClient(_mockServerUrl);
        await WriteData();

        var requests = _mockServer.LogEntries.ToList();

        Assert.That(requests[0].RequestMessage.Headers?["User-Agent"][0], Does.StartWith("influxdb3-csharp/1."));
        Assert.That(requests[0].RequestMessage.Headers?["User-Agent"][0], Does.EndWith(".0.0"));
    }

    [Test]
    public async Task Url()
    {
        _client = new InfluxDBClient(_mockServerUrl);
        await WriteData();

        var requests = _mockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Url, Is.EqualTo($"{_mockServerUrl}/api/v2/write"));
    }

    [Test]
    public async Task UrlWithBackslash()
    {
        _client = new InfluxDBClient($"{_mockServerUrl}/");
        await WriteData();

        var requests = _mockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Url, Is.EqualTo($"{_mockServerUrl}/api/v2/write"));
    }

    [Test]
    public async Task Body()
    {
        _client = new InfluxDBClient(_mockServerUrl);
        await WriteData();

        var requests = _mockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("mem,tag=a field=1"));
    }
    [Test]
    public async Task BodyConcat()
    {
        _mockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        _client = new InfluxDBClient(_mockServerUrl);

        await _client.WriteRecordsAsync(new[] { "mem,tag=a field=1", "mem,tag=b field=2" });

        var requests = _mockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("mem,tag=a field=1\nmem,tag=b field=2"));
    }

    [Test]
    public void AllowHttpRedirects()
    {
        _client = new InfluxDBClient(new InfluxDBClientConfigs(_mockServerUrl)
        {
            AllowHttpRedirects = true
        });

        Assert.Fail();
    }

    [Test]
    public void Timeout()
    {
        _client = new InfluxDBClient(new InfluxDBClientConfigs(_mockServerUrl)
        {
            Timeout = TimeSpan.FromSeconds(45)
        });

        Assert.Fail();
    }

    private async Task WriteData()
    {
        _mockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1");
    }
}