using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using WireMock.Server;
using WireMock.Settings;

namespace InfluxDB3.Client.Test;

public class ApiTest
{
    private WireMockServer _mockHttpsServer;
    private string _mockHttpsServerUrl;
    private InfluxDBClient _influxDbClient;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        if (_mockHttpsServer is { IsStarted: true })
        {
            return;
        }

        _mockHttpsServer = WireMockServer.Start(new WireMockServerSettings());
        _mockHttpsServerUrl = _mockHttpsServer.Urls[0];

        _influxDbClient = new InfluxDBClient(
            _mockHttpsServerUrl,
            "my-token",
            "my-org",
            "my-bucket"
        );
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        _mockHttpsServer?.Stop();
        _mockHttpsServer?.Dispose();
        _influxDbClient?.Dispose();
    }

    [Test]
    public void TestGetVersionInHeaderSuccess()
    {
        _mockHttpsServer
            .Given(Request.Create().WithPath("/ping").UsingGet())
            .RespondWith(Response.Create()
                .WithHeader("x-influxdb-version", "1.8.0")
                .WithStatusCode(200)
            );
        var result = _influxDbClient.GetServerVersion().GetAwaiter().GetResult();
        Assert.That(result, Is.EqualTo("1.8.0"));
    }

    [Test]
    public void TestGetVersionInBodySuccess()
    {
        _mockHttpsServer
            .Given(Request.Create().WithPath("/ping").UsingGet())
            .RespondWith(Response.Create()
                .WithBody("{\"version\": \"3.0\"}")
                .WithStatusCode(200)
            );
        var result = _influxDbClient.GetServerVersion().GetAwaiter().GetResult();
        Assert.That(result, Is.EqualTo("3.0"));
    }

    [Test]
    public void TestGetVersionEmpty()
    {
        _mockHttpsServer
            .Given(Request.Create().WithPath("/ping").UsingGet())
            .RespondWith(Response.Create()
                .WithHeader("x-influxdb-version-wrong", "2.0")
                .WithBody("{\"field\": \"3.0\"}")
                .WithStatusCode(200)
            );
        var result = _influxDbClient.GetServerVersion().GetAwaiter().GetResult();
        Assert.That(result, Is.Null);
    }

    [Test]
    public void TestGetVersionFail()
    {
        _mockHttpsServer
            .Given(Request.Create().WithPath("/ping").UsingGet())
            .RespondWith(Response.Create()
                .WithHeader("x-influxdb-version-wrong", "2.0")
                .WithBody("{\"field\": \"3.0\"}")
                .WithStatusCode(400)
            );
        Assert.ThrowsAsync<InfluxDBApiException>(async () => { await _influxDbClient.GetServerVersion(); });
    }
}