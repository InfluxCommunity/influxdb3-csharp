using System;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Internal;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;

namespace InfluxDB3.Client.Test.Internal;

public class RestClientTest : MockServerTest
{
    private RestClient _client;

    [TearDown]
    public new void TearDown()
    {
        _client?.Dispose();
    }

    [Test]
    public async Task Authorization()
    {
        _client = new RestClient(new InfluxDBClientConfigs(MockServerUrl)
        {
            Token = "my-token"
        });
        await DoRequest();

        var requests = MockServer.LogEntries.ToList();

        Assert.That(requests[0].RequestMessage.Headers?["Authorization"][0], Is.EqualTo("Token my-token"));
    }

    [Test]
    public async Task UserAgent()
    {
        _client = new RestClient(new InfluxDBClientConfigs(MockServerUrl));
        await DoRequest();

        var requests = MockServer.LogEntries.ToList();

        Assert.That(requests[0].RequestMessage.Headers?["User-Agent"][0], Does.StartWith("influxdb3-csharp/1."));
        Assert.That(requests[0].RequestMessage.Headers?["User-Agent"][0], Does.EndWith(".0.0"));
    }

    [Test]
    public async Task Url()
    {
        _client = new RestClient(new InfluxDBClientConfigs(MockServerUrl));
        await DoRequest();

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Url, Is.EqualTo($"{MockServerUrl}/api"));
    }

    [Test]
    public async Task UrlWithBackslash()
    {
        _client = new RestClient(new InfluxDBClientConfigs($"{MockServerUrl}/"));
        await DoRequest();

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Url, Is.EqualTo($"{MockServerUrl}/api"));
    }

    private async Task DoRequest()
    {
        MockServer
            .Given(Request.Create().WithPath("/api").UsingGet())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.Request("api", HttpMethod.Get);
    }

    [Test]
    public void ErrorHeader()
    {
        _client = new RestClient(new InfluxDBClientConfigs(MockServerUrl));

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("X-Influx-Error", "line protocol poorly formed and no points were written")
                .WithStatusCode(400));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.HttpResponseMessage, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo("Cannot write data to InfluxDB due: line protocol poorly formed and no points were written"));
        });
    }

    [Test]
    public void ErrorBody()
    {
        _client = new RestClient(new InfluxDBClientConfigs(MockServerUrl));

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithBody("no token was sent and they are required")
                .WithStatusCode(403));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.HttpResponseMessage, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo("Cannot write data to InfluxDB due: no token was sent and they are required"));
        });
    }

    [Test]
    public void ErrorJsonBody()
    {
        _client = new RestClient(new InfluxDBClientConfigs(MockServerUrl));

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("X-Influx-Error", "not used")
                .WithBody("{\"error\":\"token does not have sufficient permissions\"}")
                .WithStatusCode(401));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.HttpResponseMessage, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo("Cannot write data to InfluxDB due: token does not have sufficient permissions"));
        });
    }

    [Test]
    public void ErrorReason()
    {
        _client = new RestClient(new InfluxDBClientConfigs(MockServerUrl));

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithStatusCode(409));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.HttpResponseMessage, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo("Cannot write data to InfluxDB due: Conflict"));
        });
    }

    [Test]
    public void AllowHttpRedirects()
    {
        _client = new RestClient(new InfluxDBClientConfigs(MockServerUrl)
        {
            AllowHttpRedirects = true
        });

        Assert.That(_client, Is.Not.Null);
    }

    [Test]
    public void Timeout()
    {
        _client = new RestClient(new InfluxDBClientConfigs(MockServerUrl)
        {
            Timeout = TimeSpan.FromSeconds(45)
        });

        var httpClient = GetDeclaredField<HttpClient>(_client.GetType(), _client, "_httpClient");
        Assert.That(httpClient.Timeout, Is.EqualTo(TimeSpan.FromSeconds(45)));
    }

    private static T GetDeclaredField<T>(IReflect type, object instance, string fieldName)
    {
        const BindingFlags bindFlags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic
                                       | BindingFlags.Static | BindingFlags.DeclaredOnly;
        var field = type.GetField(fieldName, bindFlags);
        return (T)field?.GetValue(instance);
    }
}