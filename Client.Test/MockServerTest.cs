using WireMock.Server;
using WireMock.Settings;

namespace InfluxDB3.Client.Test;

public class MockServerTest
{
    internal WireMockServer MockServer, MockProxy;
    internal string MockServerUrl, MockProxyUrl;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        if (MockServer is { IsStarted: true })
        {
            return;
        }

        MockServer = WireMockServer.Start(new WireMockServerSettings
        {
            UseSSL = false
        });

        MockServerUrl = MockServer.Urls[0];

        MockProxy = WireMockServer.Start(new WireMockServerSettings
        {
            UseSSL = false,
            Port = 8888,
            ProxyAndRecordSettings = new ProxyAndRecordSettings
            {
                Url = MockServerUrl
            }
        });

        MockProxyUrl = MockProxy.Urls[0];
    }

    [OneTimeTearDown]
    public void OneTimeTearDownAttribute()
    {
        MockServer.Dispose();
        MockProxy.Dispose();
    }

    [TearDown]
    public void TearDown()
    {
        MockServer.Reset();
        MockProxy.Reset();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        MockServer?.Stop();
        MockProxy?.Stop();
    }
}