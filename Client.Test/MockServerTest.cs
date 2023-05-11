using WireMock.Server;
using WireMock.Settings;

namespace InfluxDB3.Client.Test;

public class MockServerTest
{
    internal WireMockServer MockServer;
    internal string MockServerUrl;

    [SetUp]
    public void SetUp()
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
    }

    [TearDown]
    public void TearDown()
    {
        MockServer.Reset();
    }

    [OneTimeTearDown]
    public void ShutdownServer()
    {
        MockServer?.Stop();
    }
}