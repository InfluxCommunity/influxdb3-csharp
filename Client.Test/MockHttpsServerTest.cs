using WireMock.Server;
using WireMock.Settings;
using WireMock.Types;

namespace InfluxDB3.Client.Test;

public class MockHttpsServerTest
{
    internal WireMockServer MockHttpsServer;
    internal string MockHttpsServerUrl;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        if (MockHttpsServer is { IsStarted: true })
        {
            return;
        }

        MockHttpsServer = WireMockServer.Start(new WireMockServerSettings
        {
            UseSSL = true,
            HostingScheme = HostingScheme.Https,
            CertificateSettings = new WireMockCertificateSettings
            {
                X509CertificateFilePath = "./TestData/ServerCert/server.p12",
                X509CertificatePassword = "password12"
            }
        });

        MockHttpsServerUrl = MockHttpsServer.Urls[0];
    }

    [OneTimeTearDown]
    public void OneTimeTearDownAttribute()
    {
        MockHttpsServer.Dispose();
    }

    [TearDown]
    public void TearDown()
    {
        MockHttpsServer.Reset();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        MockHttpsServer?.Stop();
    }
}