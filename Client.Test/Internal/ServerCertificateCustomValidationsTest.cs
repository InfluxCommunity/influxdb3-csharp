using System;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using InfluxDB3.Client.Internal;
using Moq;

namespace InfluxDB3.Client.Test.Internal;

public class ServerCertificateCustomValidationsTest
{
    private HttpRequestMessage _message;
    private X509Certificate2 _certificate;
    private X509Chain _chain;
    private Func<HttpRequestMessage, X509Certificate2, X509Chain, SslPolicyErrors, bool> _callback;


    [SetUp]
    public void SetUp()
    {
        _message = new Mock<HttpRequestMessage>().Object;
        _certificate = new X509Certificate2("./TestData/ServerCert/server.pem");
        _chain = new X509Chain();
        _chain.Build(_certificate);

        _callback = ServerCertificateCustomValidations.CreateCustomCertificatesValidationCallback(
            "./TestData/ServerCert/rootCA.pem", true);
    }

    [TearDown]
    public void Cleanup()
    {
        _certificate.Dispose();
        _chain.Dispose();
    }

    [Test]
    public void NoErrors()
    {
        var isValid = _callback(_message, _certificate, _chain, SslPolicyErrors.None);
        Assert.That(isValid, Is.True);
    }

    [Test]
    public void Error_RemoteCertificateNotAvailable()
    {
        var isValid = _callback(_message, _certificate, _chain, SslPolicyErrors.RemoteCertificateNotAvailable);
        Assert.That(isValid, Is.False);
    }

    [Test]
    public void Error_RemoteCertificateNameMismatch()
    {
        var isValid = _callback(_message, _certificate, _chain, SslPolicyErrors.RemoteCertificateNameMismatch);
        Assert.That(isValid, Is.False);
    }

    [Test]
    public void MissingCertificate()
    {
        var isValid = _callback(_message, null, _chain, SslPolicyErrors.RemoteCertificateChainErrors);
        Assert.That(isValid, Is.False);
    }

    [Test]
    public void MissingChain()
    {
        var isValid = _callback(_message, _certificate, null, SslPolicyErrors.RemoteCertificateChainErrors);
        Assert.That(isValid, Is.False);
    }
}