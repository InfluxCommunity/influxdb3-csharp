using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Grpc.Core;
using InfluxDB3.Client.Config;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;

namespace InfluxDB3.Client.Test;

public class InfluxDBClientHttpsTest : MockHttpsServerTest
{
    private InfluxDBClient _client;

    [TearDown]
    public new void TearDown()
    {
        _client?.Dispose();
    }

    [Test]
    public Task NonExistingCertificateFile()
    {
        var ae = Assert.ThrowsAsync<ArgumentException>(() =>
        {
            _client = new InfluxDBClient(new ClientConfig
            {
                Host = MockHttpsServerUrl,
                Token = "my-token",
                Organization = "my-org",
                Database = "my-database",
                DisableServerCertificateValidation = false,
                SslRootsFilePath = "./not-existing.pem"
            });
            return null;
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Does.Match("Certificate file '.*' not found"));
        return Task.CompletedTask;
    }

    [Test]
    public Task EmptyCertificateFile()
    {
        var ae = Assert.ThrowsAsync<ArgumentException>(() =>
        {
            _client = new InfluxDBClient(new ClientConfig
            {
                Host = MockHttpsServerUrl,
                Token = "my-token",
                Organization = "my-org",
                Database = "my-database",
                DisableServerCertificateValidation = false,
                SslRootsFilePath = "./TestData/OtherCerts/empty.pem"
            });
            return null;
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Does.Match("Certificate file '.*' is empty"));
        return Task.CompletedTask;
    }

    [Test]
    public Task InvalidCertificateFile()
    {
        try
        {
            _client = new InfluxDBClient(new ClientConfig
            {
                Host = MockHttpsServerUrl,
                Token = "my-token",
                Organization = "my-org",
                Database = "my-database",
                DisableServerCertificateValidation = false,
                SslRootsFilePath = "./TestData/OtherCerts/invalid.pem"
            });
        }
        catch (Exception e)
        {
            Assert.That(e, Is.Not.Null);
            Assert.That(e.Message, Does.Match("Failed to import custom certificates"));
        }

        return Task.CompletedTask;
    }

    [Test]
    public async Task WriteWithValidSslRootCertificate()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockHttpsServerUrl,
            Token = "my-token",
            Organization = "my-org",
            Database = "my-database",
            DisableServerCertificateValidation = false,
            SslRootsFilePath = "./TestData/ServerCert/rootCA.pem"
        });

        await WriteData();

        var requests = MockHttpsServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("mem,tag=a field=1"));
    }

    [Test]
    public async Task WriteWithDisabledCertificates()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockHttpsServerUrl,
            Token = "my-token",
            Organization = "my-org",
            Database = "my-database",
            DisableServerCertificateValidation = true,
        });

        await WriteData();

        var requests = MockHttpsServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Is.EqualTo("mem,tag=a field=1"));
    }

    [Test]
    public Task WriteWithOtherSslRootCertificate()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockHttpsServerUrl,
            Token = "my-token",
            Organization = "my-org",
            Database = "my-database",
            DisableServerCertificateValidation = false,
            SslRootsFilePath = "./TestData/OtherCerts/otherCA.pem"
        });

        var ae = Assert.ThrowsAsync<HttpRequestException>(async () =>
        {
            await _client.WriteRecordAsync("mem,tag=a field=1");
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Does.Contain("The SSL connection could not be established"));
        Assert.That(ae.InnerException?.Message,
            Does.Contain("The remote certificate was rejected by the provided RemoteCertificateValidationCallback"));
        return Task.CompletedTask;
    }

    [Test]
    public async Task QueryWithValidSslRootCertificate()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockHttpsServerUrl,
            Token = "my-token",
            Organization = "my-org",
            Database = "my-database",
            DisableServerCertificateValidation = false,
            SslRootsFilePath = "./TestData/ServerCert/rootCA.pem"
        });

        var ae = Assert.ThrowsAsync<RpcException>(async () => { await QueryData(); });

        // Verify: server successfully sent back the configured 404 status 
        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Does.Contain("Bad gRPC response. HTTP status code: 404"));

        // Verify: the request reached the server
        var requests = MockHttpsServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Does.Contain("SELECT 1"));
    }

    [Test]
    public async Task QueryWithDisabledCertificates()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockHttpsServerUrl,
            Token = "my-token",
            Organization = "my-org",
            Database = "my-database",
            DisableServerCertificateValidation = true,
        });

        var ae = Assert.ThrowsAsync<RpcException>(async () => { await QueryData(); });

        // Verify: server successfully sent back the configured 404 status 
        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Does.Contain("Bad gRPC response. HTTP status code: 404"));

        // Verify: the request reached the server
        var requests = MockHttpsServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.BodyData?.BodyAsString, Does.Contain("SELECT 1"));
    }

    [Test]
    public Task QueryWithOtherSslRootCertificate()
    {
        _client = new InfluxDBClient(new ClientConfig
        {
            Host = MockHttpsServerUrl,
            Token = "my-token",
            Organization = "my-org",
            Database = "my-database",
            DisableServerCertificateValidation = false,
            SslRootsFilePath = "./TestData/OtherCerts/otherCA.pem"
        });

        var ae = Assert.ThrowsAsync<RpcException>(async () => { await QueryData(); });

        // Verify: the SSL connection was not established 
        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Does.Contain("The SSL connection could not be established"));

        // Verify: the request did not reach the server
        var requests = MockHttpsServer.LogEntries.ToList();
        Assert.That(requests, Is.Empty);
        return Task.CompletedTask;
    }

    private async Task WriteData()
    {
        MockHttpsServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.WriteRecordAsync("mem,tag=a field=1");
    }


    private async Task QueryData()
    {
        // Setup mock server: return 404 for simplicity, so we don't have to implement a valid response.
        MockHttpsServer
            .Given(Request.Create().WithPath("/arrow.flight.protocol.FlightService/DoGet").UsingPost())
            .RespondWith(Response.Create()
                .WithStatusCode(404)
            );

        // Query data.
        var query = "SELECT 1";
        await _client.Query(query).ToListAsync();
    }
}