using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;

namespace InfluxDB3.Client.Test;

public class InfluxDBApiExceptionTest : MockServerTest
{

    private InfluxDBClient _client;

    [TearDown]
    public void TearDown()
    {
        base.TearDown();
        _client?.Dispose();
    }

    [Test]
    public void NullValuesTest()
    {
        var exception = new InfluxDBApiException("Testing exception", null);
        Assert.That(exception.GetStatusCode().ToString(), Is.EqualTo("0"));
        var headers = exception.GetHeaders();
        Assert.That(exception.GetHeaders(), Is.Null);
    }

    [Test]
    public async Task GeneratedInfluxDbException()
    {
        var requestId = Guid.NewGuid().ToString();

        MockServer
            .Given(Request.Create().WithPath("/api/v2/write").UsingPost())
            .RespondWith(Response.Create()
                .WithStatusCode(400)
                .WithBody("{ \"message\": \"just testing\", \"statusCode\": \"bad request\" }")
                .WithHeaders(new Dictionary<string, string>()
                {
                    {"Content-Type", "application/json"},
                    {"Trace-Id", "123456789ABCDEF0"},
                    {"Trace-Sampled", "false"},
                    {"X-Influxdb-Request-ID", requestId},
                    {"X-Influxdb-Build", "Cloud"}
                })
            );

        _client = new InfluxDBClient(MockServerUrl,
            "my-token",
            "my-org",
            "my-database");
        try
        {
            await _client.WriteRecordAsync("wetbulb,location=prg val=20.1");
        }
        catch (Exception ex)
        {
            if (ex is InfluxDBApiException)
            {
                var idbae = (InfluxDBApiException)ex;
                Assert.Multiple(() =>
                {
                    Assert.That(idbae.Message, Is.EqualTo("just testing"));
                    Assert.That(idbae.GetStatusCode().ToString(), Is.EqualTo("BadRequest"));
                    Assert.That(idbae.GetHeaders().Count() == 7);
                });
                var headersDix = idbae.GetHeaders().ToFrozenDictionary();
                Assert.Multiple(() =>
                {
                    Assert.That(headersDix["Trace-Id"].First(), Is.EqualTo("123456789ABCDEF0"));
                    Assert.That(headersDix["Trace-Sampled"].First(), Is.EqualTo("false"));
                    Assert.That(headersDix["X-Influxdb-Request-ID"].First(), Is.EqualTo(requestId));
                    Assert.That(headersDix["X-Influxdb-Build"].First(), Is.EqualTo("Cloud"));
                });
            }
            else
            {
                Assert.Fail($"Should have thrown InfluxdbApiException. Not - {ex}");
            }
        }
    }
}