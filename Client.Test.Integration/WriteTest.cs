using System;
using System.Collections.Frozen;
using System.Linq;
using System.Threading.Tasks;
using InfluxDB3.Client.Config;
using NUnit.Framework;

namespace InfluxDB3.Client.Test.Integration;

public class WriteTest : IntegrationTest
{

    [Test]
    public async Task WriteWithError()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database,
        });

        try
        {
            await client.WriteRecordAsync("vehicle,id=vwbus vel=0.0,distance=,status=\"STOPPED\"");
        }
        catch (Exception ex)
        {
            if (ex is InfluxDBApiException)
            {
                var iaex = (InfluxDBApiException)ex;
                Assert.Multiple(() =>
                {
                    Assert.That(iaex.Message,
                        Is.EqualTo(
                            "errors encountered on line(s): line 1: " +
                            "Could not parse entire line. Found trailing content: 'distance=,status=\"STOPPED\"'"
                        ));
                    Assert.That(iaex.StatusCode.ToString(), Is.EqualTo("BadRequest"));
                    Assert.That(iaex.StatusCode, Is.EqualTo(System.Net.HttpStatusCode.BadRequest));
                });
                var headersDix = iaex.Headers.ToFrozenDictionary();
                Assert.DoesNotThrow(() =>
                {
                    Assert.Multiple(() =>
                    {
                        Assert.That(headersDix["trace-id"].First(), Is.Not.Empty);
                        Assert.That(headersDix["trace-sampled"].First(), Is.EqualTo("false"));
                        Assert.That(headersDix["Strict-Transport-Security"].First(), Is.Not.Empty);
                        Assert.That(headersDix["X-Influxdb-Request-ID"].First(), Is.Not.Empty);
                        Assert.That(headersDix["X-Influxdb-Build"].First(), Is.EqualTo("Cloud"));
                    });
                });
            }
            else
            {
                Assert.Fail($"Should catch InfluxDBApiException, but received {ex.GetType()}: {ex.Message}.");
            }
        }
    }
}