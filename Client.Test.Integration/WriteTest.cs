using System;
using System.Net;
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
                        Does.Contain("Found trailing content")
                            .Or.Contain("partial write of line protocol occurred")
                            .Or.Contain("write buffer error: parsing for line protocol failed")
                    );
                    Assert.That(iaex.StatusCode.ToString(), Is.EqualTo("BadRequest"));
                    Assert.That(iaex.StatusCode, Is.EqualTo(HttpStatusCode.BadRequest));
                });
            }
            else
            {
                Assert.Fail($"Should catch InfluxDBApiException, but received {ex.GetType()}: {ex.Message}.");
            }
        }
    }
}