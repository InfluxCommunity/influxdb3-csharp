using System;
using System.Net;
using System.Threading.Tasks;
using InfluxDB3.Client.Config;
using NUnit.Framework;
using WriteOptions = InfluxDB3.Client.Config.WriteOptions;

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
            if (ex is InfluxDBApiException iaex)
            {
                Assert.Multiple((Action)(() =>
                {
                    Assert.That(iaex.Message,
                        Does.Contain("Found trailing content")
                            .Or.Contain("partial write of line protocol occurred")
                            .Or.Contain("write buffer error: parsing for line protocol failed")
                    );
                    Assert.That(iaex.StatusCode.ToString(), Is.EqualTo("BadRequest"));
                    Assert.That(iaex.StatusCode, Is.EqualTo(HttpStatusCode.BadRequest));
                }));
            }
            else
            {
                Assert.Fail($"Should catch InfluxDBApiException, but received {ex.GetType()}: {ex.Message}.");
            }
        }
    }

    [TestCase(false, true, TestName = "WritePartialBatch_WithV3Api_ReturnsStructuredPartialWriteError")]
    [TestCase(true, false, TestName = "WritePartialBatch_WithV2Api_ReturnsGenericApiError")]
    public void WritePartialBatchBehaviorByWriteApi(bool useV2Api, bool expectStructuredPartialError)
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database,
            WriteOptions = new WriteOptions
            {
                UseV2Api = useV2Api,
                AcceptPartial = true
            }
        });

        var testId = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var validLine = $"vehicle,id=vwbus vel=1.0,testId={testId}";
        var invalidLine = $"vehicle,id=vwbus vel=,testId={testId}";

        var ae = Assert.ThrowsAsync<InfluxDBApiException>((Func<Task>)(async () =>
        {
            await client.WriteRecordsAsync(new[] { validLine, invalidLine });
        }));

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae!.StatusCode, Is.EqualTo(HttpStatusCode.BadRequest));

        if (expectStructuredPartialError)
        {
            Assert.That(ae, Is.InstanceOf<InfluxDBPartialWriteException>());
            var pwe = (InfluxDBPartialWriteException)ae!;
            Assert.That(pwe.LineErrors, Is.Not.Empty);
            Assert.That(ae.Message,
                Does.Contain("partial write of line protocol occurred")
                    .Or.Contain("parsing failed for write_lp endpoint"));
        }
        else
        {
            Assert.That(ae, Is.Not.InstanceOf<InfluxDBPartialWriteException>());
        }
    }
}
