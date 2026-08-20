using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Write;
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

    [TestCase(false, true, true, TestName = "WritePartialBatch_WithV3Api_ReturnsStructuredPartialWriteError")]
    [TestCase(true, false, true, TestName = "WritePartialBatch_WithV2Api_ReturnsGenericApiError")]
    [TestCase(false, true, false, TestName = "WritePartialBatch_WithV3Api_AcceptPartialFalse_ReturnsStructuredPartialWriteError")]
    [TestCase(true, false, false, TestName = "WritePartialBatch_WithV2Api_AcceptPartialFalse_ReturnsGenericApiError")]
    public void WritePartialBatchBehaviorByWriteApi(bool useV2Api, bool expectStructuredPartialError, bool acceptPartial)
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database,
            WriteOptions = new WriteOptions
            {
                UseV2Api = useV2Api,
                AcceptPartial = acceptPartial
            }
        });

        const string validLine = "home,room=Sunroom temp=96 1735545600";
        const string invalidLine = "home,room=Sunroom temp=\"hi\" 1735549200";

        var ae = Assert.CatchAsync<InfluxDBApiException>((Func<Task>)(async () =>
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
                    .Or.Contain("parsing failed for write_lp endpoint")
                    .Or.Contain("line protocol parsing error"));
        }
        else
        {
            Assert.That(ae, Is.Not.InstanceOf<InfluxDBPartialWriteException>());
        }
    }

    [Test]
    public async Task WriteOptionsWithDefaultTagsAsWriteDataArgument()
    {
        var measurement1 = $"sensor{DateTime.Now.Ticks % 10000}01";
        var measurement2 = $"sensor{DateTime.Now.Ticks % 10000}02";
        WriteOptions writeOptions = new WriteOptions()
        {
            DefaultTags = new Dictionary<string, string>()
            {
                { "model", "HAL2001" },
                { "manu", "clarke and kubrick" },
            },
            AcceptPartial = true
        };

        PointData p1 = PointData.Measurement(measurement1)
            .SetTag("location", "hallA")
            .SetDoubleField("fVal", 3.14)
            .SetIntegerField("iVal", 42);

        PointData p2 = PointData.Measurement(measurement2)
            .SetTag("location", "lab09")
            .SetDoubleField("fVal", 6.28)
            .SetIntegerField("iVal", 21);

        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database,
            WriteOptions = new WriteOptions
            {
                UseV2Api = false,
                DefaultTags = new Dictionary<string, string>()
                {
                    { "model", "Generic" },
                    { "licensee", "SinclairZX80" }
                }
            }
        });

        await client.WritePointAsync(p1);
        await client.WritePointAsync(p2, writeOptions: writeOptions);

        var query01 = $"SELECT * FROM {measurement1} ORDER BY time DESC";
        var query02 = $"SELECT * FROM {measurement2} ORDER BY time DESC";

        var result1 = await client.QueryPoints(query01).ToListAsync();

        Assert.That(result1.Count, Is.EqualTo(1));
        Assert.That(result1.First().GetTag("location"), Is.EqualTo("hallA"));
        Assert.That(result1.First().GetTag("licensee"), Is.EqualTo("SinclairZX80"));
        Assert.That(result1.First().GetTag("model"), Is.EqualTo("Generic"));
        Assert.That(result1.First().GetTag("manu"), Is.Null);

        var result2 = await client.QueryPoints(query02).ToListAsync();

        Assert.That(result2.Count, Is.EqualTo(1));
        Assert.That(result2.First().GetTag("location"), Is.EqualTo("lab09"));
        Assert.That(result2.First().GetTag("licensee"), Is.Null);
        Assert.That(result2.First().GetTag("model"), Is.EqualTo("HAL2001"));
        Assert.That(result2.First().GetTag("manu"), Is.EqualTo("clarke and kubrick"));
    }

    [Test]
    public void WriteOptionsAsArgumentInvalidateWrite()
    {
        var measurement = $"sensor{DateTime.Now.Ticks % 10000}";

        WriteOptions writeOptions = new WriteOptions()
        {
            NoSync = true
        };

        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database,
            WriteOptions = new WriteOptions()
            {
                UseV2Api = true,
            }
        });

        var ae = Assert.ThrowsAsync<InvalidOperationException>((Func<Task>)(async () =>
        await client.WriteRecordAsync(record: $"{measurement},location=lab03 fVal=3.14,iVal=42i", writeOptions: writeOptions)));

        Assert.That(ae.Message, Contains.Substring("NoSync requires UseV2Api=false"));

    }
}
