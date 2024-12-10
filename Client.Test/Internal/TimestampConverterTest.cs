using System;
using System.Numerics;
using InfluxDB3.Client.Internal;

namespace InfluxDB3.Client.Test.Internal;

public class TimestampConverterTest
{
    private static readonly DateTime EpochStart = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    [Test]
    public void GetNanoTime()
    {
        var now = DateTime.Now;

        var localTime = DateTime.SpecifyKind(now, DateTimeKind.Local);
        var timestamp = TimestampConverter.GetNanoTime(localTime);
        BigInteger nanoTime = now.ToUniversalTime().Subtract(EpochStart).Ticks * 100;
        Assert.That(nanoTime, Is.EqualTo(timestamp));

        var unspecifiedTime = DateTime.SpecifyKind(now, DateTimeKind.Unspecified);
        timestamp = TimestampConverter.GetNanoTime(unspecifiedTime);
        nanoTime = DateTime.SpecifyKind(now, DateTimeKind.Utc).Subtract(EpochStart).Ticks * 100;
        Assert.That(nanoTime, Is.EqualTo(timestamp));
    }
}