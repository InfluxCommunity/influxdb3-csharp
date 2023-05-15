using Apache.Arrow;
using InfluxDB3.Client.Internal;

namespace InfluxDB3.Client.Test.Internal;

[TestFixture]
public class FlightSqlExtensionsTest
{
    [Test]
    public void BooleanArray()
    {
        var array = new BooleanArray.Builder().Append(true).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.True);
    }

    [Test]
    public void UInt8ArrayArray()
    {
        var array = new UInt8Array.Builder().Append(5).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(5));
    }

    [Test]
    public void Int8ArrayArray()
    {
        var array = new Int8Array.Builder().Append(13).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(13));
    }

    [Test]
    public void UInt16ArrayArray()
    {
        var array = new UInt16Array.Builder().Append(23).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(23));
    }

    [Test]
    public void Int16ArrayArray()
    {
        var array = new Int16Array.Builder().Append(25).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(25));
    }

    [Test]
    public void UInt32Array()
    {
        var array = new UInt32Array.Builder().Append(35).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(35));
    }

}