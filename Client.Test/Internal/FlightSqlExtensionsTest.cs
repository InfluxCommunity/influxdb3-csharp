using System;
using Apache.Arrow;
using Apache.Arrow.Types;
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

    [Test]
    public void Int32Array()
    {
        var array = new Int32Array.Builder().Append(37).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(37));
    }

    [Test]
    public void UInt64Array()
    {
        var array = new UInt64Array.Builder().Append(55).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(55));
    }

    [Test]
    public void Int64Array()
    {
        var array = new Int64Array.Builder().Append(552).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(552));
    }

    [Test]
    public void FloatArray()
    {
        var array = new FloatArray.Builder().Append(552.34F).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(552.34F));
    }

    [Test]
    public void DoubleArray()
    {
        var array = new DoubleArray.Builder().Append(552.322).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(552.322));
    }

    [Test]
    public void StringArray()
    {
        var array = new StringArray.Builder().Append("abc").Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo("abc"));
    }

    [Test]
    public void BinaryArray()
    {
        var array = new BinaryArray.Builder().Append(1).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(new object[] { 1 }));
    }

    [Test]
    public void TimestampArray()
    {
        var dateTimeOffset = DateTimeOffset.Now;
        var array = new TimestampArray.Builder(TimeUnit.Nanosecond, TimeZoneInfo.Utc).Append(dateTimeOffset).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(dateTimeOffset));
    }

    [Test]
    public void Date64Array()
    {
        var array = new Date64Array.Builder().Append(DateTime.Now.Date).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(DateTime.Now.Date));
    }

    [Test]
    public void Date32Array()
    {
        var array = new Date32Array.Builder().Append(DateTime.Now.Date).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(DateTime.Now.Date));
    }

    [Test]
    public void Time32Array()
    {
        var array = new Time32Array.Builder().Append(431).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(431));
    }

    [Test]
    public void Time64Array()
    {
        var array = new Time64Array.Builder().Append(1431).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(1431));
    }

    [Test]
    public void Decimal128Array()
    {
        var array = new Decimal128Array.Builder(new Decimal128Type(8, 2)).Append(1431).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(1431));
    }

    [Test]
    public void Decimal256Array()
    {
        var array = new Decimal256Array.Builder(new Decimal256Type(8, 2)).Append(1551).Build();

        Assert.That(array.Length, Is.EqualTo(1));
        Assert.That(array.GetObjectValue(0), Is.EqualTo(1551));
    }
}