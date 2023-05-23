using System;
using System.Diagnostics.CodeAnalysis;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Client.Test.Write
{
    [TestFixture]
    public class PointDataTest
    {
        [Test]
        public void TagEmptyTagValue()
        {
            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddTag("log", "to_delete")
                .AddTag("log", "")
                .AddField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void TagEscapingKeyAndValue()
        {
            var point = PointData.Measurement("h\n2\ro\t_data")
                .AddTag("new\nline", "new\nline")
                .AddTag("carriage\rreturn", "carriage\rreturn")
                .AddTag("t\tab", "t\tab")
                .AddField("level", 2);

            Assert.That(
                point.ToLineProtocol(), Is.EqualTo("h\\n2\\ro\\t_data,carriage\\rreturn=carriage\\rreturn,new\\nline=new\\nline,t\\tab=t\\tab level=2i"));
        }

        [Test]
        public void EqualSignEscaping()
        {
            var point = PointData.Measurement("h=2o")
                .AddTag("l=ocation", "e=urope")
                .AddField("l=evel", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h=2o,l\\=ocation=e\\=urope l\\=evel=2i"));
        }

        [Test]
        [SuppressMessage("Assertion", "NUnit2010:Use EqualConstraint for better assertion messages in case of failure")]
        [SuppressMessage("ReSharper", "SuspiciousTypeConversion.Global")]
        public void Immutability()
        {
            var point = PointData.Measurement("h2 o")
                .AddTag("location", "europe");

            var point1 = point
                .AddTag("TAG", "VALX")
                .AddField("level", 2);

            var point2 = point
                .AddTag("TAG", "VALX")
                .AddField("level", 2);

            var point3 = point
                .AddTag("TAG", "VALY")
                .AddField("level", 2);

            Assert.Multiple(() =>
            {
                Assert.That(point2, Is.EqualTo(point1));
                Assert.That(point1, Is.Not.EqualTo(point));
                Assert.That(ReferenceEquals(point1, point2), Is.False);
                Assert.That(point1 == point3, Is.False);
                Assert.That(point1 != point3, Is.True);
                Assert.That(point1.Equals(null), Is.False);
                Assert.That(point1.Equals(10), Is.False);
                Assert.That(point1.GetHashCode(), Is.Not.EqualTo(point3.GetHashCode()));
                Assert.That(point1, Is.Not.EqualTo(point3));
            });
        }

        [Test]
        public void MeasurementEscape()
        {
            var point = PointData.Measurement("h2 o")
                .AddTag("location", "europe")
                .AddTag("", "warn")
                .AddField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2\\ o,location=europe level=2i"));

            point = PointData.Measurement("h2=o")
                .AddTag("location", "europe")
                .AddTag("", "warn")
                .AddField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2=o,location=europe level=2i"));

            point = PointData.Measurement("h2,o")
                .AddTag("location", "europe")
                .AddTag("", "warn")
                .AddField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2\\,o,location=europe level=2i"));
        }

        [Test]
        public void TagEmptyKey()
        {
            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddTag("", "warn")
                .AddField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void TagEmptyValue()
        {
            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddTag("log", "")
                .AddField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void OverrideTagField()
        {
            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddTag("location", "europe2")
                .AddField("level", 2)
                .AddField("level", 3);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe2 level=3i"));
        }

        [Test]
        public void FieldTypes()
        {
            var point = PointData.Measurement("h2o").AddTag("location", "europe")
                .AddField("long", 1L)
                .AddField("double", 250.69D)
                .AddField("float", 35.0F)
                .AddField("integer", 7)
                .AddField("short", (short)8)
                // ReSharper disable once RedundantCast
                .AddField("byte", (byte)9)
                .AddField("ulong", (ulong)10)
                .AddField("uint", (uint)11)
                .AddField("sbyte", (sbyte)12)
                .AddField("ushort", (ushort)13)
                .AddField("point", 13.3)
                .AddField("decimal", (decimal)25.6)
                .AddField("boolean", false)
                .AddField("string", "string value");

            const string expected = "h2o,location=europe boolean=false,byte=9i,decimal=25.6,double=250.69,float=35,integer=7i,long=1i," +
                                    "point=13.300000000000001,sbyte=12i,short=8i,string=\"string value\",uint=11u,ulong=10u,ushort=13u";

            Assert.That(point.ToLineProtocol(), Is.EqualTo(expected));
        }

        [Test]
        public void DoubleFormat()
        {
            var point = PointData.Measurement("sensor")
                .AddField("double", 250.69D)
                .AddField("double15", 15.333333333333333D)
                .AddField("double16", 16.3333333333333333D)
                .AddField("double17", 17.33333333333333333D)
                .AddField("example", 459.29587181322927);

            const string expected = "sensor double=250.69,double15=15.333333333333332,double16=16.333333333333332," +
                                    "double17=17.333333333333332,example=459.29587181322927";

            Assert.That(point.ToLineProtocol(), Is.EqualTo(expected));
        }

        [Test]
        public void FieldNullValue()
        {
            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .AddField("warning", null!);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void FieldEscape()
        {
            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", "string esc\\ape value");

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=\"string esc\\\\ape value\""));

            point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", "string esc\"ape value");

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=\"string esc\\\"ape value\""));
        }

        [Test]
        public void Time()
        {
            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(123L);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123"));
        }

        [Test]
        public void TimePrecision()
        {
            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(123_000_000_000L);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123000000000"));

            point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(123_000_000L, WritePrecision.Us);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123000000000"));

            point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(123_000L, WritePrecision.Ms);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123000000000"));

            point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(123L, WritePrecision.S);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123000000000"));
        }

        [Test]
        public void LineProtocolTimePrecision()
        {
            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(123_000_000_000L);

            Assert.Multiple(() =>
            {
                Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123000000000"));
                Assert.That(point.ToLineProtocol(WritePrecision.Ns), Is.EqualTo("h2o,location=europe level=2i 123000000000"));
                Assert.That(point.ToLineProtocol(WritePrecision.Us), Is.EqualTo("h2o,location=europe level=2i 123000000"));
                Assert.That(point.ToLineProtocol(WritePrecision.Ms), Is.EqualTo("h2o,location=europe level=2i 123000"));
                Assert.That(point.ToLineProtocol(WritePrecision.S), Is.EqualTo("h2o,location=europe level=2i 123"));
            });
        }

        [Test]
        public void TimeSpanFormatting()
        {
            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(TimeSpan.FromDays(1));

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 86400000000000"));

            point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(TimeSpan.FromHours(356));

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 1281600000000000"));

            point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(TimeSpan.FromSeconds(156));

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 156000000000"));
        }

        [Test]
        public void DateTimeFormatting()
        {
            var dateTime = new DateTime(2015, 10, 15, 8, 20, 15, DateTimeKind.Utc);

            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(dateTime);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 1444897215000000000"));

            dateTime = new DateTime(2015, 10, 15, 8, 20, 15, 750, DateTimeKind.Utc);

            point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", false)
                .SetTimestamp(dateTime);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=false 1444897215750000000"));

            point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", true)
                .SetTimestamp(DateTime.UtcNow);

            var lineProtocol = point.ToLineProtocol();
            Assert.That(lineProtocol, Does.Not.Contain("."));

            point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", true)
                .SetTimestamp(DateTime.UtcNow);

            lineProtocol = point.ToLineProtocol();
            Assert.That(lineProtocol.Contains("."), Is.False);
        }

        [Test]
        public void DateTimeUnspecified()
        {
            var dateTime = new DateTime(2015, 10, 15, 8, 20, 15, DateTimeKind.Unspecified);

            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(dateTime);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 1444897215000000000"));
        }

        [Test]
        public void DateTimeLocal()
        {
            var dateTime = new DateTime(2015, 10, 15, 8, 20, 15, DateTimeKind.Local);

            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(dateTime);

            var lineProtocolLocal = point.ToLineProtocol();

            point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(TimeZoneInfo.ConvertTimeToUtc(dateTime));
            var lineProtocolUtc = point.ToLineProtocol();

            Assert.That(lineProtocolLocal, Is.EqualTo(lineProtocolUtc));
        }

        [Test]
        public void DateTimeOffsetFormatting()
        {
            var offset = DateTimeOffset.FromUnixTimeSeconds(15678);

            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(offset);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 15678000000000"));
        }

        [Test]
        public void HasFields()
        {
            Assert.Multiple(() =>
            {
                Assert.That(PointData.Measurement("h2o").HasFields(), Is.False);
                Assert.That(PointData.Measurement("h2o").AddTag("location", "europe").HasFields(), Is.False);
                Assert.That(PointData.Measurement("h2o").AddField("level", "2").HasFields(), Is.True);
                Assert.That(PointData.Measurement("h2o").AddTag("location", "europe").AddField("level", "2").HasFields(), Is.True);
            });
        }

        [Test]
        public void InfinityValues()
        {
            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("double-infinity-positive", double.PositiveInfinity)
                .AddField("double-infinity-negative", double.NegativeInfinity)
                .AddField("double-nan", double.NaN)
                .AddField("flout-infinity-positive", float.PositiveInfinity)
                .AddField("flout-infinity-negative", float.NegativeInfinity)
                .AddField("flout-nan", float.NaN)
                .AddField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void OnlyInfinityValues()
        {
            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("double-infinity-positive", double.PositiveInfinity)
                .AddField("double-infinity-negative", double.NegativeInfinity)
                .AddField("double-nan", double.NaN)
                .AddField("flout-infinity-positive", float.PositiveInfinity)
                .AddField("flout-infinity-negative", float.NegativeInfinity)
                .AddField("flout-nan", float.NaN);

            Assert.That(point.ToLineProtocol(), Is.EqualTo(""));
        }

        [Test]
        public void UseGenericObjectAsFieldValue()
        {
            var point = PointData.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("custom-object", new GenericObject { Value1 = "test", Value2 = 10 });

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe custom-object=\"test-10\""));
        }
    }

    internal class GenericObject
    {
        internal string Value1 { get; set; }
        internal int Value2 { get; set; }

        public override string ToString()
        {
            return $"{Value1}-{Value2}";
        }
    }
}