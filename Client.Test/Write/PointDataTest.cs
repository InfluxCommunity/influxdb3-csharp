using System;
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
                .Tag("location", "europe")
                .Tag("log", "to_delete")
                .Tag("log", "")
                .Field("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void TagEscapingKeyAndValue()
        {
            var point = PointData.Measurement("h\n2\ro\t_data")
                .Tag("new\nline", "new\nline")
                .Tag("carriage\rreturn", "carriage\rreturn")
                .Tag("t\tab", "t\tab")
                .Field("level", 2);

            Assert.That(
                point.ToLineProtocol(), Is.EqualTo("h\\n2\\ro\\t_data,carriage\\rreturn=carriage\\rreturn,new\\nline=new\\nline,t\\tab=t\\tab level=2i"));
        }

        [Test]
        public void EqualSignEscaping()
        {
            var point = PointData.Measurement("h=2o")
                .Tag("l=ocation", "e=urope")
                .Field("l=evel", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h=2o,l\\=ocation=e\\=urope l\\=evel=2i"));
        }

        [Test]
        public void Immutability()
        {
            var point = PointData.Measurement("h2 o")
                .Tag("location", "europe");

            var point1 = point
                .Tag("TAG", "VALX")
                .Field("level", 2);

            var point2 = point
                .Tag("TAG", "VALX")
                .Field("level", 2);

            var point3 = point
                .Tag("TAG", "VALY")
                .Field("level", 2);

            Assert.Multiple(() =>
            {
                Assert.That(point2, Is.EqualTo(point1));
                Assert.That(point1, Is.Not.EqualTo(point));
                Assert.That(ReferenceEquals(point1, point2), Is.False);
                Assert.That(point1, Is.Not.EqualTo(point3));
            });
        }

        [Test]
        public void MeasurementEscape()
        {
            var point = PointData.Measurement("h2 o")
                .Tag("location", "europe")
                .Tag("", "warn")
                .Field("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2\\ o,location=europe level=2i"));

            point = PointData.Measurement("h2=o")
                .Tag("location", "europe")
                .Tag("", "warn")
                .Field("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2=o,location=europe level=2i"));

            point = PointData.Measurement("h2,o")
                .Tag("location", "europe")
                .Tag("", "warn")
                .Field("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2\\,o,location=europe level=2i"));
        }

        [Test]
        public void TagEmptyKey()
        {
            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Tag("", "warn")
                .Field("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void TagEmptyValue()
        {
            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Tag("log", "")
                .Field("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void OverrideTagField()
        {
            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Tag("location", "europe2")
                .Field("level", 2)
                .Field("level", 3);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe2 level=3i"));
        }

        [Test]
        public void FieldTypes()
        {
            var point = PointData.Measurement("h2o").Tag("location", "europe")
                .Field("long", 1L)
                .Field("double", 250.69D)
                .Field("float", 35.0F)
                .Field("integer", 7)
                .Field("short", (short)8)
                // ReSharper disable once RedundantCast
                .Field("byte", (byte)9)
                .Field("ulong", (ulong)10)
                .Field("uint", (uint)11)
                .Field("sbyte", (sbyte)12)
                .Field("ushort", (ushort)13)
                .Field("point", 13.3)
                .Field("decimal", (decimal)25.6)
                .Field("boolean", false)
                .Field("string", "string value");

            const string expected = "h2o,location=europe boolean=false,byte=9i,decimal=25.6,double=250.69,float=35,integer=7i,long=1i," +
                                    "point=13.300000000000001,sbyte=12i,short=8i,string=\"string value\",uint=11u,ulong=10u,ushort=13u";

            Assert.That(point.ToLineProtocol(), Is.EqualTo(expected));
        }

        [Test]
        public void DoubleFormat()
        {
            var point = PointData.Measurement("sensor")
                .Field("double", 250.69D)
                .Field("double15", 15.333333333333333D)
                .Field("double16", 16.3333333333333333D)
                .Field("double17", 17.33333333333333333D)
                .Field("example", 459.29587181322927);

            const string expected = "sensor double=250.69,double15=15.333333333333332,double16=16.333333333333332," +
                                    "double17=17.333333333333332,example=459.29587181322927";

            Assert.That(point.ToLineProtocol(), Is.EqualTo(expected));
        }

        [Test]
        public void FieldNullValue()
        {
            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", 2)
                .Field("warning", null);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void FieldEscape()
        {
            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", "string esc\\ape value");

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=\"string esc\\\\ape value\""));

            point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", "string esc\"ape value");

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=\"string esc\\\"ape value\""));
        }

        [Test]
        public void Time()
        {
            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", 2)
                .Timestamp(123L);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123"));
        }

        [Test]
        public void TimeSpanFormatting()
        {
            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", 2)
                .Timestamp(TimeSpan.FromDays(1));

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 86400000000000"));

            point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", 2)
                .Timestamp(TimeSpan.FromHours(356));

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 1281600000000000"));

            point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", 2)
                .Timestamp(TimeSpan.FromSeconds(156));

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 156000000000"));
        }

        [Test]
        public void DateTimeFormatting()
        {
            var dateTime = new DateTime(2015, 10, 15, 8, 20, 15, DateTimeKind.Utc);

            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", 2)
                .Timestamp(dateTime);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 1444897215000000000"));

            dateTime = new DateTime(2015, 10, 15, 8, 20, 15, 750, DateTimeKind.Utc);

            point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", false)
                .Timestamp(dateTime);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=false 1444897215750000000"));

            point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", true)
                .Timestamp(DateTime.UtcNow);

            var lineProtocol = point.ToLineProtocol();
            Assert.That(lineProtocol, Does.Not.Contain("."));

            point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", true)
                .Timestamp(DateTime.UtcNow);

            lineProtocol = point.ToLineProtocol();
            Assert.That(lineProtocol.Contains("."), Is.False);
        }

        [Test]
        public void DateTimeUnspecified()
        {
            var dateTime = new DateTime(2015, 10, 15, 8, 20, 15, DateTimeKind.Unspecified);

            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", 2)
                .Timestamp(dateTime);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 1444897215000000000"));
        }

        [Test]
        public void DateTimeLocal()
        {
            var dateTime = new DateTime(2015, 10, 15, 8, 20, 15, DateTimeKind.Local);

            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", 2)
                .Timestamp(dateTime);

            var lineProtocolLocal = point.ToLineProtocol();

            point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", 2)
                .Timestamp(TimeZoneInfo.ConvertTimeToUtc(dateTime));
            var lineProtocolUtc = point.ToLineProtocol();

            Assert.That(lineProtocolLocal, Is.EqualTo(lineProtocolUtc));
        }

        [Test]
        public void DateTimeOffsetFormatting()
        {
            var offset = DateTimeOffset.FromUnixTimeSeconds(15678);

            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("level", 2)
                .Timestamp(offset);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 15678000000000"));
        }

        [Test]
        public void HasFields()
        {
            Assert.Multiple(() =>
            {
                Assert.That(PointData.Measurement("h2o").HasFields(), Is.False);
                Assert.That(PointData.Measurement("h2o").Tag("location", "europe").HasFields(), Is.False);
                Assert.That(PointData.Measurement("h2o").Field("level", "2").HasFields(), Is.True);
                Assert.That(PointData.Measurement("h2o").Tag("location", "europe").Field("level", "2").HasFields(), Is.True);
            });
        }

        [Test]
        public void InfinityValues()
        {
            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("double-infinity-positive", double.PositiveInfinity)
                .Field("double-infinity-negative", double.NegativeInfinity)
                .Field("double-nan", double.NaN)
                .Field("flout-infinity-positive", float.PositiveInfinity)
                .Field("flout-infinity-negative", float.NegativeInfinity)
                .Field("flout-nan", float.NaN)
                .Field("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void OnlyInfinityValues()
        {
            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("double-infinity-positive", double.PositiveInfinity)
                .Field("double-infinity-negative", double.NegativeInfinity)
                .Field("double-nan", double.NaN)
                .Field("flout-infinity-positive", float.PositiveInfinity)
                .Field("flout-infinity-negative", float.NegativeInfinity)
                .Field("flout-nan", float.NaN);

            Assert.That(point.ToLineProtocol(), Is.EqualTo(""));
        }

        [Test]
        public void UseGenericObjectAsFieldValue()
        {
            var point = PointData.Measurement("h2o")
                .Tag("location", "europe")
                .Field("custom-object", new GenericObject { Value1 = "test", Value2 = 10 });

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