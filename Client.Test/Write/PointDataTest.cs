using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
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
                .SetTag("location", "europe")
                .SetTag("log", "to_delete")
                .SetTag("log", "")
                .SetField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void TagEscapingKeyAndValue()
        {
            var point = PointData.Measurement("h\n2\ro\t_data")
                .SetTag("new\nline", "new\nline")
                .SetTag("carriage\rreturn", "carriage\rreturn")
                .SetTag("t\tab", "t\tab")
                .SetField("level", 2);

            Assert.That(
                point.ToLineProtocol(),
                Is.EqualTo(
                    "h\\n2\\ro\\t_data,carriage\\rreturn=carriage\\rreturn,new\\nline=new\\nline,t\\tab=t\\tab level=2i"));
        }

        [Test]
        public void EqualSignEscaping()
        {
            var point = PointData.Measurement("h=2o")
                .SetTag("l=ocation", "e=urope")
                .SetField("l=evel", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h=2o,l\\=ocation=e\\=urope l\\=evel=2i"));
        }

        [Test]
        public void MeasurementEscape()
        {
            var point = PointData.Measurement("h2 o")
                .SetTag("location", "europe")
                .SetTag("", "warn")
                .SetField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2\\ o,location=europe level=2i"));

            point = PointData.Measurement("h2=o")
                .SetTag("location", "europe")
                .SetTag("", "warn")
                .SetField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2=o,location=europe level=2i"));

            point = PointData.Measurement("h2,o")
                .SetTag("location", "europe")
                .SetTag("", "warn")
                .SetField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2\\,o,location=europe level=2i"));
        }

        [Test]
        public void TagEmptyKey()
        {
            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetTag("", "warn")
                .SetField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void TagEmptyValue()
        {
            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetTag("log", "")
                .SetField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void OverrideTagField()
        {
            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetTag("location", "europe2")
                .SetField("level", 2)
                .SetField("level", 3);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe2 level=3i"));
        }

        [Test]
        [SuppressMessage("ReSharper", "RedundantCast")]
        public void FieldTypes()
        {
            var point = PointData.Measurement("h2o").SetTag("location", "europe")
                .SetField("long", 1L)
                .SetField("double", 250.69D)
                .SetField("float", 35.0F)
                .SetField("integer", 7)
                .SetField("short", (short)8)
                .SetField("byte", (byte)9)
                .SetField("ulong", (ulong)10)
                .SetUintegerField("uint", (uint)11)
                .SetField("sbyte", (sbyte)12)
                .SetUintegerField("ushort", (ushort)13)
                .SetField("point", 13.3)
                .SetField("decimal", (decimal)25.6)
                .SetField("boolean", false)
                .SetField("string", "string value");

            const string expected =
                "h2o,location=europe boolean=false,byte=9i,decimal=25.6,double=250.69,float=35,integer=7i,long=1i," +
                "point=13.300000000000001,sbyte=12i,short=8i,string=\"string value\",uint=11u,ulong=10u,ushort=13u";

            Assert.That(point.ToLineProtocol(), Is.EqualTo(expected));
        }

        [Test]
        public void DoubleFormat()
        {
            var point = PointData.Measurement("sensor")
                .SetField("double", 250.69D)
                .SetField("double15", 15.333333333333333D)
                .SetField("double16", 16.3333333333333333D)
                .SetField("double17", 17.33333333333333333D)
                .SetField("example", 459.29587181322927);

            const string expected = "sensor double=250.69,double15=15.333333333333332,double16=16.333333333333332," +
                                    "double17=17.333333333333332,example=459.29587181322927";

            Assert.That(point.ToLineProtocol(), Is.EqualTo(expected));
        }

        [Test]
        public void FieldNullValue()
        {
            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetField("warning", null!);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void FieldEscape()
        {
            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", "string esc\\ape value");

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=\"string esc\\\\ape value\""));

            point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", "string esc\"ape value");

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=\"string esc\\\"ape value\""));
        }

        [Test]
        public void Time()
        {
            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(123L);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123"));
        }

        [Test]
        public void TimePrecision()
        {
            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(123_000_000_000L);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123000000000"));

            point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(123_000_000L, WritePrecision.Us);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123000000000"));

            point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(123_000L, WritePrecision.Ms);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123000000000"));

            point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(123L, WritePrecision.S);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123000000000"));
        }

        [Test]
        public void LineProtocolTimePrecision()
        {
            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(123_000_000_000L);

            Assert.Multiple(() =>
            {
                Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123000000000"));
                Assert.That(point.ToLineProtocol(WritePrecision.Ns),
                    Is.EqualTo("h2o,location=europe level=2i 123000000000"));
                Assert.That(point.ToLineProtocol(WritePrecision.Us),
                    Is.EqualTo("h2o,location=europe level=2i 123000000"));
                Assert.That(point.ToLineProtocol(WritePrecision.Ms), Is.EqualTo("h2o,location=europe level=2i 123000"));
                Assert.That(point.ToLineProtocol(WritePrecision.S), Is.EqualTo("h2o,location=europe level=2i 123"));
            });
        }

        [Test]
        public void TimeSpanFormatting()
        {
            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(TimeSpan.FromDays(1));

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 86400000000000"));

            point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(TimeSpan.FromHours(356));

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 1281600000000000"));

            point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(TimeSpan.FromSeconds(156));

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 156000000000"));
        }

        [Test]
        public void DateTimeFormatting()
        {
            var dateTime = new DateTime(2015, 10, 15, 8, 20, 15, DateTimeKind.Utc);

            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(dateTime);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 1444897215000000000"));

            dateTime = new DateTime(2015, 10, 15, 8, 20, 15, 750, DateTimeKind.Utc);

            point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", false)
                .SetTimestamp(dateTime);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=false 1444897215750000000"));

            point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", true)
                .SetTimestamp(DateTime.UtcNow);

            var lineProtocol = point.ToLineProtocol();
            Assert.That(lineProtocol, Does.Not.Contain("."));

            point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", true)
                .SetTimestamp(DateTime.UtcNow);

            lineProtocol = point.ToLineProtocol();
            Assert.That(lineProtocol.Contains("."), Is.False);
        }

        [Test]
        public void DateTimeUnspecified()
        {
            var dateTime = new DateTime(2015, 10, 15, 8, 20, 15, DateTimeKind.Unspecified);

            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(dateTime);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 1444897215000000000"));
        }

        [Test]
        public void DateTimeLocal()
        {
            var dateTime = new DateTime(2015, 10, 15, 8, 20, 15, DateTimeKind.Local);

            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(dateTime);

            var lineProtocolLocal = point.ToLineProtocol();

            point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(TimeZoneInfo.ConvertTimeToUtc(dateTime));
            var lineProtocolUtc = point.ToLineProtocol();

            Assert.That(lineProtocolLocal, Is.EqualTo(lineProtocolUtc));
        }

        [Test]
        public void DateTimeOffsetFormatting()
        {
            var offset = DateTimeOffset.FromUnixTimeSeconds(15678);

            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("level", 2)
                .SetTimestamp(offset);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 15678000000000"));
        }

        [Test]
        public void HasFields()
        {
            Assert.Multiple(() =>
            {
                Assert.That(PointData.Measurement("h2o").HasFields(), Is.False);
                Assert.That(PointData.Measurement("h2o").SetTag("location", "europe").HasFields(), Is.False);
                Assert.That(PointData.Measurement("h2o").SetField("level", "2").HasFields(), Is.True);
                Assert.That(
                    PointData.Measurement("h2o").SetTag("location", "europe").SetField("level", "2").HasFields(),
                    Is.True);
            });
        }

        [Test]
        public void GetFieldNames()
        {
            Assert.Multiple(() =>
            {
                Assert.That(PointData.Measurement("h2o").GetFieldNames(), Is.EqualTo(Array.Empty<string>()));
                Assert.That(PointData.Measurement("h2o").SetField("level", "2").GetFieldNames(),
                    Is.EqualTo(new[] { "level" }));
            });
        }

        [Test]
        public void RemoveField()
        {
            Assert.Multiple(() =>
            {
                var pointData = PointData.Measurement("h2o").RemoveField("level");
                Assert.That(pointData.GetFieldNames(), Is.EqualTo(Array.Empty<string>()));

                pointData = pointData.SetField("level", "2");
                Assert.That(pointData.GetFieldNames(), Is.EqualTo(new[] { "level" }));

                pointData = pointData.RemoveField("level");
                Assert.That(pointData.GetFieldNames(), Is.EqualTo(Array.Empty<string>()));
            });
        }

        [Test]
        public void SetFields()
        {
            Assert.That(PointData.Measurement("h2o").SetFields(new Dictionary<string, object>()
            {
                { "level", "2" }, { "size", 15 }
            }).GetFieldNames(), Is.EqualTo(new[] { "level", "size" }));
        }

        [Test]
        public void GetTagNames()
        {
            Assert.Multiple(() =>
            {
                Assert.That(PointData.Measurement("h2o").GetTagNames(), Is.EqualTo(Array.Empty<string>()));
                Assert.That(PointData.Measurement("h2o").SetTag("location", "europe").GetTagNames(),
                    Is.EqualTo(new[] { "location" }));
            });
        }

        [Test]
        public void RemoveTag()
        {
            Assert.Multiple(() =>
            {
                var pointData = PointData.Measurement("h2o").RemoveTag("location");
                Assert.That(pointData.GetTagNames(), Is.EqualTo(Array.Empty<string>()));

                pointData = pointData.SetTag("location", "europe");
                Assert.That(pointData.GetTagNames(), Is.EqualTo(new[] { "location" }));

                pointData = pointData.RemoveTag("location");
                Assert.That(pointData.GetTagNames(), Is.EqualTo(Array.Empty<string>()));
            });
        }

        [Test]
        public void InfinityValues()
        {
            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("double-infinity-positive", double.PositiveInfinity)
                .SetField("double-infinity-negative", double.NegativeInfinity)
                .SetField("double-nan", double.NaN)
                .SetField("flout-infinity-positive", float.PositiveInfinity)
                .SetField("flout-infinity-negative", float.NegativeInfinity)
                .SetField("flout-nan", float.NaN)
                .SetField("level", 2);

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void OnlyInfinityValues()
        {
            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("double-infinity-positive", double.PositiveInfinity)
                .SetField("double-infinity-negative", double.NegativeInfinity)
                .SetField("double-nan", double.NaN)
                .SetField("flout-infinity-positive", float.PositiveInfinity)
                .SetField("flout-infinity-negative", float.NegativeInfinity)
                .SetField("flout-nan", float.NaN);

            Assert.That(point.ToLineProtocol(), Is.EqualTo(""));
        }

        [Test]
        public void UseGenericObjectAsFieldValue()
        {
            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("custom-object", new GenericObject { Value1 = "test", Value2 = 10 });

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe custom-object=\"test-10\""));
        }

        [Test]
        public void GetMeasurementTagFieldTimestamp()
        {
            var point = PointData.Measurement("h2o")
                .SetTag("location", "europe")
                .SetField("a", 1)
                .SetTimestamp(123L);

            Assert.That(point.GetMeasurement(), Is.EqualTo("h2o"));
            Assert.That(point.GetTag("location"), Is.EqualTo("europe"));
            Assert.That(point.GetField("a"), Is.EqualTo(1));
            Assert.That(point.GetTimestamp(), Is.EqualTo(new BigInteger(123)));
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