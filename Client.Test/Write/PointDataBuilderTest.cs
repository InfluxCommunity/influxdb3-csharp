using System;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Client.Test.Write
{
    [TestFixture]
    public class PointDataBuilderTest
    {
        [Test]
        public void BuilderValuesToPoint()
        {
            var builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe")
                .AddTag("log", "some_log")
                .AddField("level", 2);

            var point = builder.ToPointData();
            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe,log=some_log level=2i"));
        }

        [Test]
        public void TagEmptyRemovesTagValue()
        {
            var builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe")
                .AddTag("log", "to_delete")
                .AddTag("log", "")
                .AddField("level", 2);

            var point = builder.ToPointData();
            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void ReplaceTagValue()
        {
            var builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe")
                .AddTag("log", "old_log")
                .AddTag("log", "new_log")
                .AddField("level", 2);

            var point = builder.ToPointData();
            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe,log=new_log level=2i"));
        }

        [Test]
        public void ReplaceTagValueInNewPoint()
        {
            var builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe")
                .AddTag("log", "old_log")
                .AddField("level", 2);

            var point = builder.ToPointData();

            builder.AddTag("log", "new_log");

            var anotherPoint = builder.ToPointData();
            Assert.Multiple(() =>
            {
                Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe,log=old_log level=2i"));
                Assert.That(anotherPoint.ToLineProtocol(), Is.EqualTo("h2o,location=europe,log=new_log level=2i"));
            });
        }

        [Test]
        public void TagEmptyKey()
        {
            var builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe")
                .AddTag("", "warn")
                .AddField("level", 2);

            var point = builder.ToPointData();

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void TagEmptyValue()
        {
            var builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe")
                .AddTag("log", "")
                .AddField("level", 2);

            var point = builder.ToPointData();

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void ReplaceFieldValue()
        {
            var builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe2")
                .AddField("level", 2)
                .AddField("level", 3);

            var point = builder.ToPointData();

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe2 level=3i"));
        }

        [Test]
        public void MultipleFields()
        {
            var builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe2")
                .AddField("levelA", 2)
                .AddField("levelB", 3);

            var point = builder.ToPointData();

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe2 levelA=2i,levelB=3i"));
        }

        [Test]
        public void FieldNullValue()
        {
            var builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .AddField("warning", null!);

            var point = builder.ToPointData();

            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i"));
        }

        [Test]
        public void Time()
        {
            var builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(123L);

            var point = builder.ToPointData();
            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 123"));

            var dateTime = new DateTime(2015, 10, 15, 8, 20, 15, DateTimeKind.Utc);
            builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(dateTime);

            point = builder.ToPointData();
            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 1444897215000000000"));

            builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(TimeSpan.FromDays(1));

            point = builder.ToPointData();
            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 86400000000000"));

            var offset = DateTimeOffset.FromUnixTimeSeconds(15678);
            builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2)
                .SetTimestamp(offset);

            point = builder.ToPointData();
            Assert.That(point.ToLineProtocol(), Is.EqualTo("h2o,location=europe level=2i 15678000000000"));
        }

        [Test]
        public void DateTimeMustBeUtc()
        {
            var dateTime = new DateTime(2015, 10, 15, 8, 20, 15);

            var builder = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("level", 2);

            Assert.Throws<ArgumentException>(() => builder.SetTimestamp(dateTime));
        }

        [Test]
        public void HasFields()
        {
            Assert.Multiple(() =>
            {
                Assert.That(PointData.Builder.Measurement("h2o").HasFields(), Is.False);
                Assert.That(PointData.Builder.Measurement("h2o").AddTag("location", "europe").HasFields(), Is.False);
                Assert.That(PointData.Builder.Measurement("h2o").AddField("level", "2").HasFields(), Is.True);
                Assert.That(
                    PointData.Builder.Measurement("h2o").AddTag("location", "europe").AddField("level", "2").HasFields(),
                    Is.True);
            });
        }

        [Test]
        public void FieldTypes()
        {
            var point = PointData.Builder.Measurement("h2o").AddTag("location", "europe")
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

            const string expected =
                "h2o,location=europe boolean=false,byte=9i,decimal=25.6,double=250.69,float=35,integer=7i,long=1i," +
                "point=13.300000000000001,sbyte=12i,short=8i,string=\"string value\",uint=11u,ulong=10u,ushort=13u";

            Assert.That(point.ToPointData().ToLineProtocol(), Is.EqualTo(expected));
        }

        [Test]
        public void UseGenericObjectAsFieldValue()
        {
            var point = PointData.Builder.Measurement("h2o")
                .AddTag("location", "europe")
                .AddField("custom-object", new GenericObject { Value1 = "test", Value2 = 10 });

            Assert.That(point.ToPointData().ToLineProtocol(),
                Is.EqualTo("h2o,location=europe custom-object=\"test-10\""));
        }
    }
}