using System;
using Apache.Arrow;
using Apache.Arrow.Types;
using InfluxDB3.Client.Internal;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Client.Test.Internal;

public class RecordBatchConverterTest
{
    [Test]
    public void ConvertToPointDataValue()
    {
        Schema schema;
        RecordBatch recordBatch;
        PointDataValues point;
        
        var stringField = new Field("measurement", StringType.Default, true);
        var stringArray = new StringArray.Builder().Append("host").Build();
        schema = new Schema(new[] { stringField, }, null);
        recordBatch = new RecordBatch(schema, new[] { stringArray }, 1);
        point = RecordBatchConverter.ConvertToPointDataValue(recordBatch, 0);
        Assert.That(point.GetMeasurement(), Is.EqualTo("host"));
        
        Assert.Multiple(() =>
        {
            var now = new DateTimeOffset();
            
            var timeField = new Field("time", TimestampType.Default, true);
            var timeArray = new TimestampArray.Builder().Append(now).Build();
            schema = new Schema(new[] { timeField, }, null);
            recordBatch = new RecordBatch(schema, new[] { timeArray }, 1);
            
            point = RecordBatchConverter.ConvertToPointDataValue(recordBatch, 0);
            Assert.That(point.GetTimestamp(), Is.EqualTo(TimestampConverter.GetNanoTime(now.UtcDateTime)));
            
            timeField = new Field("test", TimestampType.Default, true);
            schema = new Schema(new[] { timeField, }, null);
            recordBatch = new RecordBatch(schema, new[] { timeArray }, 1);
            point = RecordBatchConverter.ConvertToPointDataValue(recordBatch, 0);
            Assert.That(point.GetField("test"), Is.EqualTo(now));
        });
    }
}