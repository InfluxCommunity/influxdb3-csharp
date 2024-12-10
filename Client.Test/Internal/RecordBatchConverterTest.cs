using Apache.Arrow;
using Apache.Arrow.Types;
using InfluxDB3.Client.Internal;

namespace InfluxDB3.Client.Test.Internal;

public class RecordBatchConverterTest
{
    [Test]
    public void ConvertToPointDataValue()
    {
        var stringField = new Field("measurement", StringType.Default, true);
        var stringArray = new StringArray.Builder().Append("host").Build();
        var schema = new Schema(new[] { stringField, }, null);
        var recordBatch = new RecordBatch(schema, new[] { stringArray }, 1);
        var point = RecordBatchConverter.ConvertToPointDataValue(recordBatch, 0);
        Assert.That(point.GetMeasurement(), Is.EqualTo("host"));
    }
}