using System;
using System.Numerics;
using Apache.Arrow;
using InfluxDB3.Client.Write;
using ArrowArray = Apache.Arrow.Array;

namespace InfluxDB3.Client.Internal;

public static class RecordBatchConverter
{
    /// <summary>
    /// Convert a given row of data from RecordBatch to PointDataValues
    /// </summary>
    /// <param name="recordBatch">The RecordBatch to get data from</param>
    /// <param name="rowNumber">The row number</param>
    /// <returns>The PointDataValues</returns>
    public static PointDataValues ConvertToPointDataValue(RecordBatch recordBatch, int rowNumber)
    {
        PointDataValues point = new();
        for (var columnIndex = 0; columnIndex < recordBatch.ColumnCount; columnIndex++)
        {
            var schema = recordBatch.Schema.FieldsList[columnIndex];
            var fullName = schema.Name;

            if (recordBatch.Column(columnIndex) is not ArrowArray array)
                continue;

            var objectValue = array.GetObjectValue(rowNumber);
            if (fullName is "measurement" or "iox::measurement" &&
                objectValue is string value)
            {
                point = point.SetMeasurement(value);
                continue;
            }

            if (!schema.HasMetadata)
            {
                if (fullName == "time" && objectValue is DateTimeOffset timestamp)
                {
                    point = point.SetTimestamp(timestamp);
                }
                else
                    // just push as field If you don't know what type is it
                    point = point.SetField(fullName, objectValue);

                continue;
            }

            var type = schema.Metadata["iox::column::type"];
            var parts = type.Split(new[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
            var valueType = parts[2];
            // string fieldType = parts.Length > 3 ? parts[3] : "";
            var mappedValue = TypeCasting.GetMappedValue(schema, objectValue);
            if (valueType == "field")
            {
                point = point.SetField(fullName, mappedValue);
            }
            else if (valueType == "tag")
            {
                point = point.SetTag(fullName, (string)mappedValue);
            }
            else if (valueType == "timestamp")
            {
                point = point.SetTimestamp((BigInteger)mappedValue);
            }
        }

        return point;
    }
}