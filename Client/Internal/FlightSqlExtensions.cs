using System;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Array = Apache.Arrow.Array;

namespace InfluxDB3.Client.Internal;

internal static class FlightSqlExtensions
{
    /// <summary>
    /// Get row value from array. The implementation is based on the
    /// <see href="https://github.com/apache/arrow/blob/main/csharp/src/Apache.Arrow/Arrays/ArrowArrayFactory.cs">ArrowArrayFactory</see>.
    /// </summary>
    /// <param name="array">Array</param>
    /// <param name="index">Row index</param>
    /// <returns></returns>
    /// <exception cref="NotSupportedException">Type of array is not supported</exception>
    internal static object? GetObjectValue(this Array array, int index)
    {
        return array switch
        {
            BooleanArray booleanArray => booleanArray.GetValue(index),
            UInt8Array uInt8Array => uInt8Array.GetValue(index),
            Int8Array int8Array => int8Array.GetValue(index),
            UInt16Array uInt16Array => uInt16Array.GetValue(index),
            Int16Array int16Array => int16Array.GetValue(index),
            UInt32Array uInt32Array => uInt32Array.GetValue(index),
            Int32Array int32Array => int32Array.GetValue(index),
            UInt64Array uInt64Array => uInt64Array.GetValue(index),
            Int64Array int64Array => int64Array.GetValue(index),
            FloatArray floatArray => floatArray.GetValue(index),
            DoubleArray doubleArray => doubleArray.GetValue(index),
            StringArray stringArray => stringArray.GetString(index),
            BinaryArray binaryArray => binaryArray.GetBytes(index).ToArray(),
            TimestampArray timestampArray => timestampArray.GetTimestamp(index),
            Date64Array date64Array => date64Array.GetDateTime(index),
            Date32Array date32Array => date32Array.GetDateTime(index),
            Time32Array time32Array => time32Array.GetValue(index),
            Time64Array time64Array => time64Array.GetValue(index),
            Decimal128Array decimal128Array => decimal128Array.GetValue(index),
            Decimal256Array decimal256Array => decimal256Array.GetValue(index),
            _ => throw new NotSupportedException($"The datatype {array.Data.DataType} is not supported.")
        };
    }
}