using System;
using System.Diagnostics;
using System.Numerics;
using Apache.Arrow;

namespace InfluxDB3.Client.Internal;

public class TypeCasting
{
    /// <summary>
    /// Function to cast value return based on metadata from InfluxDB.
    /// </summary>
    /// <param name="field">The Field object from Arrow</param>
    /// <param name="value">The value to cast</param>
    /// <returns>The value with the correct type</returns>
    public static object? GetMappedValue(Field field, object? value)
    {
        if (value == null)
            return null;

        var fieldName = field.Name;
        var metaType = field.HasMetadata ? field.Metadata["iox::column::type"] : null;
        if (metaType == null)
        {
            if (fieldName == "time" && value is DateTimeOffset timeOffset)
            {
                return TimestampConverter.GetNanoTime(timeOffset.UtcDateTime);
            }

            return value;
        }

        var parts = metaType.Split(new[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
        var valueType = parts[2];
        if (valueType == "field")
        {
            switch (metaType)
            {
                case "iox::column_type::field::integer":
                    if (IsNumber(value))
                    {
                        return Convert.ToInt64(value);
                    }

                    Trace.TraceWarning($"Value [{value}] is not a long");
                    return value;
                case "iox::column_type::field::uinteger":
                    if (IsNumber(value) && Convert.ToInt64(value) >= 0)
                    {
                        return Convert.ToUInt64(value);
                    }

                    Trace.TraceWarning($"Value [{value}] is not an unsigned long");
                    return value;
                case "iox::column_type::field::float":
                    if (IsNumber(value))
                    {
                        return Convert.ToDouble(value);
                    }

                    Trace.TraceWarning($"Value [{value}] is not a double");
                    return value;
                case "iox::column_type::field::string":
                    if (value is string)
                    {
                        return value;
                    }

                    Trace.TraceWarning($"Value [{value}] is not a string");
                    return value;
                case "iox::column_type::field::boolean":
                    if (value is bool)
                    {
                        return Convert.ToBoolean(value);
                    }

                    Trace.TraceWarning($"Value [{value}] is not a boolean");
                    return value;
                default:
                    return value;
            }
        }

        if (valueType == "timestamp" && value is DateTimeOffset dateTimeOffset)
        {
            return TimestampConverter.GetNanoTime(dateTimeOffset.UtcDateTime);
        }

        return value;
    }

    public static bool IsNumber(object? value)
    {
        return value is sbyte
            or byte
            or short
            or ushort
            or int
            or uint
            or long
            or ulong
            or float
            or double
            or decimal
            or BigInteger;
    }
}