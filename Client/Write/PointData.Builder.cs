using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using InfluxDB3.Client.Internal;

namespace InfluxDB3.Client.Write
{
    public partial class PointData
    {
        public sealed class Builder
        {
            private readonly string _measurementName;
            private readonly Dictionary<string, string> _tags = new();
            private readonly Dictionary<string, object> _fields = new();

            private BigInteger? _time;

            private Builder(string measurementName)
            {
                Arguments.CheckNonEmptyString(measurementName, "Measurement name");

                _measurementName = measurementName;
            }

            /// <summary>
            /// Create a new Point withe specified a measurement name.
            /// </summary>
            /// <param name="measurementName">the measurement name</param>
            /// <returns>the new Point</returns>
            public static Builder Measurement(string measurementName)
            {
                return new Builder(measurementName);
            }

            /// <summary>
            /// Adds or replaces a tag value for a point.
            /// </summary>
            /// <param name="name">the tag name</param>
            /// <param name="value">the tag value</param>
            /// <returns>this</returns>
            public Builder Tag(string name, string value)
            {
                var isEmptyValue = string.IsNullOrEmpty(value);
                if (isEmptyValue)
                {
                    if (_tags.ContainsKey(name))
                    {
                        Trace.TraceWarning(
                            $"Empty tags will cause deletion of, tag [{name}], measurement [{_measurementName}]");
                        _tags.Remove(name);
                    }
                    else
                    {
                        Trace.TraceWarning($"Empty tags has no effect, tag [{name}], measurement [{_measurementName}]");
                    }
                }
                else
                {
                    _tags[name] = value;
                }

                return this;
            }

            /// <summary>
            /// Add a field with a <see cref="byte"/> value.
            /// </summary>
            /// <param name="name">the field name</param>
            /// <param name="value">the field value</param>
            /// <returns>this</returns>
            public Builder Field(string name, byte value)
            {
                return PutField(name, value);
            }

            /// <summary>
            /// Add a field with a <see cref="float"/> value.
            /// </summary>
            /// <param name="name">the field name</param>
            /// <param name="value">the field value</param>
            /// <returns>this</returns>
            public Builder Field(string name, float value)
            {
                return PutField(name, value);
            }

            /// <summary>
            /// Add a field with a <see cref="double"/> value.
            /// </summary>
            /// <param name="name">the field name</param>
            /// <param name="value">the field value</param>
            /// <returns>this</returns>
            public Builder Field(string name, double value)
            {
                return PutField(name, value);
            }

            /// <summary>
            /// Add a field with a <see cref="decimal"/> value.
            /// </summary>
            /// <param name="name">the field name</param>
            /// <param name="value">the field value</param>
            /// <returns>this</returns>
            public Builder Field(string name, decimal value)
            {
                return PutField(name, value);
            }

            /// <summary>
            /// Add a field with a <see cref="long"/> value.
            /// </summary>
            /// <param name="name">the field name</param>
            /// <param name="value">the field value</param>
            /// <returns>this</returns>
            public Builder Field(string name, long value)
            {
                return PutField(name, value);
            }

            /// <summary>
            /// Add a field with a <see cref="ulong"/> value.
            /// </summary>
            /// <param name="name">the field name</param>
            /// <param name="value">the field value</param>
            /// <returns>this</returns>
            public Builder Field(string name, ulong value)
            {
                return PutField(name, value);
            }

            /// <summary>
            /// Add a field with a <see cref="uint"/> value.
            /// </summary>
            /// <param name="name">the field name</param>
            /// <param name="value">the field value</param>
            /// <returns>this</returns>
            public Builder Field(string name, uint value)
            {
                return PutField(name, value);
            }

            /// <summary>
            /// Add a field with a <see cref="string"/> value.
            /// </summary>
            /// <param name="name">the field name</param>
            /// <param name="value">the field value</param>
            /// <returns>this</returns>
            public Builder Field(string name, string value)
            {
                return PutField(name, value);
            }

            /// <summary>
            /// Add a field with a <see cref="bool"/> value.
            /// </summary>
            /// <param name="name">the field name</param>
            /// <param name="value">the field value</param>
            /// <returns>this</returns>
            public Builder Field(string name, bool value)
            {
                return PutField(name, value);
            }

            /// <summary>
            /// Add a field with an <see cref="object"/> value.
            /// </summary>
            /// <param name="name">the field name</param>
            /// <param name="value">the field value</param>
            /// <returns>this</returns>
            public Builder Field(string name, object value)
            {
                return PutField(name, value);
            }

            /// <summary>
            /// Updates the timestamp for the point.
            /// </summary>
            /// <param name="timestamp">the timestamp</param>
            /// <returns></returns>
            public Builder Timestamp(long timestamp)
            {
                _time = timestamp;
                return this;
            }

            /// <summary>
            /// Updates the timestamp for the point represented by <see cref="TimeSpan"/>.
            /// </summary>
            /// <param name="timestamp">the timestamp</param>
            /// <returns></returns>
            public Builder Timestamp(TimeSpan timestamp)
            {
                _time = TimeSpanToBigInteger(timestamp);
                return this;
            }

            /// <summary>
            /// Updates the timestamp for the point represented by <see cref="DateTime"/>.
            /// </summary>
            /// <param name="timestamp">the timestamp</param>
            /// <returns></returns>
            public Builder Timestamp(DateTime timestamp)
            {
                if (timestamp != null && timestamp.Kind != DateTimeKind.Utc)
                {
                    throw new ArgumentException("Timestamps must be specified as UTC", nameof(timestamp));
                }

                var timeSpan = timestamp.Subtract(EpochStart);

                return Timestamp(timeSpan);
            }

            /// <summary>
            /// Updates the timestamp for the point represented by <see cref="DateTime"/>.
            /// </summary>
            /// <param name="timestamp">the timestamp</param>
            /// <returns></returns>
            public Builder Timestamp(DateTimeOffset timestamp)
            {
                return Timestamp(timestamp.UtcDateTime);
            }

            /// <summary>
            /// Has point any fields?
            /// </summary>
            /// <returns>true, if the point contains any fields, false otherwise.</returns>
            public bool HasFields()
            {
                return _fields.Count > 0;
            }

            /// <summary>
            /// The PointData
            /// </summary>
            /// <returns></returns>
            public PointData ToPointData()
            {
                return new PointData(_measurementName, _time,
                    new SortedDictionary<string, string>(_tags),
                    new SortedDictionary<string, object>(_fields));
            }

            private Builder PutField(string name, object value)
            {
                Arguments.CheckNonEmptyString(name, "Field name");

                _fields[name] = value;
                return this;
            }
        }
    }
}