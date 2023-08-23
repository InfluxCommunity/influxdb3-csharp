using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Text;
using InfluxDB3.Client.Internal;

namespace InfluxDB3.Client.Write
{
    /// <summary>
    /// Point defines the values that will be written to the database.
    /// <a href="http://bit.ly/influxdata-point">See Go Implementation</a>.
    /// </summary>
    public partial class PointData : IEquatable<PointData>
    {
        private static readonly DateTime EpochStart = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private readonly string _measurementName;

        private readonly SortedDictionary<string, string> _tags = new();
        private readonly SortedDictionary<string, object> _fields = new();

        private readonly BigInteger? _time;

        private const long C1000 = 1000L;
        private const long C1000000 = C1000 * 1000L;
        private const long C1000000000 = C1000000 * 1000L;

        private PointData(string measurementName)
        {
            Arguments.CheckNonEmptyString(measurementName, "Measurement name");

            _measurementName = measurementName;
        }

        /// <summary>
        /// Create a new Point withe specified a measurement name.
        /// </summary>
        /// <param name="measurementName">the measurement name</param>
        /// <returns>the new Point</returns>
        public static PointData Measurement(string measurementName)
        {
            return new PointData(measurementName);
        }

        private PointData(string measurementName, BigInteger? time, SortedDictionary<string, string> tags,
            SortedDictionary<string, object> fields)
        {
            _measurementName = measurementName;
            _time = time;
            _tags = tags;
            _fields = fields;
        }

        public string GetMeasurement()
        {
            return _measurementName;
        }

        public BigInteger? GetTime()
        {
            return _time;
        }

        public string? GetTag(string name)
        {
            return _tags.TryGetValue(name, out string value) ? value : null;
        }

        /// <summary>
        /// Adds or replaces a tag value for a point.
        /// </summary>
        /// <param name="name">the tag name</param>
        /// <param name="value">the tag value</param>
        /// <returns>this</returns>
        public PointData AddTag(string name, string value)
        {
            var isEmptyValue = string.IsNullOrEmpty(value);
            var tags = new SortedDictionary<string, string>(_tags);
            if (isEmptyValue)
            {
                if (tags.ContainsKey(name))
                {
                    Trace.TraceWarning(
                        $"Empty tags will cause deletion of, tag [{name}], measurement [{_measurementName}]");
                }
                else
                {
                    Trace.TraceWarning($"Empty tags has no effect, tag [{name}], measurement [{_measurementName}]");
                    return this;
                }
            }

            if (tags.ContainsKey(name))
            {
                tags.Remove(name);
            }

            if (!isEmptyValue)
            {
                tags.Add(name, value);
            }

            return new PointData(_measurementName, _time, tags, _fields);
        }

        /// <summary>
        /// Add a field with a <see cref="byte"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData AddField(string name, byte value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Add a field with a <see cref="float"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData AddField(string name, float value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Add a field with a <see cref="double"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData AddField(string name, double value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Add a field with a <see cref="decimal"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData AddField(string name, decimal value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Add a field with a <see cref="long"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData AddField(string name, long value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Add a field with a <see cref="ulong"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData AddField(string name, ulong value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Add a field with a <see cref="uint"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData AddField(string name, uint value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Add a field with a <see cref="string"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData AddField(string name, string value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Add a field with a <see cref="bool"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData AddField(string name, bool value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Add a field with an <see cref="object"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData AddField(string name, object value)
        {
            return PutField(name, value);
        }


        public PointData SetMeasurement(string measurementName)
        {
            return new PointData(
                measurementName,
                _time,
                _tags,
                _fields
            );
        }

        /// <summary>
        /// Updates the timestamp for the point.
        /// </summary>
        /// <param name="timestamp">the timestamp</param>
        /// <param name="timeUnit">the timestamp precision. Default is 'nanoseconds'.</param>
        /// <returns></returns>
        public PointData SetTimestamp(long timestamp, WritePrecision? timeUnit = null)
        {
            return new PointData(_measurementName,
                LongToBigInteger(timestamp, timeUnit),
                _tags,
                _fields);
        }

        /// <summary>
        /// Updates the timestamp for the point represented by <see cref="TimeSpan"/>.
        /// </summary>
        /// <param name="timestamp">the timestamp</param>
        /// <returns></returns>
        public PointData SetTimestamp(TimeSpan timestamp)
        {
            var time = TimeSpanToBigInteger(timestamp);
            return new PointData(_measurementName,
                time,
                _tags,
                _fields);
        }

        /// <summary>
        /// Updates the timestamp for the point represented by <see cref="DateTime"/>.
        /// </summary>
        /// <param name="timestamp">the timestamp</param>
        /// <returns></returns>
        public PointData SetTimestamp(DateTime timestamp)
        {
            var utcTimestamp = timestamp.Kind switch
            {
                DateTimeKind.Local => timestamp.ToUniversalTime(),
                DateTimeKind.Unspecified => DateTime.SpecifyKind(timestamp, DateTimeKind.Utc),
                _ => timestamp
            };

            var timeSpan = utcTimestamp.Subtract(EpochStart);

            return SetTimestamp(timeSpan);
        }

        /// <summary>
        /// Updates the timestamp for the point represented by <see cref="DateTime"/>.
        /// </summary>
        /// <param name="timestamp">the timestamp</param>
        /// <returns></returns>
        public PointData SetTimestamp(DateTimeOffset timestamp)
        {
            return SetTimestamp(timestamp.UtcDateTime);
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
        /// Transform to Line Protocol.
        /// </summary>
        /// <param name="timeUnit">the timestamp precision</param>
        /// <returns>Line Protocol</returns>
        public string ToLineProtocol(WritePrecision? timeUnit = null)
        {
            var sb = new StringBuilder();

            EscapeKey(sb, _measurementName, false);
            AppendTags(sb);
            var appendedFields = AppendFields(sb);
            if (!appendedFields)
            {
                return "";
            }

            AppendTime(sb, timeUnit);

            return sb.ToString();
        }

        // public object? GetField(string name) {
        //     return _fields.TryGetValue(name, out object value) ? value : null;
        // }

        public T? GetField<T>(string name) where T : struct
        {
            return _fields.TryGetValue(name, out object value) ? (T)value : null;
        }

        private PointData PutField(string name, object value)
        {
            Arguments.CheckNonEmptyString(name, "Field name");

            var fields = new SortedDictionary<string, object>(_fields);
            if (fields.ContainsKey(name))
            {
                fields.Remove(name);
            }

            fields.Add(name, value);

            return new PointData(_measurementName,
                _time,
                _tags,
                fields);
        }

        private static BigInteger TimeSpanToBigInteger(TimeSpan timestamp)
        {
            return timestamp.Ticks * 100;
        }

        private static BigInteger LongToBigInteger(long timestamp, WritePrecision? timeUnit = null)
        {
            switch (timeUnit ?? WritePrecision.Ns)
            {
                case WritePrecision.Us:
                    return timestamp * C1000;
                case WritePrecision.Ms:
                    return timestamp * C1000000;
                case WritePrecision.S:
                    return timestamp * C1000000000;
                case WritePrecision.Ns:
                default:
                    return timestamp;
            }
        }

        /// <summary>
        /// Appends the tags.
        /// </summary>
        /// <param name="writer">The writer.</param>
        private void AppendTags(StringBuilder writer)
        {
            foreach (var keyValue in _tags)
            {
                var key = keyValue.Key;
                var value = keyValue.Value;

                if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(value))
                {
                    continue;
                }

                writer.Append(',');
                EscapeKey(writer, key);
                writer.Append('=');
                EscapeKey(writer, value);
            }

            writer.Append(' ');
        }

        /// <summary>
        /// Appends the fields.
        /// </summary>
        /// <param name="sb">The sb.</param>
        /// <returns></returns>
        private bool AppendFields(StringBuilder sb)
        {
            var appended = false;

            foreach (var keyValue in _fields)
            {
                var key = keyValue.Key;
                var value = keyValue.Value;

                if (IsNotDefined(value))
                {
                    continue;
                }

                EscapeKey(sb, key);
                sb.Append('=');

                if (value is float)
                {
                    sb.Append(((IConvertible)value).ToString(CultureInfo.InvariantCulture));
                }
                else if (value is double)
                {
                    var valueStr = ((double)value).ToString("G17", CultureInfo.InvariantCulture);
                    sb.Append((IConvertible)valueStr);
                }
                else if (value is uint || value is ulong || value is ushort)
                {
                    sb.Append(((IConvertible)value).ToString(CultureInfo.InvariantCulture));
                    sb.Append('u');
                }
                else if (value is byte || value is int || value is long || value is sbyte || value is short)
                {
                    sb.Append(((IConvertible)value).ToString(CultureInfo.InvariantCulture));
                    sb.Append('i');
                }
                else if (value is bool b)
                {
                    sb.Append(b ? "true" : "false");
                }
                else if (value is string s)
                {
                    sb.Append('"');
                    EscapeValue(sb, s);
                    sb.Append('"');
                }
                else if (value is IConvertible c)
                {
                    sb.Append(c.ToString(CultureInfo.InvariantCulture));
                }
                else
                {
                    sb.Append('"');
                    EscapeValue(sb, value.ToString());
                    sb.Append('"');
                }

                sb.Append(',');
                appended = true;
            }

            if (appended)
            {
                sb.Remove(sb.Length - 1, 1);
            }

            return appended;
        }

        /// <summary>
        /// Appends the time.
        /// </summary>
        /// <param name="sb">The sb.</param>
        /// <param name="writePrecision"></param>
        private void AppendTime(StringBuilder sb, WritePrecision? writePrecision)
        {
            if (_time == null)
            {
                return;
            }

            var timestamp = (BigInteger)_time;
            switch (writePrecision ?? WritePrecision.Ns)
            {
                case WritePrecision.Us:
                    timestamp /= C1000;
                    break;
                case WritePrecision.Ms:
                    timestamp /= C1000000;
                    break;
                case WritePrecision.S:
                    timestamp /= C1000000000;
                    break;
                case WritePrecision.Ns:
                default:
                    break;
            }

            sb.Append(' ');
            sb.Append(timestamp.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Escapes the key.
        /// </summary>
        /// <param name="sb">The sb.</param>
        /// <param name="key">The key.</param>
        /// <param name="escapeEqual">Configure to escaping equal.</param>
        private void EscapeKey(StringBuilder sb, string key, bool escapeEqual = true)
        {
            foreach (var c in key)
            {
                switch (c)
                {
                    case '\n':
                        sb.Append("\\n");
                        continue;
                    case '\r':
                        sb.Append("\\r");
                        continue;
                    case '\t':
                        sb.Append("\\t");
                        continue;
                    case ' ':
                    case ',':
                        sb.Append("\\");
                        break;
                    case '=':
                        if (escapeEqual)
                        {
                            sb.Append("\\");
                        }

                        break;
                }

                sb.Append(c);
            }
        }

        /// <summary>
        /// Escapes the value.
        /// </summary>
        /// <param name="sb">The sb.</param>
        /// <param name="value">The value.</param>
        private void EscapeValue(StringBuilder sb, string value)
        {
            foreach (var c in value)
            {
                switch (c)
                {
                    case '\\':
                    case '\"':
                        sb.Append("\\");
                        break;
                }

                sb.Append(c);
            }
        }

        /// <summary>
        /// Determines whether [is not defined] [the specified value].
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        ///   <c>true</c> if [is not defined] [the specified value]; otherwise, <c>false</c>.
        /// </returns>
        private bool IsNotDefined(object? value)
        {
            return value == null
                   || value is double d && (double.IsInfinity(d) || double.IsNaN(d))
                   || value is float f && (float.IsInfinity(f) || float.IsNaN(f));
        }

        /// <summary>
        /// Determines whether the specified <see cref="System.Object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object" /> to compare with this instance.</param>
        /// <returns>
        ///   <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object? obj)
        {
            return Equals(obj as PointData);
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other">other</paramref> parameter; otherwise, false.
        /// </returns>
        public bool Equals(PointData? other)
        {
            if (other == null)
            {
                return false;
            }

            var otherTags = other._tags;

            var result = _tags.Count == otherTags.Count &&
                         _tags.All(pair =>
                         {
                             var key = pair.Key;
                             var value = pair.Value;
                             return otherTags.ContainsKey(key) &&
                                    otherTags[key] == value;
                         });
            var otherFields = other._fields;
            result = result && _fields.Count == otherFields.Count &&
                     _fields.All(pair =>
                     {
                         var key = pair.Key;
                         var value = pair.Value;
                         return otherFields.ContainsKey(key) &&
                                Equals(otherFields[key], value);
                     });

            result = result &&
                     _measurementName == other._measurementName &&
                     EqualityComparer<BigInteger?>.Default.Equals(_time, other._time);

            return result;
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            var hashCode = 318335609;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(_measurementName);
            hashCode = hashCode * -1521134295 + _time.GetHashCode();

            foreach (var pair in _tags)
            {
                hashCode = hashCode * -1521134295 + pair.Key?.GetHashCode() ?? 0;
                hashCode = hashCode * -1521134295 + pair.Value?.GetHashCode() ?? 0;
            }

            foreach (var pair in _fields)
            {
                hashCode = hashCode * -1521134295 + pair.Key?.GetHashCode() ?? 0;
                hashCode = hashCode * -1521134295 + pair.Value?.GetHashCode() ?? 0;
            }

            return hashCode;
        }

        /// <summary>
        /// Implements the operator ==.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator ==(PointData? left, PointData? right)
        {
            if (left is null && right is null)
                return true;
            if (left is null || right is null)
                return false;

            return EqualityComparer<PointData>.Default.Equals(left, right);
        }

        /// <summary>
        /// Implements the operator !=.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator !=(PointData left, PointData right)
        {
            return !(left == right);
        }
    }
}