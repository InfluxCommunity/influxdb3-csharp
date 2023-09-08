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
        private const long C1000 = 1000L;
        private const long C1000000 = C1000 * 1000L;
        private const long C1000000000 = C1000000 * 1000L;

        private PointDataValues _values;


        public PointData(PointDataValues values) { _values = values; }

        /// <summary>
        /// Create a new Point withe specified a measurement name.
        /// </summary>
        /// <param name="measurementName">the measurement name</param>
        /// <returns>the new Point</returns>
        public static PointData Measurement(string measurementName)
        {
            return new PointData(new PointDataValues()).SetMeasurement(measurementName);
        }

        /// <summary>
        /// Get measurement name.
        /// </summary>
        /// <returns>Measurement name</returns>
        public string? GetMeasurement()
        {
            return _values.GetMeasurement();
        }

        /// <summary>
        /// Create new Point with this values and provided measurement.
        /// </summary>
        /// <param name="measurementName">the measurement name</param>
        /// <returns>copy of this Point with given measurement name</returns>
        public PointData SetMeasurement(string measurementName)
        {
            _values.SetMeasurement(measurementName);
            return this;
        }

        public BigInteger? GetTimestamp()
        {
            return _values.GetTimestamp();
        }

        /// <summary>
        /// Updates the timestamp for the point.
        /// </summary>
        /// <param name="timestamp">the timestamp</param>
        /// <param name="timeUnit">the timestamp precision. Default is 'nanoseconds'.</param>
        /// <returns></returns>
        public PointData SetTimestamp(long timestamp, WritePrecision? timeUnit = null)
        {
            _values.SetTimestamp(timestamp, timeUnit);
            return this;
        }

        /// <summary>
        /// Updates the timestamp for the point represented by <see cref="TimeSpan"/>.
        /// </summary>
        /// <param name="timestamp">the timestamp</param>
        /// <returns></returns>
        public PointData SetTimestamp(TimeSpan timestamp)
        {
            _values.SetTimestamp(timestamp);
            return this;
        }

        /// <summary>
        /// Updates the timestamp for the point represented by <see cref="DateTime"/>.
        /// </summary>
        /// <param name="timestamp">the timestamp</param>
        /// <returns></returns>
        public PointData SetTimestamp(DateTime timestamp)
        {
            _values.SetTimestamp(timestamp);
            return this;
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

        public string? GetTag(string name)
        {
            return _values.GetTag(name);
        }

        /// <summary>
        /// Adds or replaces a tag value for a point.
        /// </summary>
        /// <param name="name">the tag name</param>
        /// <param name="value">the tag value</param>
        /// <returns>this</returns>
        public PointData SetTag(string name, string value)
        {
            _values.SetTag(name, value);
            return this;
        }

        public PointData RemoveTag(string name)
        {
            _values.RemoveTag(name);
            return this;
        }

        public string[] GetTagNames()
        {
            return _values.GetTagNames();
        }



        public double? GetFloatField(string name)
        {
            return _values.GetFloatField(name);
        }

        public PointData SetFloatField(string name, float value)
        {
            _values.SetFloatField(name, value);
            return this;
        }

        public PointData SetFloatField(string name, double value)
        {
            _values.SetFloatField(name, value);
            return this;
        }


        public int? GetIntegerField(string name)
        {
            return _values.GetIntegerField(name);
        }

        public PointData SetIntegerField(string name, byte value)
        {
            _values.SetIntegerField(name, value);
            return this;
        }

        public PointData SetIntegerField(string name, int value)
        {
            _values.SetIntegerField(name, value);
            return this;
        }

        public PointData SetIntegerField(string name, long value)
        {
            _values.SetIntegerField(name, value);
            return this;
        }

        public PointData SetIntegerField(string name, sbyte value)
        {
            _values.SetIntegerField(name, value);
            return this;
        }

        public PointData SetIntegerField(string name, short value)
        {
            _values.SetIntegerField(name, value);
            return this;
        }


        public uint? GetUintegerField(string name)
        {
            return _values.GetUintegerField(name);
        }

        public PointData SetUintegerField(string name, uint value)
        {
            _values.SetUintegerField(name, value);
            return this;
        }

        public PointData SetUintegerField(string name, ulong value)
        {
            _values.SetUintegerField(name, value);
            return this;
        }

        public PointData SetUintegerField(string name, ushort value)
        {
            _values.SetUintegerField(name, value);
            return this;
        }


        public string? GetStringField(string name)
        {
            return _values.GetStringField(name);
        }

        public PointData SetStringField(string name, string value)
        {
            _values.SetStringField(name, value);
            return this;
        }


        public bool? GetBooleanField(string name)
        {
            return _values.GetBooleanField(name);
        }

        public PointData SetBooleanField(string name, bool value)
        {
            _values.SetBooleanField(name, value);
            return this;
        }


        /// <summary>
        /// Get field of given name. Can be null if field doesn't exist.
        /// </summary>
        /// <returns>Field as object</returns>
        public object? GetField(string name)
        {
            return _values.GetField(name);
        }

        /// <summary>
        /// Get field of given name as type. Can be null if field doesn't exist.
        /// </summary>
        /// <returns>Field as given type</returns>
        /// <exception cref="InvalidCastException">Field doesn't match given type</exception>
        public T? GetField<T>(string name) where T : struct
        {
            return _values.GetField<T>(name);
        }

        public Type? GetFieldType(string name) {
            return _values.GetFieldType(name);
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="byte"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData SetField(string name, byte value)
        {
            _values.SetField(name, value);
            return this;
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="float"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData SetField(string name, float value)
        {
            _values.SetField(name, value);
            return this;
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="double"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData SetField(string name, double value)
        {
            _values.SetField(name, value);
            return this;
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="decimal"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData SetField(string name, decimal value)
        {
            _values.SetField(name, value);
            return this;
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="long"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData SetField(string name, long value)
        {
            _values.SetField(name, value);
            return this;
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="ulong"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData SetField(string name, ulong value)
        {
            _values.SetField(name, value);
            return this;
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="uint"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData SetField(string name, uint value)
        {
            _values.SetField(name, value);
            return this;
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="string"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData SetField(string name, string value)
        {
            _values.SetField(name, value);
            return this;
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="bool"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData SetField(string name, bool value)
        {
            _values.SetField(name, value);
            return this;
        }

        /// <summary>
        /// Adds or replaces a field with an <see cref="object"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointData SetField(string name, object value)
        {
            _values.SetField(name, value);
            return this;
        }

        public PointData SetFields(Dictionary<string, object> fields) {
            _values.SetFields(fields);
            return this;
        }

        public PointData RemoveField(string name) {
            _values.RemoveField(name);
            return this;
        }

        public string[] GetFieldNames() {
            return _values.GetFieldNames();
        }

        /// <summary>
        /// Has point any fields?
        /// </summary>
        /// <returns>true, if the point contains any fields, false otherwise.</returns>
        public bool HasFields()
        {
            return _values.HasFields();
        }

        public PointData Copy() {
            return new PointData(_values.Copy());
        }

        /// <summary>
        /// Transform to Line Protocol.
        /// </summary>
        /// <param name="timeUnit">the timestamp precision</param>
        /// <returns>Line Protocol</returns>
        public string ToLineProtocol(WritePrecision? timeUnit = null)
        {
            var sb = new StringBuilder();

            EscapeKey(sb, _values.GetMeasurement()!, false);
            AppendTags(sb);
            var appendedFields = AppendFields(sb);
            if (!appendedFields)
            {
                return "";
            }

            AppendTime(sb, timeUnit);

            return sb.ToString();
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
            foreach (var name in _values.GetTagNames())
            {
                var value = _values.GetTag(name);

                if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(value))
                {
                    continue;
                }

                writer.Append(',');
                EscapeKey(writer, name);
                writer.Append('=');
                EscapeKey(writer, value!);
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

            foreach (var name in _values.GetFieldNames())
            {
                var value = _values.GetField(name)!;

                if (IsNotDefined(value))
                {
                    continue;
                }

                EscapeKey(sb, name);
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
            var time = _values.GetTimestamp();
            if (time == null)
            {
                return;
            }

            var timestamp = (BigInteger)time;
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

            return _values.Equals(other._values);
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            return _values.GetHashCode();
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