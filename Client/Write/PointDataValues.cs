using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using InfluxDB3.Client.Internal;

namespace InfluxDB3.Client.Write
{
    /// <summary>
    /// Point defines the values that will be written to the database.
    /// <a href="http://bit.ly/influxdata-point">See Go Implementation</a>.
    /// </summary>
    public class PointDataValues : IEquatable<PointDataValues>
    {
        private static readonly DateTime EpochStart = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private string? _measurementName;

        private SortedDictionary<string, string> _tags = new();
        private SortedDictionary<string, object> _fields = new();

        private BigInteger? _time;

        private const long C1000 = 1000L;
        private const long C1000000 = C1000 * 1000L;
        private const long C1000000000 = C1000000 * 1000L;

        public PointDataValues() { }

        /// <summary>
        /// Create a new Point withe specified a measurement name.
        /// </summary>
        /// <param name="measurementName">the measurement name</param>
        /// <returns>the new Point</returns>
        public static PointDataValues Measurement(string measurementName)
        {
            return new PointDataValues().SetMeasurement(measurementName);
        }

        /// <summary>
        /// Get measurement name.
        /// </summary>
        /// <returns>Measurement name</returns>
        public string? GetMeasurement()
        {
            return _measurementName;
        }

        /// <summary>
        /// Create new Point with this values and provided measurement.
        /// </summary>
        /// <param name="measurementName">the measurement name</param>
        /// <returns>copy of this Point with given measurement name</returns>
        public PointDataValues SetMeasurement(string measurementName)
        {
            _measurementName = measurementName;
            return this;
        }

        public BigInteger? GetTimestamp()
        {
            return _time;
        }

        /// <summary>
        /// Updates the timestamp for the point.
        /// </summary>
        /// <param name="timestamp">the timestamp</param>
        /// <param name="timeUnit">the timestamp precision. Default is 'nanoseconds'.</param>
        /// <returns></returns>
        public PointDataValues SetTimestamp(long timestamp, WritePrecision? timeUnit = null)
        {
            _time = LongToBigInteger(timestamp, timeUnit);
            return this;
        }

        /// <summary>
        /// Updates the timestamp for the point represented by <see cref="TimeSpan"/>.
        /// </summary>
        /// <param name="timestamp">the timestamp</param>
        /// <returns></returns>
        public PointDataValues SetTimestamp(TimeSpan timestamp)
        {
            _time = TimeSpanToBigInteger(timestamp);
            return this;
        }

        /// <summary>
        /// Updates the timestamp for the point represented by <see cref="DateTime"/>.
        /// </summary>
        /// <param name="timestamp">the timestamp</param>
        /// <returns></returns>
        public PointDataValues SetTimestamp(DateTime timestamp)
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
        public PointDataValues SetTimestamp(DateTimeOffset timestamp)
        {
            return SetTimestamp(timestamp.UtcDateTime);
        }

        /// <summary>
        /// Gets value of tag with given name. Returns null if tag not found.
        /// </summary>
        ///
        /// <param name="name">the tag name</param>
        /// <returns>tag value or null</returns>
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
        public PointDataValues SetTag(string name, string value)
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

        public PointDataValues RemoveTag(string name)
        {
            _tags.Remove(name);
            return this;
        }

        public string[] GetTagNames()
        {
            return _tags.Keys.ToArray();
        }



        public double? GetFloatField(string name)
        {
            return _fields.TryGetValue(name, out object result) ? (double)result : null;
        }

        public PointDataValues SetFloatField(string name, double value)
        {
            PutField(name, value);
            return this;
        }


        public long? GetIntegerField(string name)
        {
            return _fields.TryGetValue(name, out object result) ? (long)result : null;
        }

        public PointDataValues SetIntegerField(string name, long value)
        {
            PutField(name, value);
            return this;
        }

        public ulong? GetUintegerField(string name)
        {
            return _fields.TryGetValue(name, out object result) ? (ulong)result : null;
        }

        public PointDataValues SetUintegerField(string name, ulong value)
        {
            PutField(name, value);
            return this;
        }

        public string? GetStringField(string name)
        {
            return _fields.TryGetValue(name, out object result) ? (string)result : null;
        }

        public PointDataValues SetStringField(string name, string value)
        {
            PutField(name, value);
            return this;
        }


        public bool? GetBooleanField(string name)
        {
            return _fields.TryGetValue(name, out object result) ? (bool)result : null;
        }

        public PointDataValues SetBooleanField(string name, bool value)
        {
            PutField(name, value);
            return this;
        }


        /// <summary>
        /// Get field of given name. Can be null if field doesn't exist.
        /// </summary>
        /// <returns>Field as object</returns>
        public object? GetField(string name)
        {
            return _fields.TryGetValue(name, out object value) ? value : null;
        }

        /// <summary>
        /// Get field of given name as type. Can be null if field doesn't exist.
        /// </summary>
        /// <returns>Field as given type</returns>
        /// <exception cref="InvalidCastException">Field doesn't match given type</exception>
        public T? GetField<T>(string name) where T : struct
        {
            return _fields.TryGetValue(name, out object value) ? (T)value : null;
        }

        public Type? GetFieldType(string name) {
            return _fields.TryGetValue(name, out object value) ? value.GetType() : null;
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="byte"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointDataValues SetField(string name, byte value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="float"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointDataValues SetField(string name, float value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="double"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointDataValues SetField(string name, double value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="decimal"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointDataValues SetField(string name, decimal value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="long"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointDataValues SetField(string name, long value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="ulong"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointDataValues SetField(string name, ulong value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="uint"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointDataValues SetField(string name, uint value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="string"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointDataValues SetField(string name, string value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Adds or replaces a field with a <see cref="bool"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointDataValues SetField(string name, bool value)
        {
            return PutField(name, value);
        }

        /// <summary>
        /// Adds or replaces a field with an <see cref="object"/> value.
        /// </summary>
        /// <param name="name">the field name</param>
        /// <param name="value">the field value</param>
        /// <returns>this</returns>
        public PointDataValues SetField(string name, object value)
        {
            return PutField(name, value);
        }

        private PointDataValues PutField(string name, object value)
        {
            // TODO: move to setfield
            Arguments.CheckNonEmptyString(name, "Field name");

            _fields[name] = value;
            return this;
        }

        public PointDataValues SetFields(Dictionary<string, object> fields) {
            foreach (var item in fields)
            {
                SetField(item.Key, item.Value);
            }
            return this;
        }

        public PointDataValues RemoveField(string name) {
            _fields.Remove(name);
            return this;
        }

        public string[] GetFieldNames() {
            return _fields.Keys.ToArray();
        }

        /// <summary>
        /// Has point any fields?
        /// </summary>
        /// <returns>true, if the point contains any fields, false otherwise.</returns>
        public bool HasFields()
        {
            return _fields.Count > 0;
        }

        public PointDataValues Copy() {
            return new PointDataValues
            {
                _measurementName = _measurementName,
                _tags = new SortedDictionary<string, string>(_tags),
                _fields = new SortedDictionary<string, object>(_fields),
                _time = _time,
            };
        }

        public PointData AsPoint(string measurement) {
            SetMeasurement(measurement);
            return AsPoint();
        }

        public PointData AsPoint() {
            return PointData.fromValues(this);
        }

        public PointData AsPointData() {
            return AsPoint();
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
        /// Determines whether the specified <see cref="System.Object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object" /> to compare with this instance.</param>
        /// <returns>
        ///   <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object? obj)
        {
            return Equals(obj as PointDataValues);
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other">other</paramref> parameter; otherwise, false.
        /// </returns>
        public bool Equals(PointDataValues? other)
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
        public static bool operator ==(PointDataValues? left, PointDataValues? right)
        {
            if (left is null && right is null)
                return true;
            if (left is null || right is null)
                return false;

            return EqualityComparer<PointDataValues>.Default.Equals(left, right);
        }

        /// <summary>
        /// Implements the operator !=.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator !=(PointDataValues left, PointDataValues right)
        {
            return !(left == right);
        }
    }
}