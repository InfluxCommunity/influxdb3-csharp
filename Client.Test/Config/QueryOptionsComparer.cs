using System;
using System.Collections.Generic;
using InfluxDB3.Client.Config;

namespace InfluxDB3.Client.Test.Config;

internal class QueryOptionsComparer : IEqualityComparer<QueryOptions>
{
    public bool Equals(QueryOptions x, QueryOptions y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null) return false;
        if (y is null) return false;
        if (x.GetType() != y.GetType()) return false;
        return Nullable.Equals(x.Deadline, y.Deadline) && x.MaxReceiveMessageSize == y.MaxReceiveMessageSize &&
               x.MaxSendMessageSize == y.MaxSendMessageSize && Equals(x.CompressionProviders, y.CompressionProviders) &&
               x.DisableGrpcCompression == y.DisableGrpcCompression;
    }

    public int GetHashCode(QueryOptions obj)
    {
        return HashCode.Combine(obj.Deadline, obj.MaxReceiveMessageSize, obj.MaxSendMessageSize,
            obj.CompressionProviders, obj.DisableGrpcCompression);
    }
}