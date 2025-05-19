using System;
using System.Collections.Generic;
using Grpc.Net.Compression;

namespace InfluxDB3.Client.Config;

/// <summary>
/// Represents the options for configuring query behavior in the client.
/// </summary>
public class QueryOptions : ICloneable, IEquatable<QueryOptions>
{
    /// <summary>
    /// Gets or sets the optional deadline for query execution.
    /// </summary>
    /// <remarks>
    /// This property specifies the maximum time allowed for query execution before a timeout.
    /// If set to <c>null</c>, no deadline is applied. The value is represented as a nullable <see cref="DateTime"/>.
    /// </remarks>
    public DateTime? Deadline { get; set; }

    /// <summary>
    /// Gets or sets the maximum size, in bytes, of a single message that can be received.
    /// </summary>
    /// <remarks>
    /// This property defines an optional limit for the size of incoming messages to avoid excessive memory allocation.
    /// A value of <c>null</c> specifies that no maximum size is enforced. The default value is 4,194,304 bytes (4 MB).
    /// </remarks>
    public int? MaxReceiveMessageSize { get; set; }

    /// <summary>
    /// Gets or sets the maximum size of a message that can be sent.
    /// </summary>
    /// <remarks>
    /// This property defines the maximum allowable size, in bytes, for messages sent by the client.
    /// If set to <c>null</c>, there is no limit on the size of the sent messages.
    /// </remarks>
    public int? MaxSendMessageSize { get; set; }

    /// <summary>
    /// Gets or sets the collection of compression providers used for gRPC message compression.
    /// </summary>
    /// <remarks>
    /// This property specifies the list of compression algorithms available for compressing gRPC messages.
    /// The value is represented as a nullable list of <see cref="ICompressionProvider"/>.
    /// If set to <c>null</c>, Gzip will be used
    /// </remarks>
    public IList<ICompressionProvider>? CompressionProviders { get; set; }

    /// <summary>
    /// Represents the default query options used throughout the client configuration.
    /// </summary>
    /// <remarks>
    /// This variable is a static, pre-configured instance of <see cref="QueryOptions"/> with default values.
    /// It specifies parameters such as deadlines, message sizes, and compression for query execution.
    /// </remarks>
    internal static readonly QueryOptions DefaultOptions = new()
    {
        Deadline = null,
        MaxReceiveMessageSize = 4_194_304,
        MaxSendMessageSize = null,
        CompressionProviders = null
    };

    /// <summary>
    /// Creates a shallow copy of the current QueryOptions instance.
    /// </summary>
    /// <returns>
    /// A new object that is a shallow copy of the current QueryOptions instance.
    /// </returns>
    public object Clone()
    {
        return MemberwiseClone();
    }

    public bool Equals(QueryOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return Nullable.Equals(Deadline, other.Deadline) && MaxReceiveMessageSize == other.MaxReceiveMessageSize &&
               MaxSendMessageSize == other.MaxSendMessageSize &&
               Equals(CompressionProviders, other.CompressionProviders);
    }

    public override bool Equals(object? obj)
    {
        if (obj is null) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != GetType()) return false;
        return Equals((QueryOptions)obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            var hashCode = Deadline.GetHashCode();
            hashCode = (hashCode * 397) ^ MaxReceiveMessageSize.GetHashCode();
            hashCode = (hashCode * 397) ^ MaxSendMessageSize.GetHashCode();
            hashCode = (hashCode * 397) ^ (CompressionProviders != null ? CompressionProviders.GetHashCode() : 0);
            return hashCode;
        }
    }
}