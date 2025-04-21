using System;

namespace InfluxDB3.Client.Config;

public class QueryOptions : ICloneable
{
    public DateTime? Deadline { get; set; }

    public int? MaxReceiveMessageSize { get; set; }

    public int? MaxSendMessageSize { get; set; }

    public object Clone()
    {
        return MemberwiseClone();
    }

    internal static readonly QueryOptions DefaultOptions = new()
    {
        Deadline = null,
        MaxReceiveMessageSize = 4_194_304,
        MaxSendMessageSize = null
    };
}