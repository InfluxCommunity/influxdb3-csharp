using InfluxDB3.Client.Write;

namespace InfluxDB3.Client.Config;

public class WriteOptions
{
    /// <summary>
    /// The default precision to use for the timestamp of points if no precision is specified in the write API call.
    /// </summary>
    public WritePrecision? Precision { get; set; }

    /// <summary>
    /// The threshold in bytes for gzipping the body.
    /// </summary>
    public int GzipThreshold { get; set; }

    internal static readonly WriteOptions DefaultOptions = new()
    {
        Precision = WritePrecision.Ns,
        GzipThreshold = 1000
    };
}
