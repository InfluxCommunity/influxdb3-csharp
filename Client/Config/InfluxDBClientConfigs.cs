using System;

namespace InfluxDB3.Client.Config;

public class InfluxDBClientConfigs
{
    /// <summary>
    /// The configuration of the client.
    /// </summary>
    public InfluxDBClientConfigs()
    {
        Timeout = TimeSpan.FromSeconds(10);
        AllowHttpRedirects = false;
    }

    /// <summary>
    /// The hostname or IP address of the InfluxDB server.
    /// </summary>
    public string Host { get; set; }

    /// <summary>
    /// The authentication token for accessing the InfluxDB server.
    /// </summary>
    public string Token { get; set; }

    /// <summary>
    /// The database to be used for InfluxDB operations.
    /// </summary>
    public string Database { get; set; }

    /// <summary>
    /// Timeout to wait before the HTTP request times out. Default to '10 seconds'.
    /// </summary>
    public TimeSpan Timeout { get; set; }

    /// <summary>
    /// Automatically following HTTP 3xx redirects. Default to 'false'.
    /// </summary>
    public bool AllowHttpRedirects { get; set; }
}