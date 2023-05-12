using System;
using Grpc.Core;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Client.Config;

public class InfluxDBClientConfigs
{
    private string _host = "";

    /// <summary>
    /// The configuration of the client.
    /// </summary>
    public InfluxDBClientConfigs()
    {
    }

    /// <summary>
    /// The hostname or IP address of the InfluxDB server.
    /// </summary>
    public string Host
    {
        get => _host;
        set => _host = string.IsNullOrEmpty(value) ? value : value.EndsWith("/") ? value : $"{value}/";
    }

    /// <summary>
    /// The authentication token for accessing the InfluxDB server.
    /// </summary>
    public string? Token { get; set; }

    /// <summary>
    /// The organization to be used for operations.
    /// </summary>
    public string? Org { get; set; }

    /// <summary>
    /// The database to be used for InfluxDB operations.
    /// </summary>
    public string? Database { get; set; }

    /// <summary>
    /// The default precision to use for the timestamp of points if no precision is specified in the write API call.
    /// </summary>
    public WritePrecision? WritePrecision { get; set; }

    /// <summary>
    /// Timeout to wait before the HTTP request times out. Default to '10 seconds'.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Automatically following HTTP 3xx redirects. Default to 'false'.
    /// </summary>
    public bool AllowHttpRedirects { get; set; }

    /// <summary>
    /// Disable server SSL certificate validation. Default to 'false'.
    /// </summary>
    public bool DisableServerCertificateValidation { get; set; }

    /// <summary>
    /// Default headers to be sent with every request to FlightSQL server.
    /// </summary>
    internal Metadata? Headers { get; set; }

    internal void Validate()
    {
        if (string.IsNullOrEmpty(Host))
        {
            throw new ArgumentException("The hostname or IP address of the InfluxDB server has to be defined.");
        }
    }
}