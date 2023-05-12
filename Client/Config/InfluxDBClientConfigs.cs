using System;
using Grpc.Core;

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
        set => _host = value.EndsWith("/") ? value : $"{value}/";
    }

    /// <summary>
    /// The authentication token for accessing the InfluxDB server.
    /// </summary>
    public string? Token { get; set; }

    /// <summary>
    /// The database to be used for InfluxDB operations.
    /// </summary>
    public string? Database { get; set; }

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
    public bool DisableServerCertificateValidation { get; set; } = false;

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

        if (string.IsNullOrEmpty(Database))
        {
            throw new ArgumentException("The database to be used for InfluxDB operations has to be defined.");
        }
    }
}