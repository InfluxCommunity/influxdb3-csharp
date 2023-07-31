using System;
using System.Collections.Generic;
using System.Net;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Client.Config;

public class InfluxDBClientConfigs
{
    private string _hostUrl = "";

    /// <summary>
    /// The configuration of the client.
    /// </summary>
    public InfluxDBClientConfigs()
    {
    }

    /// <summary>
    /// The hostname or IP address of the InfluxDB server.
    /// </summary>
    public string HostUrl
    {
        get => _hostUrl;
        set => _hostUrl = string.IsNullOrEmpty(value) ? value : value.EndsWith("/") ? value : $"{value}/";
    }

    /// <summary>
    /// The authentication token for accessing the InfluxDB server.
    /// </summary>
    public string? AuthToken { get; set; }

    /// <summary>
    /// The organization to be used for operations.
    /// </summary>
    public string? Organization { get; set; }

    /// <summary>
    /// The database to be used for InfluxDB operations.
    /// </summary>
    public string? Database { get; set; }

    /// <summary>
    /// The set of HTTP headers to be included in requests.
    /// </summary>
    public List<KeyValuePair<String, String>>? Headers { get; set; }

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
    /// The web proxy for HTTP communication.
    /// </summary>
    public WebProxy? Proxy { get; set; }

    /// <summary>
    /// Write options.
    /// </summary>
    public WriteOptions? WriteOptions { get; set; }

    internal void Validate()
    {
        if (string.IsNullOrEmpty(HostUrl))
        {
            throw new ArgumentException("The hostname or IP address of the InfluxDB server has to be defined.");
        }
    }

    internal WritePrecision WritePrecision
    {
        get => WriteOptions != null ? WriteOptions.Precision ?? WritePrecision.Ns : WritePrecision.Ns;
    }
}