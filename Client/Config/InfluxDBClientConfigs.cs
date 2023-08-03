using System;
using System.Collections.Generic;
using System.Net;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Client.Config;

/// <summary>
/// The InfluxDBClientConfigs class holds the configuration for the InfluxDB client.
///
/// You can configure following options:
/// - HostUrl: The hostname or IP address of the InfluxDB server.
/// - AuthToken: The authentication token for accessing the InfluxDB server.
/// - Organization: The organization to be used for operations.
/// - Database: The database to be used for InfluxDB operations.
/// - Headers: The set of HTTP headers to be included in requests.
/// - Timeout: Timeout to wait before the HTTP request times out. Default to '10 seconds'.
/// - AllowHttpRedirects: Automatically following HTTP 3xx redirects. Default to 'false'.
/// - DisableServerCertificateValidation: Disable server SSL certificate validation. Default to 'false'.
/// - Proxy: The HTTP proxy URL. Default is not set.
/// - WriteOptions: Write options.
///
/// If you want create client with custom options, you can use the following code:
/// <code>
/// using var client = new InfluxDBClient(new InfluxDBClientConfigs{
///     HostUrl = "https://us-east-1-1.aws.cloud2.influxdata.com",
///     Organization = "my-org",
///     Database = "my-database",
///     AllowHttpRedirects = true,
///     DisableServerCertificateValidation = true,
///     WriteOptions = new WriteOptions
///    {
///        Precision = WritePrecision.S,
///        GzipThreshold = 4096
///    }
/// }); 
/// </code>
/// </summary>

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
    public Dictionary<String, String>? Headers { get; set; }

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
    /// The HTTP proxy URL. Default is not set.
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