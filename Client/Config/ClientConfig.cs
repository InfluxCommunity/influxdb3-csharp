using System;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Web;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Client.Config;

/// <summary>
/// <para>The ClientConfig class holds the configuration for the InfluxDB client.</para>
///
/// <para>You can configure following options:</para>
/// <list type="bulleted">
/// <item>- Host: The URL of the InfluxDB server.</item>
/// <item>- Token: The authentication token for accessing the InfluxDB server.</item>
/// <item>- AuthScheme: Token authentication scheme. Default is 'null' for Cloud access. Set to 'Bearer' for Edge access.</item>
/// <item>- Organization: The organization to be used for operations.</item>
/// <item>- Database: The database to be used for InfluxDB operations.</item>
/// <item>- Headers: The set of HTTP headers to be included in requests.</item>
/// <item>- [Deprecated] Timeout: Timeout to wait before the HTTP request times out. Default to '10 seconds'.</item>
/// <item>- QueryTimeout: The maximum duration to wait for a query to complete before timing out.</item>
/// <item>- WriteTimeout: The duration to wait before timing out a write operation to the InfluxDB server.</item>
/// <item>- AllowHttpRedirects: Automatically following HTTP 3xx redirects. Default to 'false'.</item>
/// <item>- DisableServerCertificateValidation: Disable server SSL certificate validation. Default to 'false'.</item>
/// <item>- DisableCertificateRevocationListCheck: Disable SSL certificate revocation list (CRL) checking. Default to 'false'.</item>
/// <item>- SslRootsFilePath: SSL root certificates file path.</item>
/// <item>- Proxy: The HTTP proxy URL. Default is not set.</item>
/// <item>- WriteOptions: Write options.</item>
/// <item>- QueryOptions Query options.</item>
/// <item>- HttpClient: The HttpClient will be used for Write and Query apis.</item>
/// </list>
///
/// <para>If you want create client with custom options, you can use the following code:</para>
/// <code>
/// using var client = new InfluxDBClient(new ClientConfig{
///     Host = "https://us-east-1-1.aws.cloud2.influxdata.com",
///     Token = "my-token",
///     Organization = "my-org",
///     Database = "my-database",
///     AllowHttpRedirects = true,
///     DisableServerCertificateValidation = true,
///     WriteOptions = new WriteOptions
///    {
///        Precision = WritePrecision.S,
///        GzipThreshold = 4096,
///        NoSync = false
///    },
///    QueryOptions = new QueryOptions
///    {
///        Deadline = DateTime.UtcNow.AddSeconds(10),
///        MaxReceiveMessageSize = 4096,
///        MaxSendMessageSize = 4096,
///        CompressionProviders = new List&lt;ICompressionProvider&gt;
///        {
///            Grpc.Net.Compression.GzipCompressionProvider.Default
///        }
///    }
/// }); 
/// </code>
/// </summary>
public class ClientConfig
{
    internal const string EnvInfluxHost = "INFLUX_HOST";
    internal const string EnvInfluxToken = "INFLUX_TOKEN";
    internal const string EnvInfluxAuthScheme = "INFLUX_AUTH_SCHEME";
    internal const string EnvInfluxOrg = "INFLUX_ORG";
    internal const string EnvInfluxDatabase = "INFLUX_DATABASE";
    internal const string EnvInfluxPrecision = "INFLUX_PRECISION";
    internal const string EnvInfluxGzipThreshold = "INFLUX_GZIP_THRESHOLD";
    internal const string EnvInfluxWriteNoSync = "INFLUX_WRITE_NO_SYNC";
    internal const string EnvInfluxDisableGrpcCompression = "INFLUX_DISABLE_GRPC_COMPRESSION";

    private string _host = "";

    /// <summary>
    /// Initializes a new instance of client configuration.
    /// </summary>
    public ClientConfig()
    {
        QueryOptions = (QueryOptions)QueryOptions.DefaultOptions.Clone();
    }

    /// <summary>
    /// Initializes a new instance of client configuration from connection string.
    /// </summary>
    public ClientConfig(string connectionString)
    {
        var uri = new Uri(connectionString);
        Host = uri.GetLeftPart(UriPartial.Path);
        var values = HttpUtility.ParseQueryString(uri.Query);
        Token = values.Get("token");
        AuthScheme = values.Get("authScheme");
        Organization = values.Get("org");
        Database = values.Get("database");
        QueryOptions = (QueryOptions)QueryOptions.DefaultOptions.Clone();
        ParsePrecision(values.Get("precision"));
        ParseGzipThreshold(values.Get("gzipThreshold"));
        ParseWriteNoSync(values.Get("writeNoSync"));
        ParseDisableGrpcCompression(values.Get("disableGrpcCompression"));
    }

    /// <summary>
    /// Initializes a new instance of client configuration from environment variables.
    /// </summary>
    public ClientConfig(IDictionary env)
    {
        Host = (string)env[EnvInfluxHost];
        Token = env[EnvInfluxToken] as string;
        AuthScheme = env[EnvInfluxAuthScheme] as string;
        Organization = env[EnvInfluxOrg] as string;
        Database = env[EnvInfluxDatabase] as string;
        QueryOptions = (QueryOptions)QueryOptions.DefaultOptions.Clone();
        ParsePrecision(env[EnvInfluxPrecision] as string);
        ParseGzipThreshold(env[EnvInfluxGzipThreshold] as string);
        ParseWriteNoSync(env[EnvInfluxWriteNoSync] as string);
        ParseDisableGrpcCompression(env[EnvInfluxDisableGrpcCompression] as string);
    }

    /// <summary>
    /// The URL of the InfluxDB server.
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
    /// Token authentication scheme.
    /// </summary>
    public string? AuthScheme { get; set; }

    /// <summary>
    /// The organization to be used for operations.
    /// </summary>
    public string? Organization { get; set; }

    /// <summary>
    /// The database to be used for InfluxDB operations.
    /// </summary>
    public string? Database { get; set; }

    /// <summary>
    /// The custom headers that will be added to requests. This is useful for adding custom headers to requests,
    /// such as tracing headers. To add custom headers use following code:
    ///
    /// <code>
    /// using var client = new InfluxDBClient(new ClientConfig
    /// {
    ///     Host = "https://us-east-1-1.aws.cloud2.influxdata.com",
    ///     Token = "my-token",
    ///     Organization = "my-org",
    ///     Database = "my-database",
    ///     Headers = new Dictionary&lt;string, string&gt;
    ///     {
    ///         { "X-Tracing-Id", "123" },
    ///     }
    /// });
    /// </code>
    /// </summary>
    public Dictionary<string, string>? Headers { get; set; }

    /// <summary>
    /// Timeout to wait before the HTTP request times out. Default to '10 seconds'.
    /// </summary>
    [Obsolete("Please use more informative properties like WriteTimeout or QueryTimeout")]
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// The maximum duration to wait for a query to complete before timing out.
    /// </summary>
    public TimeSpan? QueryTimeout { get; set; }

    /// <summary>
    /// The duration to wait before timing out a write operation to the InfluxDB server.
    /// </summary>
    public TimeSpan? WriteTimeout { get; set; }

    /// <summary>
    /// Automatically following HTTP 3xx redirects. Default to 'false'.
    /// </summary>
    public bool AllowHttpRedirects { get; set; }

    /// <summary>
    /// Disable server SSL certificate validation. Default to 'false'.
    /// </summary>
    public bool DisableServerCertificateValidation { get; set; }

    /// <summary>
    /// Disable SSL certificate revocation list (CRL) checking. Default to 'false'.
    /// </summary>
    public bool DisableCertificateRevocationListCheck { get; set; }

    /// <summary>
    /// SSL root certificates file path.
    /// </summary>
    public string? SslRootsFilePath { get; set; }

    /// <summary>
    /// The HTTP proxy URL. Default is not set.
    /// </summary>
    public WebProxy? Proxy { get; set; }

    /// <summary>
    /// Write options.
    /// </summary>
    public WriteOptions? WriteOptions { get; set; }

    /// <summary>
    /// Configuration options for query behavior in the InfluxDB client.
    /// </summary>
    public QueryOptions QueryOptions { get; set; }

    /// <summary>
    /// User-defined HttpClient.
    /// Influxdb client will add an authentication header and base url to HttpClient. The rest is up to the users.
    /// Users will be responsible for closing the HttpClient.
    /// </summary>
    public HttpClient? HttpClient { get; set; }

    internal void Validate()
    {
        if (string.IsNullOrEmpty(Host))
        {
            throw new ArgumentException("The URL of the InfluxDB server has to be defined");
        }
    }

    internal WritePrecision WritePrecision
    {
        get => WriteOptions != null ? WriteOptions.Precision ?? WritePrecision.Ns : WritePrecision.Ns;
    }

    internal bool WriteNoSync
    {
        get => (WriteOptions ?? WriteOptions.DefaultOptions).NoSync;
    }

    private void ParsePrecision(string? precision)
    {
        if (precision != null)
        {
            var writePrecision = precision switch
            {
                "ns" => WritePrecision.Ns,
                "nanosecond" => WritePrecision.Ns,
                "us" => WritePrecision.Us,
                "microsecond" => WritePrecision.Us,
                "ms" => WritePrecision.Ms,
                "millisecond" => WritePrecision.Ms,
                "s" => WritePrecision.S,
                "second" => WritePrecision.S,
                _ => throw new ArgumentException($"Unsupported precision '{precision}'"),
            };
            WriteOptions ??= (WriteOptions)WriteOptions.DefaultOptions.Clone();
            WriteOptions.Precision = writePrecision;
        }
    }

    private void ParseGzipThreshold(string? threshold)
    {
        if (threshold != null)
        {
            var gzipThreshold = int.Parse(threshold);
            WriteOptions ??= (WriteOptions)WriteOptions.DefaultOptions.Clone();
            WriteOptions.GzipThreshold = gzipThreshold;
        }
    }

    private void ParseWriteNoSync(string? strVal)
    {
        if (strVal != null)
        {
            var noSync = bool.Parse(strVal);
            WriteOptions ??= (WriteOptions)WriteOptions.DefaultOptions.Clone();
            WriteOptions.NoSync = noSync;
        }
    }

    private void ParseDisableGrpcCompression(string? strVal)
    {
        if (strVal != null)
        {
            var disable = bool.Parse(strVal);
            QueryOptions.DisableGrpcCompression = disable;
        }
    }
}