using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Internal;
using InfluxDB3.Client.Query;
using InfluxDB3.Client.Write;
using ArrowArray = Apache.Arrow.Array;

namespace InfluxDB3.Client
{
    public interface IInfluxDBClient : IDisposable
    {
        /// <summary>
        /// Query data from InfluxDB IOx using FlightSQL.
        /// </summary>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="bucket">The bucket to be used for InfluxDB operations.</param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        IAsyncEnumerable<object?[]> Query(string query, QueryType? queryType = null, string? bucket = null);

        /// <summary>
        /// Query data from InfluxDB IOx using FlightSQL.
        /// </summary>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="bucket">The bucket to be used for InfluxDB operations.</param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        IAsyncEnumerable<RecordBatch> QueryBatches(string query, QueryType? queryType = null, string? bucket = null);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="record">Specifies the record in InfluxDB Line Protocol. The <see cref="record" /> is considered as one batch unit. </param>
        /// <param name="bucket">The bucket to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WriteRecordAsync(string record, string? bucket = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="records">Specifies the records in InfluxDB Line Protocol. The <see cref="records" /> is considered as one batch unit.</param>
        /// <param name="bucket">The bucket to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WriteRecordsAsync(IEnumerable<string> records, string? bucket = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="point">Specifies the Data point to write into InfluxDB. The <see cref="point" /> is considered as one batch unit. </param>
        /// <param name="bucket">The bucket to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WritePointAsync(PointData point, string? bucket = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="points">Specifies the Data points to write into InfluxDB. The <see cref="points" /> is considered as one batch unit.</param>
        /// <param name="bucket">The bucket to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WritePointsAsync(IEnumerable<PointData> points, string? bucket = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default);
    }

    public class InfluxDBClient : IInfluxDBClient
    {
        private bool _disposed;

        private readonly ClientConfig _config;
        private readonly HttpClient _httpClient;
        private readonly FlightSqlClient _flightSqlClient;
        private readonly RestClient _restClient;
        private readonly GzipHandler _gzipHandler;

        /// <summary>
        /// This class provides an interface for interacting with an InfluxDB server,
        /// simplifying common operations such as writing, querying.
        /// </summary>
        /// <param name="host">The URL of the InfluxDB server.</param>
        /// <param name="token">The authentication token for accessing the InfluxDB server.</param>
        /// <param name="organization">The organization name to be used for operations.</param>
        /// <param name="bucket">The bucket to be used for InfluxDB operations.</param>
        public InfluxDBClient(string host, string? token = null, string? organization = null,
            string? bucket = null) : this(
            new ClientConfig
            {
                Host = host,
                Organization = organization,
                Bucket = bucket,
                Token = token,
                WriteOptions = WriteOptions.DefaultOptions
            })
        {
        }

        /// <summary>
        /// This class provides an interface for interacting with an InfluxDB server,
        /// simplifying common operations such as writing, querying.
        /// </summary>
        /// <param name="config">The configuration of the client.</param>
        public InfluxDBClient(ClientConfig config)
        {
            if (config is null)
            {
                throw new ArgumentException("The configuration of the client has to be defined.");
            }

            config.Validate();

            _config = config;
            _httpClient = CreateAndConfigureHttpClient(_config);
            _flightSqlClient = new FlightSqlClient(config: _config, httpClient: _httpClient);
            _restClient = new RestClient(config: _config, httpClient: _httpClient);
            _gzipHandler = new GzipHandler(config.WriteOptions != null ? config.WriteOptions.GzipThreshold : 0);
        }

        /// <summary>
        /// Query data from InfluxDB IOx using FlightSQL.
        /// </summary>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="bucket">The bucket to be used for InfluxDB operations.</param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        public async IAsyncEnumerable<object?[]> Query(string query, QueryType? queryType = null,
            string? bucket = null)
        {
            await foreach (var batch in QueryBatches(query, queryType, bucket).ConfigureAwait(false))
            {
                var rowCount = batch.Column(0).Length;
                for (var i = 0; i < rowCount; i++)
                {
                    var row = new List<object?>();
                    for (var j = 0; j < batch.ColumnCount; j++)
                    {
                        if (batch.Column(j) is ArrowArray array)
                        {
                            row.Add(array.GetObjectValue(i));
                        }
                    }

                    yield return row.ToArray();
                }
            }
        }

        /// <summary>
        /// Query data from InfluxDB IOx using FlightSQL.
        /// </summary>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="bucket">The bucket to be used for InfluxDB operations.</param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        public IAsyncEnumerable<RecordBatch> QueryBatches(string query, QueryType? queryType = null,
            string? bucket = null)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(InfluxDBClient));
            }

            return _flightSqlClient.Execute(query,
                (bucket ?? _config.Bucket) ?? throw new InvalidOperationException(OptionMessage("bucket")),
                queryType ?? QueryType.SQL);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="record">Specifies the record in InfluxDB Line Protocol. The <see cref="record" /> is considered as one batch unit.</param>
        /// <param name="bucket">The bucket to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WriteRecordAsync(string record, string? bucket = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            return WriteRecordsAsync(new[] { record }, bucket, precision, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="records">Specifies the records in InfluxDB Line Protocol. The <see cref="records" /> is considered as one batch unit.</param>
        /// <param name="bucket">The bucket to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WriteRecordsAsync(IEnumerable<string> records, string? bucket = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            return WriteData(records, bucket, precision, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="point">Specifies the Data point to write into InfluxDB. The <see cref="point" /> is considered as one batch unit. </param>
        /// <param name="bucket">The bucket to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WritePointAsync(PointData point, string? bucket = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            return WritePointsAsync(new[] { point }, bucket, precision, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="points">Specifies the Data points to write into InfluxDB. The <see cref="points" /> is considered as one batch unit.</param>
        /// <param name="bucket">The bucket to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WritePointsAsync(IEnumerable<PointData> points, string? bucket = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            return WriteData(points, bucket, precision, cancellationToken);
        }

        private async Task WriteData(IEnumerable<object> data, string? bucket = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(InfluxDBClient));
            }

            var precisionNotNull = precision ?? _config.WritePrecision;
            var sb = ToLineProtocolBody(data, precisionNotNull);
            if (sb.Length == 0)
            {
                Trace.WriteLine($"The writes: {data} doesn't contains any Line Protocol, skipping");
                return;
            }

            var body = sb.ToString();
            var content = _gzipHandler.Process(body) ?? new StringContent(body, Encoding.UTF8, "text/plain");
            var queryParams = new Dictionary<string, string?>()
            {
                {
                    "bucket",
                    (bucket ?? _config.Bucket) ?? throw new InvalidOperationException(OptionMessage("bucket"))
                },
                { "org", _config.Organization },
                {
                    "precision",
                    Enum.GetName(typeof(WritePrecision), precisionNotNull)
                        ?.ToLowerInvariant()
                }
            };

            await _restClient
                .Request("api/v2/write", HttpMethod.Post, content, queryParams, cancellationToken)
                .ConfigureAwait(false);
        }

        public void Dispose()
        {
            _httpClient.Dispose();
            _flightSqlClient.Dispose();
            _disposed = true;
        }

        private static StringBuilder ToLineProtocolBody(IEnumerable<object?> data, WritePrecision precision)
        {
            var sb = new StringBuilder("");

            foreach (var item in data)
            {
                var lineProtocol = item switch
                {
                    PointData pointData => pointData.ToLineProtocol(precision),
                    _ => item?.ToString()
                };

                if (string.IsNullOrEmpty(lineProtocol))
                {
                    continue;
                }

                sb.Append(lineProtocol);
                sb.Append("\n");
            }

            if (sb.Length != 0)
            {
                // remove last \n
                sb.Remove(sb.Length - 1, 1);
            }

            return sb;
        }

        internal static HttpClient CreateAndConfigureHttpClient(ClientConfig config)
        {
            var handler = new HttpClientHandler();
            if (handler.SupportsRedirectConfiguration)
            {
                handler.AllowAutoRedirect = config.AllowHttpRedirects;
            }
            if (handler.SupportsAutomaticDecompression)
            {
                handler.AutomaticDecompression = System.Net.DecompressionMethods.GZip | System.Net.DecompressionMethods.Deflate;
            }
            if (handler.SupportsProxy && config.Proxy != null)
            {
                handler.Proxy = config.Proxy;
            }
            if (config.DisableServerCertificateValidation)
            {
                handler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
            }

            var client = new HttpClient(handler)
            {
                Timeout = config.Timeout
            };
            if (config.Headers != null)
            {
                foreach (var header in config.Headers)
                {
                    client.DefaultRequestHeaders.Add(header.Key, header.Value);
                }
            }
            client.DefaultRequestHeaders.UserAgent.ParseAdd($"influxdb3-csharp/{AssemblyHelper.GetVersion()}");
            if (!string.IsNullOrEmpty(config.Token))
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", config.Token);
            }

            return client;
        }

        private static string OptionMessage(string property)
        {
            return $"Please specify the '{property}' as a method parameter or use default configuration " +
                   $"at 'InfluxDBClientConfigs.{property[0].ToString().ToUpper()}{property.Substring(1)}'.";
        }
    }
}