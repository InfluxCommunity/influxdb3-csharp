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
using InfluxDB3.Client.Write;

namespace InfluxDB3.Client
{
    public interface IInfluxDBClient : IDisposable
    {
        /// <summary>
        /// Query data from InfluxDB IOx using FlightSQL.
        /// </summary>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        IAsyncEnumerable<RecordBatch> Query(string query, string? database = null);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="record">Specifies the record in InfluxDB Line Protocol. The <see cref="record" /> is considered as one batch unit. </param>
        /// <param name="org">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WriteRecordAsync(string record, string? org = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="records">Specifies the records in InfluxDB Line Protocol. The <see cref="records" /> is considered as one batch unit.</param>
        /// <param name="org">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WriteRecordsAsync(IEnumerable<string> records, string? org = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="point">Specifies the Data point to write into InfluxDB. The <see cref="point" /> is considered as one batch unit. </param>
        /// <param name="org">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WritePointAsync(PointData point, string? org = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="points">Specifies the Data points to write into InfluxDB. The <see cref="points" /> is considered as one batch unit.</param>
        /// <param name="org">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WritePointsAsync(IEnumerable<PointData> points, string? org = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default);
    }

    public class InfluxDBClient : IInfluxDBClient
    {
        private bool _disposed;

        private readonly InfluxDBClientConfigs _configs;
        private readonly HttpClient _httpClient;
        private readonly FlightSqlClient _flightSqlClient;
        private readonly RestClient _restClient;

        /// <summary>
        /// This class provides an interface for interacting with an InfluxDB server,
        /// simplifying common operations such as writing, querying.
        /// </summary>
        /// <param name="host">The hostname or IP address of the InfluxDB server.</param>
        /// <param name="token">The authentication token for accessing the InfluxDB server.</param>
        /// <param name="org">The organization name to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        public InfluxDBClient(string host, string? token = null, string? org = null, string? database = null) : this(
            new InfluxDBClientConfigs
            {
                Host = host,
                Org = org,
                Database = database,
                Token = token,
            })
        {
        }

        /// <summary>
        /// This class provides an interface for interacting with an InfluxDB server,
        /// simplifying common operations such as writing, querying.
        /// </summary>
        /// <param name="configs">The configuration of the client.</param>
        public InfluxDBClient(InfluxDBClientConfigs configs)
        {
            if (configs is null)
            {
                throw new ArgumentException("The configuration of the client has to be defined.");
            }

            configs.Validate();

            _configs = configs;
            _httpClient = CreateAndConfigureHttpClient(_configs);
            _flightSqlClient = new FlightSqlClient(configs: _configs, httpClient: _httpClient);
            _restClient = new RestClient(configs: _configs, httpClient: _httpClient);
        }

        /// <summary>
        /// Query data from InfluxDB IOx using FlightSQL.
        /// </summary>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        public IAsyncEnumerable<RecordBatch> Query(string query, string? database = null)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(InfluxDBClient));
            }

            return _flightSqlClient.Execute(query,
                (database ?? _configs.Database) ?? throw new InvalidOperationException(OptionMessage("database")));
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="record">Specifies the record in InfluxDB Line Protocol. The <see cref="record" /> is considered as one batch unit.</param>
        /// <param name="org">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WriteRecordAsync(string record, string? org = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            return WriteRecordsAsync(new[] { record }, org, database, precision, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="records">Specifies the records in InfluxDB Line Protocol. The <see cref="records" /> is considered as one batch unit.</param>
        /// <param name="org">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WriteRecordsAsync(IEnumerable<string> records, string? org = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            return WriteData(records, org, database, precision, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="point">Specifies the Data point to write into InfluxDB. The <see cref="point" /> is considered as one batch unit. </param>
        /// <param name="org">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WritePointAsync(PointData point, string? org = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            return WritePointsAsync(new[] { point }, org, database, precision, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="points">Specifies the Data points to write into InfluxDB. The <see cref="points" /> is considered as one batch unit.</param>
        /// <param name="org">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WritePointsAsync(IEnumerable<PointData> points, string? org = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            return WriteData(points, org, database, precision, cancellationToken);
        }

        private async Task WriteData(IEnumerable<object> data, string? org = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(InfluxDBClient));
            }

            var sb = ToLineProtocolBody(data);
            if (sb.Length == 0)
            {
                Trace.WriteLine($"The writes: {data} doesn't contains any Line Protocol, skipping");
                return;
            }

            var content = new StringContent(sb.ToString(), Encoding.UTF8, "text/plain");
            var queryParams = new Dictionary<string, string?>()
            {
                {
                    "bucket",
                    (database ?? _configs.Database) ?? throw new InvalidOperationException(OptionMessage("database"))
                },
                { "org", (org ?? _configs.Org) ?? throw new InvalidOperationException(OptionMessage("org")) },
                {
                    "precision",
                    Enum.GetName(typeof(WritePrecision), (precision ?? _configs.WritePrecision) ?? WritePrecision.Ns)
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

        private static StringBuilder ToLineProtocolBody(IEnumerable<object> data)
        {
            var sb = new StringBuilder("");

            foreach (var item in data)
            {
                var lineProtocol = item switch
                {
                    PointData pointData => pointData.ToLineProtocol(),
                    string str => str,
                    _ => data.ToString()
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

        internal static HttpClient CreateAndConfigureHttpClient(InfluxDBClientConfigs configs)
        {
            var handler = new HttpClientHandler
            {
                AllowAutoRedirect = configs.AllowHttpRedirects
            };

            if (configs.DisableServerCertificateValidation)
            {
                handler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
            }

            var client = new HttpClient(handler);

            client.Timeout = configs.Timeout;
            client.DefaultRequestHeaders.UserAgent.ParseAdd($"influxdb3-csharp/{AssemblyHelper.GetVersion()}");
            if (!string.IsNullOrEmpty(configs.Token))
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", configs.Token);
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