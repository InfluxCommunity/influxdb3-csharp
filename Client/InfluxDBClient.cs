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
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        IAsyncEnumerable<RecordBatch> Query(string query);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="record">Specifies the record in InfluxDB Line Protocol. The <see cref="record" /> is considered as one batch unit. </param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests</param>
        Task WriteRecordAsync(string record, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="records">Specifies the records in InfluxDB Line Protocol. The <see cref="records" /> is considered as one batch unit.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests</param>
        Task WriteRecordsAsync(IEnumerable<string> records, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="point">Specifies the Data point to write into InfluxDB. The <see cref="point" /> is considered as one batch unit. </param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests</param>
        Task WritePointAsync(PointData point, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="points">Specifies the Data points to write into InfluxDB. The <see cref="points" /> is considered as one batch unit.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests</param>
        Task WritePointsAsync(IEnumerable<PointData> points, CancellationToken cancellationToken = default);
    }

    public class InfluxDBClient : IInfluxDBClient
    {
        private InfluxDBClientConfigs _configs;
        private bool _disposed;

        private readonly FlightSqlClient _flightSqlClient;
        private readonly HttpClient _httpClient;

        /// <summary>
        /// This class provides an interface for interacting with an InfluxDB server,
        /// simplifying common operations such as writing, querying.
        /// </summary>
        /// <param name="host">The hostname or IP address of the InfluxDB server.</param>
        /// <param name="token">The authentication token for accessing the InfluxDB server.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        public InfluxDBClient(string host, string? token = null, string? database = null) : this(
            new InfluxDBClientConfigs(host)
            {
                Token = token,
                Database = database
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
            _configs = configs ??
                       throw new ArgumentException("The configuration of the client has to be defined.");

            _flightSqlClient = new FlightSqlClient(host: _configs.Host);
            _httpClient = new HttpClient(new HttpClientHandler
            {
                AllowAutoRedirect = _configs.AllowHttpRedirects
            });

            _httpClient.Timeout = _configs.Timeout;
            _httpClient.DefaultRequestHeaders.UserAgent.ParseAdd($"influxdb3-csharp/{AssemblyHelper.GetVersion()}");
            if (!string.IsNullOrEmpty(configs.Token))
            {
                _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", configs.Token);
            }
        }

        /// <summary>
        /// Query data from InfluxDB IOx using FlightSQL.
        /// </summary>
        /// <param name="query">The SQL query string to execute.</param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        public IAsyncEnumerable<RecordBatch> Query(string query)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(FlightSqlClient));
            }

            return _flightSqlClient.Execute(query);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="record">Specifies the record in InfluxDB Line Protocol. The <see cref="record" /> is considered as one batch unit.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests</param>
        public Task WriteRecordAsync(string record, CancellationToken cancellationToken = default)
        {
            return WriteRecordsAsync(new[] { record }, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="records">Specifies the records in InfluxDB Line Protocol. The <see cref="records" /> is considered as one batch unit.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests</param>
        public Task WriteRecordsAsync(IEnumerable<string> records, CancellationToken cancellationToken = default)
        {
            return WriteData(records, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="point">Specifies the Data point to write into InfluxDB. The <see cref="point" /> is considered as one batch unit. </param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests</param>
        public Task WritePointAsync(PointData point, CancellationToken cancellationToken = default)
        {
            return WritePointsAsync(new[] { point }, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="points">Specifies the Data points to write into InfluxDB. The <see cref="points" /> is considered as one batch unit.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests</param>
        public Task WritePointsAsync(IEnumerable<PointData> points, CancellationToken cancellationToken = default)
        {
            return WriteData(points, cancellationToken);
        }

        private async Task WriteData(IEnumerable<object> data, CancellationToken cancellationToken)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(FlightSqlClient));
            }

            var sb = ToLineProtocolBody(data);
            if (sb.Length == 0)
            {
                Trace.WriteLine($"The writes: {data} doesn't contains any Line Protocol, skipping");
                return;
            }

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                RequestUri = new Uri($"{_configs.Host}api/v2/write"),
                Content = new StringContent(sb.ToString(), Encoding.UTF8, "text/plain")
            };

            request.Headers.Add("Accept", "application/json");

            var result = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            if (!result.IsSuccessStatusCode)
            {
                // TODO detail message test
                throw new InfluxDBApiException("Cannot write date due.", result);
            }
        }

        public void Dispose()
        {
            _flightSqlClient.Dispose();
            _httpClient.Dispose();
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
    }
}