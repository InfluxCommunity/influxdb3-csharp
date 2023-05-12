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
        private bool _disposed;

        private readonly HttpClient _httpClient;
        private readonly FlightSqlClient _flightSqlClient;
        private readonly RestClient _restClient;

        /// <summary>
        /// This class provides an interface for interacting with an InfluxDB server,
        /// simplifying common operations such as writing, querying.
        /// </summary>
        /// <param name="host">The hostname or IP address of the InfluxDB server.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="token">The authentication token for accessing the InfluxDB server.</param>
        public InfluxDBClient(string host, string database, string? token = null) : this(
            new InfluxDBClientConfigs
            {
                Host = host,
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
            if (configs is null)
            {
                throw new ArgumentException("The configuration of the client has to be defined.");
            }

            configs.Validate();

            _httpClient = CreateAndConfigureHttpClient(configs);
            _flightSqlClient = new FlightSqlClient(configs: configs, httpClient: _httpClient);
            _restClient = new RestClient(configs: configs, httpClient: _httpClient);
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

            var content = new StringContent(sb.ToString(), Encoding.UTF8, "text/plain");

            await _restClient.Request("api/v2/write", HttpMethod.Post, content, cancellationToken);
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
    }
}