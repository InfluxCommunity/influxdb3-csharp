using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
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
        private static readonly string[] ErrorHeaders =
            { "X-Platform-Error-Code", "X-Influx-Error", "X-InfluxDb-Error" };

        private readonly InfluxDBClientConfigs _configs;

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

            string? message = null;
            var result = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            if (!result.IsSuccessStatusCode)
            {
                var content = await result.Content.ReadAsStringAsync().ConfigureAwait(true);
                // error message in body
                if (!string.IsNullOrEmpty(content))
                {
                    using var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(content));
                    try
                    {
                        if (new DataContractJsonSerializer(typeof(ErrorBody)).ReadObject(memoryStream) is ErrorBody errorBody)
                        {
                            message = errorBody.Message;
                        }
                    }
                    catch (SerializationException se)
                    {
                        Debug.WriteLine($"Cannot parse error response as JSON: {content}. {se}");
                    }
                }

                // from header
                if (string.IsNullOrEmpty(message))
                {
                    message = result.Headers?
                        .Where(header => ErrorHeaders.Contains(header.Key, StringComparer.OrdinalIgnoreCase))
                        .Select(header => header.Value.FirstOrDefault()?.ToString())
                        .FirstOrDefault();
                }

                // whole body
                if (string.IsNullOrEmpty(message))
                {
                    message = content;
                }

                // reason
                if (string.IsNullOrEmpty(message))
                {
                    message = result.ReasonPhrase;
                }

                throw new InfluxDBApiException($"Cannot write data to InfluxDB due: {message}", result);
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
    }

    [DataContract]
    internal class ErrorBody
    {
        [DataMember(Name = "error")] public string? Message { get; set; }
    }
}