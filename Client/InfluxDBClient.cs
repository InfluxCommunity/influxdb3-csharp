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
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        IAsyncEnumerable<object?[]> Query(string query, QueryType? queryType = null, string? database = null);

        /// <summary>
        /// Query data from InfluxDB IOx using FlightSQL.
        /// </summary>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        IAsyncEnumerable<RecordBatch> QueryBatches(string query, QueryType? queryType = null, string? database = null);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="record">Specifies the record in InfluxDB Line Protocol. The <see cref="record" /> is considered as one batch unit. </param>
        /// <param name="organization">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WriteRecordAsync(string record, string? organization = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="records">Specifies the records in InfluxDB Line Protocol. The <see cref="records" /> is considered as one batch unit.</param>
        /// <param name="organization">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WriteRecordsAsync(IEnumerable<string> records, string? organization = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="point">Specifies the Data point to write into InfluxDB. The <see cref="point" /> is considered as one batch unit. </param>
        /// <param name="organization">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WritePointAsync(PointData point, string? organization = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="points">Specifies the Data points to write into InfluxDB. The <see cref="points" /> is considered as one batch unit.</param>
        /// <param name="organization">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WritePointsAsync(IEnumerable<PointData> points, string? organization = null, string? database = null,
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
        /// <param name="hostUrl">The hostname or IP address of the InfluxDB server.</param>
        /// <param name="authToken">The authentication token for accessing the InfluxDB server.</param>
        /// <param name="organization">The organization name to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        public InfluxDBClient(string hostUrl, string? authToken = null, string? organization = null,
            string? database = null) : this(
            new InfluxDBClientConfigs
            {
                HostUrl = hostUrl,
                Organization = organization,
                Database = database,
                AuthToken = authToken,
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
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        public async IAsyncEnumerable<object?[]> Query(string query, QueryType? queryType = null,
            string? database = null)
        {
            await foreach (var batch in QueryBatches(query, queryType, database).ConfigureAwait(false))
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
        /// Query data from InfluxDB IOx into PointData structure using FlightSQL.
        /// </summary>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        public async IAsyncEnumerable<PointData> QueryPoints(string query, QueryType? queryType = null,
            string? database = null)
        {
            await foreach (var batch in QueryBatches(query, queryType, database).ConfigureAwait(false))
            {
                var rowCount = batch.Column(0).Length;
                for (var i = 0; i < rowCount; i++)
                {
                    // TODO: measurement
                    var point = PointData.Measurement("__empty__");
                    for (var j = 0; j < batch.ColumnCount; j++)
                    {
                        var schema = batch.Schema.FieldsList[j];
                        var fullName = schema.Name;

                        if (batch.Column(j) is not ArrowArray array)
                            continue;

                        var objectValue = array.GetObjectValue(i);
                        if (objectValue is null)
                            continue;

                        if ((fullName == "measurement" || fullName == "iox::measurement") && objectValue is string)
                        {
                            point = point.SetMeasurement((string)objectValue);
                            continue;
                        }

                        if (!schema.HasMetadata)
                        {
                            if (fullName == "time" && objectValue is DateTimeOffset timestamp)
                            {
                                point = point.SetTimestamp(timestamp);
                            }
                            else
                                // just push as field If you don't know what type is it
                                point = point.AddField(fullName, objectValue);

                            continue;
                        }

                        string type = schema.Metadata["iox::column::type"];
                        string[] parts = type.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                        string valueType = parts[2];
                        // string fieldType = parts.Length > 3 ? parts[3] : "";

                        if (valueType == "field")
                        {
                            point = point.AddField(fullName, objectValue);
                        }
                        else if (valueType == "tag")
                        {
                            point = point.AddTag(fullName, (string)objectValue);
                        }
                        else if (valueType == "timestamp" && objectValue is DateTimeOffset timestamp)
                        {
                            point = point.SetTimestamp(timestamp);
                        }

                    }

                    yield return point;
                }
            }
        }

        /// <summary>
        /// Query data from InfluxDB IOx using FlightSQL.
        /// </summary>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        public IAsyncEnumerable<RecordBatch> QueryBatches(string query, QueryType? queryType = null,
            string? database = null)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(InfluxDBClient));
            }

            return _flightSqlClient.Execute(query,
                (database ?? _configs.Database) ?? throw new InvalidOperationException(OptionMessage("database")),
                queryType ?? QueryType.SQL);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="record">Specifies the record in InfluxDB Line Protocol. The <see cref="record" /> is considered as one batch unit.</param>
        /// <param name="organization">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WriteRecordAsync(string record, string? organization = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            return WriteRecordsAsync(new[] { record }, organization, database, precision, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="records">Specifies the records in InfluxDB Line Protocol. The <see cref="records" /> is considered as one batch unit.</param>
        /// <param name="organization">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WriteRecordsAsync(IEnumerable<string> records, string? organization = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            return WriteData(records, organization, database, precision, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="point">Specifies the Data point to write into InfluxDB. The <see cref="point" /> is considered as one batch unit. </param>
        /// <param name="organization">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WritePointAsync(PointData point, string? organization = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            return WritePointsAsync(new[] { point }, organization, database, precision, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        /// <param name="points">Specifies the Data points to write into InfluxDB. The <see cref="points" /> is considered as one batch unit.</param>
        /// <param name="organization">The organization to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WritePointsAsync(IEnumerable<PointData> points, string? organization = null,
            string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            return WriteData(points, organization, database, precision, cancellationToken);
        }

        private async Task WriteData(IEnumerable<object> data, string? organization = null, string? database = null,
            WritePrecision? precision = null, CancellationToken cancellationToken = default)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(InfluxDBClient));
            }

            var precisionNotNull = precision ?? _configs.WritePrecision ?? WritePrecision.Ns;
            var sb = ToLineProtocolBody(data, precisionNotNull);
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
                { "org", organization ?? _configs.Organization },
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
            if (!string.IsNullOrEmpty(configs.AuthToken))
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", configs.AuthToken);
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