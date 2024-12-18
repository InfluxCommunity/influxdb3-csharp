using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
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
        /// 
        /// <example>
        /// The following example shows how to use SQL query with named parameters:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        /// 
        /// var results = client.Query(
        ///     query: "SELECT a, b, c FROM my_table WHERE id = $id AND name = $name",
        ///     namedParameters: new Dictionary&lt;string, object&gt; { { "id", 1 }, { "name", "test" } }
        /// );
        /// </code>
        /// </example>
        /// 
        /// <example>
        /// The following example shows how to use custom request headers:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        /// 
        /// var results = client.Query(
        ///     query: "SELECT a, b, c FROM my_table",
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="namedParameters">
        ///     Name:Value pairs to use as named parameters for this query. Parameters referenced using
        ///     '$placeholder' syntax in the query will be substituted with the values provided.
        ///     The supported types are: string, bool, int, float.
        /// </param>
        /// <param name="headers">
        ///     The headers to be added to query request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        IAsyncEnumerable<object?[]> Query(string query, QueryType? queryType = null, string? database = null,
            Dictionary<string, object>? namedParameters = null, Dictionary<string, string>? headers = null);

        /// <summary>
        /// Query data from InfluxDB IOx using FlightSQL.
        /// </summary>
        /// 
        /// <example>
        /// The following example shows how to use SQL query with named parameters:
        /// 
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        /// 
        /// var results = client.QueryBatches(
        ///     query: "SELECT a, b, c FROM my_table WHERE id = $id AND name = $name",
        ///     namedParameters: new Dictionary&lt;string, object&gt; { { "id", 1 }, { "name", "test" } }
        /// );
        /// </code>
        /// </example>
        /// 
        /// <example>
        /// The following example shows how to use custom request headers:
        /// 
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        /// 
        /// var results = client.QueryBatches(
        ///     query: "SELECT a, b, c FROM my_table",
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="namedParameters">
        ///     Name:Value pairs to use as named parameters for this query. Parameters referenced using
        ///     '$placeholder' syntax in the query will be substituted with the values provided.
        ///     The supported types are: string, bool, int, float.
        /// </param>
        /// <param name="headers">
        ///     The headers to be added to query request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        IAsyncEnumerable<RecordBatch> QueryBatches(string query, QueryType? queryType = null, string? database = null,
            Dictionary<string, object>? namedParameters = null, Dictionary<string, string>? headers = null);

        /// <summary>
        /// Query data from InfluxDB IOx into PointData structure using FlightSQL.
        /// </summary>
        ///
        /// <example>
        /// The following example shows how to use SQL query with named parameters:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        /// 
        /// var results = client.QueryPoints(
        ///     query: "SELECT a, b, c FROM my_table WHERE id = $id AND name = $name",
        ///     namedParameters: new Dictionary&lt;string, object&gt; { { "id", 1 }, { "name", "test" } }
        /// );
        /// </code>
        /// </example>
        /// <example>
        /// The following example shows how to use custom request headers:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        /// 
        /// var results = client.QueryPoints(
        ///     query: "SELECT a, b, c FROM my_table",
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="namedParameters">
        ///     Name:Value pairs to use as named parameters for this query. Parameters referenced using
        ///     '$placeholder' syntax in the query will be substituted with the values provided.
        ///     The supported types are: string, bool, int, float.
        /// </param>
        /// <param name="headers">
        ///     The headers to be added to query request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        IAsyncEnumerable<PointDataValues> QueryPoints(string query, QueryType? queryType = null,
            string? database = null, Dictionary<string, object>? namedParameters = null,
            Dictionary<string, string>? headers = null);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        ///
        /// <example>
        /// The following example shows how to write a single record with custom headers:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        ///
        /// await client.WriteRecordAsync(
        ///     record: "stat,unit=temperature value=24.5",
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example>
        /// <param name="record">Specifies the record in InfluxDB Line Protocol. The <see cref="record" /> is considered as one batch unit. </param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="headers">
        ///     The headers to be added to write request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WriteRecordAsync(string record, string? database = null, WritePrecision? precision = null,
            Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        ///
        /// <example>
        /// The following example shows how to write multiple records with custom headers:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        ///
        /// await client.WriteRecordsAsync(
        ///     records: new[] { "stat,unit=temperature value=24.5", "stat,unit=temperature value=25.5" },
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example>
        /// <param name="records">Specifies the records in InfluxDB Line Protocol. The <see cref="records" /> is considered as one batch unit.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="headers">
        ///     The headers to be added to write request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WriteRecordsAsync(IEnumerable<string> records, string? database = null, WritePrecision? precision = null,
            Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        ///
        /// <example>
        /// The following example shows how to write a single point with custom headers:
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        ///
        /// await client.WritePointAsync(
        ///     point: PointData.Measurement("h2o").SetTag("location", "europe").SetField("level", 2),
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example>
        /// <param name="point">Specifies the Data point to write into InfluxDB. The <see cref="point" /> is considered as one batch unit. </param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="headers">
        ///     The headers to be added to write request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WritePointAsync(PointData point, string? database = null, WritePrecision? precision = null,
            Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        ///
        /// <example>
        /// The following example shows how to write multiple points with custom headers:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        ///
        /// await client.WritePointsAsync(
        ///     points: new[]{
        ///         PointData.Measurement("h2o").SetTag("location", "europe").SetField("level", 2),
        ///         PointData.Measurement("h2o").SetTag("location", "us-west").SetField("level", 4),
        ///     },
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example>
        /// <param name="points">Specifies the Data points to write into InfluxDB. The <see cref="points" /> is considered as one batch unit.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="headers">
        ///     The headers to be added to write request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        Task WritePointsAsync(IEnumerable<PointData> points, string? database = null, WritePrecision? precision = null,
            Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default);
    }

    public class InfluxDBClient : IInfluxDBClient
    {
        private bool _disposed;

        private readonly ClientConfig _config;
        private readonly HttpClient _httpClient;
        private readonly RestClient _restClient;
        private readonly GzipHandler _gzipHandler;

        /// <summary>
        /// The FlightSQLClient should be updated only by the constructor or tests.
        /// </summary>
        internal IFlightSqlClient FlightSqlClient;

        /// <summary>
        /// Initializes a new instance of the client with provided configuration options.
        /// </summary>
        /// <param name="host">The URL of the InfluxDB server.</param>
        /// <param name="token">The authentication token for accessing the InfluxDB server.</param>
        /// <param name="organization">The organization name to be used for operations.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="authScheme">Token authentications scheme.</param>
        /// <example>
        /// using var client = new InfluxDBClient(host: "https://us-east-1-1.aws.cloud2.influxdata.com", token: "my-token", organization: "my-org", database: "my-database");
        /// </example>
        public InfluxDBClient(string host, string token, string? organization = null,
            string? database = null, string? authScheme = null) : this(
            new ClientConfig
            {
                Host = host,
                Organization = organization,
                Database = database,
                Token = token,
                AuthScheme = authScheme,
                WriteOptions = WriteOptions.DefaultOptions
            })
        {
        }

        /// <summary>
        /// Initializes a new instance of the client with provided configuration.
        /// </summary>
        /// <param name="config">The configuration of the client.</param>
        /// <example>
        /// using var client = new InfluxDBClient(
        ///    new ClientConfig
        ///    {
        ///        Host = "https://us-east-1-1.aws.cloud2.influxdata.com",
        ///        Token = "my-token",
        ///        Organization = "my-org",
        ///        Database = "my-database"
        ///    }
        ///  );
        /// </example>
        public InfluxDBClient(ClientConfig config)
        {
            if (config is null)
            {
                throw new ArgumentNullException("config");
            }

            config.Validate();

            _config = config;
            _httpClient = CreateAndConfigureHttpClient(_config);
            FlightSqlClient = new FlightSqlClient(config: _config, httpClient: _httpClient);
            _restClient = new RestClient(config: _config, httpClient: _httpClient);
            _gzipHandler = new GzipHandler(config.WriteOptions != null ? config.WriteOptions.GzipThreshold : 0);
        }

        /// <summary>
        /// Initializes a new instance of the client using connection string.
        /// <para>
        /// Supported parameters are:
        /// <list type="bullet">
        /// <item>
        /// <description>token - authentication token (required)</description>
        /// </item>
        /// <item>
        /// <description>authScheme - token authentication scheme (default <c>null</c> for Cloud access)</description>
        /// </item>
        /// <item>
        /// <description>org - organization name</description>
        /// </item>
        /// <item>
        /// <description>database - database (bucket) name</description>
        /// </item>
        /// <item>
        /// <description>precision - timestamp precision when writing data (<c>ns</c> (default), <c>us</c>, <c>ms</c>, <c>s</c>)</description>
        /// </item>
        /// <item>
        /// <description>gzipThreshold - threshold for gzip data when writing (default is <c>1000</c>)</description>
        /// </item>
        /// </list>
        /// </para>
        /// </summary>
        /// <param name="connectionString">Connection string in URL format.</param>
        /// <example>
        /// using var client = new InfluxDBClient(host: "https://us-east-1-1.aws.cloud2.influxdata.com?token=my-token&amp;database=my-db");
        /// </example>
        public InfluxDBClient(string connectionString) : this(new ClientConfig(connectionString))
        {
        }

        /// <summary>
        /// Initializes a new instance of the client using connection string.
        /// <para>
        /// Supported parameters are:
        /// <list type="bullet">
        /// <item>
        /// <description>INFLUX_HOST - InfluxDB host URL (required)</description>
        /// </item>
        /// <item>
        /// <description>INFLUX_TOKEN - authentication token (required)</description>
        /// </item>
        /// <item>
        /// <description>INFLUX_AUTH_SCHEME - token authentication scheme (default is <c>null</c> for Cloud access)</description>
        /// </item>
        /// <item>
        /// <description>INFLUX_ORG - organization name</description>
        /// </item>
        /// <item>
        /// <description>INFLUX_DATABASE - database (bucket) name</description>
        /// </item>
        /// <item>
        /// <description>INFLUX_PRECISION - timestamp precision when writing data (<c>ns</c> (default), <c>us</c>, <c>ms</c>, <c>s</c>)</description>
        /// </item>
        /// <item>
        /// <description>INFLUX_GZIP_THRESHOLD - threshold for gzipping data when writing (default is <c>1000</c>)</description>
        /// </item>
        /// </list>
        /// </para>
        /// </summary>
        /// <example>
        /// using var client = new InfluxDBClient();
        /// </example>
        public InfluxDBClient() : this(
            new ClientConfig(Environment.GetEnvironmentVariables(EnvironmentVariableTarget.Process)))
        {
        }

        /// <summary>
        /// Query data from InfluxDB IOx using FlightSQL.
        /// </summary>
        /// 
        /// <example>
        /// The following example shows how to use SQL query with named parameters:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        /// 
        /// var results = client.Query(
        ///     query: "SELECT a, b, c FROM my_table WHERE id = $id AND name = $name",
        ///     namedParameters: new Dictionary&lt;string, object&gt; { { "id", 1 }, { "name", "test" } }
        /// );
        /// </code>
        /// </example>
        /// 
        /// <example>
        /// The following example shows how to use custom request headers:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        /// 
        /// var results = client.Query(
        ///     query: "SELECT a, b, c FROM my_table",
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="namedParameters">
        ///     Name:Value pairs to use as named parameters for this query. Parameters referenced using
        ///     '$placeholder' syntax in the query will be substituted with the values provided.
        ///     The supported types are: string, bool, int, float.
        /// </param>
        /// <param name="headers">
        ///     The headers to be added to query request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        public async IAsyncEnumerable<object?[]> Query(string query, QueryType? queryType = null,
            string? database = null, Dictionary<string, object>? namedParameters = null,
            Dictionary<string, string>? headers = null)
        {
            await foreach (var batch in QueryBatches(query, queryType, database, namedParameters, headers)
                               .ConfigureAwait(false))
            {
                for (var i = 0; i < batch.Column(0).Length; i++)
                {
                    var columnCount = batch.ColumnCount;
                    var row = new object?[columnCount];
                    for (var j = 0; j < columnCount; j++)
                    {
                        if (batch.Column(j) is not ArrowArray array)
                        {
                            continue;
                        }
                        row[j] = TypeCasting.GetMappedValue(
                            batch.Schema.FieldsList[j],
                            array.GetObjectValue(i)
                        );
                    }

                    yield return row;
                }
            }
        }

        /// <summary>
        /// Query data from InfluxDB IOx into PointData structure using FlightSQL.
        /// </summary>
        ///
        /// <example>
        /// The following example shows how to use SQL query with named parameters:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        /// 
        /// var results = client.QueryPoints(
        ///     query: "SELECT a, b, c FROM my_table WHERE id = $id AND name = $name",
        ///     namedParameters: new Dictionary&lt;string, object&gt; { { "id", 1 }, { "name", "test" } }
        /// );
        /// </code>
        /// </example>
        /// <example>
        /// The following example shows how to use custom request headers:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        /// 
        /// var results = client.QueryPoints(
        ///     query: "SELECT a, b, c FROM my_table",
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="namedParameters">
        ///     Name:Value pairs to use as named parameters for this query. Parameters referenced using
        ///     '$placeholder' syntax in the query will be substituted with the values provided.
        ///     The supported types are: string, bool, int, float.
        /// </param>
        /// <param name="headers">
        ///     The headers to be added to query request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        public async IAsyncEnumerable<PointDataValues> QueryPoints(string query, QueryType? queryType = null,
            string? database = null, Dictionary<string, object>? namedParameters = null,
            Dictionary<string, string>? headers = null)
        {
            await foreach (var batch in QueryBatches(query, queryType, database, namedParameters, headers)
                               .ConfigureAwait(false))
            {
                for (var i = 0; i < batch.Column(0).Length; i++)
                {
                    var point = RecordBatchConverter.ConvertToPointDataValue(batch, i);
                    yield return point;
                }
            }
        }

        /// <summary>
        /// Query data from InfluxDB IOx using FlightSQL.
        /// </summary>
        /// 
        /// <example>
        /// The following example shows how to use SQL query with named parameters:
        /// 
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        /// 
        /// var results = client.QueryBatches(
        ///     query: "SELECT a, b, c FROM my_table WHERE id = $id AND name = $name",
        ///     namedParameters: new Dictionary&lt;string, object&gt; { { "id", 1 }, { "name", "test" } }
        /// );
        /// </code>
        /// </example>
        /// 
        /// <example>
        /// The following example shows how to use custom request headers:
        /// 
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        /// 
        /// var results = client.QueryBatches(
        ///     query: "SELECT a, b, c FROM my_table",
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example>
        /// <param name="query">The SQL query string to execute.</param>
        /// <param name="queryType">The type of query sent to InfluxDB. Default to 'SQL'.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="namedParameters">
        ///     Name:Value pairs to use as named parameters for this query. Parameters referenced using
        ///     '$placeholder' syntax in the query will be substituted with the values provided.
        ///     The supported types are: string, bool, int, float.
        /// </param>
        /// <param name="headers">
        ///     The headers to be added to query request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        public IAsyncEnumerable<RecordBatch> QueryBatches(string query, QueryType? queryType = null,
            string? database = null, Dictionary<string, object>? namedParameters = null,
            Dictionary<string, string>? headers = null)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(InfluxDBClient));
            }

            return FlightSqlClient.Execute(query,
                (database ?? _config.Database) ?? throw new InvalidOperationException(OptionMessage("database")),
                queryType ?? QueryType.SQL,
                namedParameters ?? new Dictionary<string, object>(),
                headers ?? new Dictionary<string, string>());
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        ///
        /// <example>
        /// The following example shows how to write a single record with custom headers:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        ///
        /// await client.WriteRecordAsync(
        ///     record: "stat,unit=temperature value=24.5",
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example>
        /// 
        /// <param name="record">Specifies the record in InfluxDB Line Protocol. The <see cref="record" /> is considered as one batch unit.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="headers">
        ///     The headers to be added to write request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WriteRecordAsync(string record, string? database = null, WritePrecision? precision = null,
            Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
        {
            return WriteRecordsAsync(new[] { record }, database, precision, headers, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        ///
        /// <example>
        /// The following example shows how to write multiple records with custom headers:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        ///
        /// await client.WriteRecordsAsync(
        ///     records: new[] { "stat,unit=temperature value=24.5", "stat,unit=temperature value=25.5" },
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example>
        /// <param name="records">Specifies the records in InfluxDB Line Protocol. The <see cref="records" /> is considered as one batch unit.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="headers">
        ///     The headers to be added to write request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WriteRecordsAsync(IEnumerable<string> records, string? database = null,
            WritePrecision? precision = null, Dictionary<string, string>? headers = null,
            CancellationToken cancellationToken = default)
        {
            return WriteData(records, database, precision, headers, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        ///
        /// <example>
        /// The following example shows how to write a single point with custom headers:
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        ///
        /// await client.WritePointAsync(
        ///     point: PointData.Measurement("h2o").SetTag("location", "europe").SetField("level", 2),
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example>
        /// <param name="point">Specifies the Data point to write into InfluxDB. The <see cref="point" /> is considered as one batch unit. </param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="headers">
        ///     The headers to be added to write request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WritePointAsync(PointData point, string? database = null, WritePrecision? precision = null,
            Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
        {
            return WritePointsAsync(new[] { point }, database, precision, headers, cancellationToken);
        }

        /// <summary>
        /// Write data to InfluxDB.
        /// </summary>
        ///
        /// <example>
        /// The following example shows how to write multiple points with custom headers:
        ///
        /// <code>
        /// using var client = new InfluxDBClient(host: "http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");
        ///
        /// await client.WritePointsAsync(
        ///     points: new[]{
        ///         PointData.Measurement("h2o").SetTag("location", "europe").SetField("level", 2),
        ///         PointData.Measurement("h2o").SetTag("location", "us-west").SetField("level", 4),
        ///     },
        ///     headers: new Dictionary&lt;string, string&gt; { { "X-Tracing-Id", "123" } }
        /// );
        /// </code>
        /// </example> 
        /// <param name="points">Specifies the Data points to write into InfluxDB. The <see cref="points" /> is considered as one batch unit.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        /// <param name="precision">The to use for the timestamp in the write API call.</param>
        /// <param name="headers">
        ///     The headers to be added to write request. The headers specified here are preferred over
        ///     the headers specified in the client configuration.
        /// </param>
        /// <param name="cancellationToken">specifies the token to monitor for cancellation requests.</param>
        public Task WritePointsAsync(IEnumerable<PointData> points, string? database = null,
            WritePrecision? precision = null, Dictionary<string, string>? headers = null,
            CancellationToken cancellationToken = default)
        {
            return WriteData(points, database, precision, headers, cancellationToken);
        }

        private async Task WriteData(IEnumerable<object> data, string? database = null,
            WritePrecision? precision = null, Dictionary<string, string>? headers = null,
            CancellationToken cancellationToken = default)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(InfluxDBClient));
            }

            var precisionNotNull = precision ?? _config.WritePrecision;
            var sb = ToLineProtocolBody(data, precisionNotNull, _config.WriteOptions?.DefaultTags);
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
                    (database ?? _config.Database) ?? throw new InvalidOperationException(OptionMessage("database"))
                },
                { "org", _config.Organization },
                {
                    "precision",
                    Enum.GetName(typeof(WritePrecision), precisionNotNull)
                        ?.ToLowerInvariant()
                }
            };

            await _restClient
                .Request("api/v2/write", HttpMethod.Post, content, queryParams, headers, cancellationToken)
                .ConfigureAwait(false);
        }

        public void Dispose()
        {
            _httpClient.Dispose();
            FlightSqlClient.Dispose();
            _disposed = true;
        }

        private static StringBuilder ToLineProtocolBody(IEnumerable<object?> data, WritePrecision precision,
            Dictionary<string, string>? defaultTags = null)
        {
            var sb = new StringBuilder("");

            foreach (var item in data)
            {
                var lineProtocol = item switch
                {
                    PointData pointData => pointData.ToLineProtocol(precision, defaultTags),
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
                handler.AutomaticDecompression =
                    DecompressionMethods.GZip | DecompressionMethods.Deflate;
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

            client.DefaultRequestHeaders.UserAgent.ParseAdd(AssemblyHelper.GetUserAgent());
            if (!string.IsNullOrEmpty(config.Token))
            {
                string authScheme = string.IsNullOrEmpty(config.AuthScheme) ? "Token" : config.AuthScheme;
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(authScheme, config.Token);
            }

            return client;
        }

        private static string OptionMessage(string property)
        {
            return $"Please specify the '{property}' as a method parameter or use default configuration " +
                   $"at 'ClientConfig.{property[0].ToString().ToUpper()}{property.Substring(1)}'.";
        }
    }
}