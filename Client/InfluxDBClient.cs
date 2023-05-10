using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using InfluxDB3.Client.Internal;
using InfluxDB3.Client.Writes;

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
        private readonly FlightSqlClient _flightSqlClient;

        /// <summary>
        /// This class provides an interface for interacting with an InfluxDB server,
        /// simplifying common operations such as writing, querying.
        /// </summary>
        /// <param name="host">The hostname or IP address of the InfluxDB server.</param>
        /// <param name="token">The authentication token for accessing the InfluxDB server.</param>
        /// <param name="database">The database to be used for InfluxDB operations.</param>
        public InfluxDBClient(string host, string token = null, string database = null)
        {
            if (string.IsNullOrEmpty(host))
            {
                throw new ArgumentException("The hostname or IP address of the InfluxDB server has to be defined.");
            }

            _flightSqlClient = new FlightSqlClient(host: host);
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
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(FlightSqlClient));
            }

            return Task.CompletedTask;
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
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(FlightSqlClient));
            }

            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _flightSqlClient.Dispose();
            _disposed = true;
        }
    }
}