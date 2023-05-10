using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Client;
using Grpc.Core;
using Grpc.Net.Client;
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

        public InfluxDBClient()
        {
            _flightSqlClient = new FlightSqlClient("http://localhost:8086");
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

    internal class FlightSqlClient : IDisposable
    {
        private readonly GrpcChannel _channel;
        private readonly FlightClient _flightClient;

        internal FlightSqlClient(string host)
        {
            _channel = GrpcChannel.ForAddress(host);
            _flightClient = new FlightClient(_channel);
        }

        internal async IAsyncEnumerable<RecordBatch> Execute(string query, Metadata headers = null)
        {
            var descriptor = FlightDescriptor.CreateCommandDescriptor(query);
            var info = await _flightClient.GetInfo(descriptor, headers).ResponseAsync.ConfigureAwait(false);

            // var stream = _flightClient.GetStream(info.Endpoints[0].Ticket);
            //
            // while (await stream.ResponseStream.MoveNext())
            // {
            //     yield return stream.ResponseStream.Current;
            // }

            foreach (var endpoint in info.Endpoints)
            {
                var stream = _flightClient.GetStream(endpoint.Ticket);
                while (await stream.ResponseStream.MoveNext())
                {
                    yield return stream.ResponseStream.Current;
                }
            }

            // foreach (var endpoint in info.Endpoints)
            // {
            //     // We may have multiple locations to choose from. Here we choose the first.
            //     var download_channel = GrpcChannel.ForAddress(endpoint.Locations.First().Uri);
            //     var download_client = new FlightClient(download_channel);
            //
            //     var stream = download_client.GetStream(endpoint.Ticket);
            //
            //     while (await stream.ResponseStream.MoveNext())
            //     { 
            //         yield return stream.ResponseStream.Current;
            //     }
            // }
        }

        public void Dispose()
        {
            _channel.Dispose();
        }
    }
}