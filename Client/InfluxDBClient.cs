using System;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Client;
using Grpc.Core;
using Grpc.Net.Client;

namespace InfluxDB3.Client
{
    public interface IInfluxDBClient: IDisposable
    {
        /// <summary>
        /// Query data from InfluxDB IOx using FlightSQL.
        /// </summary>
        /// <param name="query">The SQL query string to execute.</param>
        /// <returns>Batches of rows</returns>
        /// <exception cref="ObjectDisposedException">The client is already disposed</exception>
        IAsyncEnumerable<RecordBatch> Query(string query);
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