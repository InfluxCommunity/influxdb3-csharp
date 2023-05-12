using System;
using System.Collections.Generic;
using System.Net.Http;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Client;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using InfluxDB3.Client.Config;

namespace InfluxDB3.Client.Internal;

internal class FlightSqlClient : IDisposable
{
    private readonly GrpcChannel _channel;
    private readonly FlightClient _flightClient;

    private readonly InfluxDBClientConfigs _configs;

    internal FlightSqlClient(InfluxDBClientConfigs configs, HttpClient httpClient)
    {
        _configs = configs;
        _channel = GrpcChannel.ForAddress(
            _configs.Host,
            new GrpcChannelOptions
            {
                HttpClient = httpClient,
                DisposeHttpClient = false,
                Credentials = _configs.Host.StartsWith("https", StringComparison.OrdinalIgnoreCase)
                    ? ChannelCredentials.SecureSsl
                    : ChannelCredentials.Insecure,
            });
        _flightClient = new FlightClient(_channel);
    }

    internal async IAsyncEnumerable<RecordBatch> Execute(string query, string database)
    {
        var headers = new Metadata();

        // authorization by token
        if (!string.IsNullOrEmpty(_configs.Token))
        {
            headers.Add("Authorization", $"Bearer {_configs.Token}");
        }

        // database
        headers.Add("database", database);

        // copy default headers
        if (_configs.Headers is not null)
        {
            foreach (var header in _configs.Headers)
            {
                headers.Add(header);
            }
        }

        var command = new CommandStatementQuery { Query = query };
        var descriptor = FlightDescriptor.CreateCommandDescriptor(Any.Pack(command).ToByteArray());

        var info = await _flightClient.GetInfo(descriptor, headers).ResponseAsync.ConfigureAwait(false);
        foreach (var endpoint in info.Endpoints)
        {
            var stream = _flightClient.GetStream(endpoint.Ticket, headers);
            while (await stream.ResponseStream.MoveNext().ConfigureAwait(false))
            {
                yield return stream.ResponseStream.Current;
            }
        }
    }

    public void Dispose()
    {
        _channel.Dispose();
    }
}