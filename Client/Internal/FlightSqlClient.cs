using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Runtime.Serialization.Json;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Client;
using Grpc.Core;
using Grpc.Net.Client;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Query;

namespace InfluxDB3.Client.Internal;

internal class FlightSqlClient : IDisposable
{
    private readonly GrpcChannel _channel;
    private readonly FlightClient _flightClient;

    private readonly InfluxDBClientConfigs _configs;
    private readonly DataContractJsonSerializer _serializer;

    internal FlightSqlClient(InfluxDBClientConfigs configs, HttpClient httpClient)
    {
        _configs = configs;
        _channel = GrpcChannel.ForAddress(
            _configs.HostUrl,
            new GrpcChannelOptions
            {
                HttpClient = httpClient,
                DisposeHttpClient = false,
                Credentials = _configs.HostUrl.StartsWith("https", StringComparison.OrdinalIgnoreCase)
                    ? ChannelCredentials.SecureSsl
                    : ChannelCredentials.Insecure,
            });
        _flightClient = new FlightClient(_channel);
        _serializer = new DataContractJsonSerializer(typeof(Dictionary<string, string>),
            new DataContractJsonSerializerSettings
            {
                UseSimpleDictionaryFormat = true
            });
    }

    internal async IAsyncEnumerable<RecordBatch> Execute(string query, string database, QueryType queryType)
    {
        var headers = new Metadata();

        // authorization by token
        if (!string.IsNullOrEmpty(_configs.AuthToken))
        {
            headers.Add("Authorization", $"Bearer {_configs.AuthToken}");
        }

        // set query parameters
        var ticketData = new Dictionary<string, string>()
        {
            { "database", database },
            { "sql_query", query },
            { "query_type", Enum.GetName(typeof(QueryType), queryType)!.ToLowerInvariant() }
        };

        // serialize to json
        using var memoryStream = new MemoryStream();
        _serializer.WriteObject(memoryStream, ticketData);
        var json = Encoding.UTF8.GetString(memoryStream.ToArray());

        using var stream = _flightClient.GetStream(new FlightTicket(json), headers);
        while (await stream.ResponseStream.MoveNext().ConfigureAwait(false))
        {
            yield return stream.ResponseStream.Current;
        }
    }

    public void Dispose()
    {
        _channel.Dispose();
    }
}