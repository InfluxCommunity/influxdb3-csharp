using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.Serialization;
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

/// <summary>
/// Simple "FlightSQL" client implementation.
/// </summary>
internal interface IFlightSqlClient : IDisposable
{
    /// <summary>
    /// Execute the query and return the result as a sequence of record batches.
    /// </summary>
    internal IAsyncEnumerable<RecordBatch> Execute(string query, string database, QueryType queryType,
        Dictionary<string, object> namedParameters, Dictionary<string, string> headers, TimeSpan? timeout = null);

    /// <summary>
    /// Prepare the FlightTicket for the query.
    /// </summary>
    internal FlightTicket PrepareFlightTicket(string query, string database, QueryType queryType,
        Dictionary<string, object> namedParameters);

    /// <summary>
    /// Prepare the headers metadata.
    /// </summary>
    /// <param name="headers">The invocation headers</param>
    /// <returns></returns>
    internal Metadata PrepareHeadersMetadata(Dictionary<string, string> headers);
}

internal class FlightSqlClient : IFlightSqlClient
{
    private readonly GrpcChannel _channel;
    private readonly FlightClient _flightClient;

    private readonly ClientConfig _config;
    private readonly DataContractJsonSerializer _serializer;

    internal FlightSqlClient(ClientConfig config, HttpClient httpClient)
    {
        _config = config;
        _channel = GrpcChannel.ForAddress(
            _config.Host,
            new GrpcChannelOptions
            {
                HttpClient = httpClient,
                DisposeHttpClient = false,
                Credentials = _config.Host.StartsWith("https", StringComparison.OrdinalIgnoreCase)
                    ? ChannelCredentials.SecureSsl
                    : ChannelCredentials.Insecure,
                MaxReceiveMessageSize = _config.QueryOptions.MaxReceiveMessageSize,
                MaxSendMessageSize = _config.QueryOptions.MaxSendMessageSize,
                CompressionProviders = _config.QueryOptions.CompressionProviders,
            });
        _flightClient = new FlightClient(_channel);
        var knownTypes = new List<Type> { typeof(string), typeof(int), typeof(float), typeof(bool) };
        _serializer = new DataContractJsonSerializer(typeof(Dictionary<string, object>),
            new DataContractJsonSerializerSettings
            {
                UseSimpleDictionaryFormat = true,
                KnownTypes = knownTypes,
                EmitTypeInformation = EmitTypeInformation.Never
            });
    }

    async IAsyncEnumerable<RecordBatch> IFlightSqlClient.Execute(string query, string database, QueryType queryType,
        Dictionary<string, object> namedParameters, Dictionary<string, string> headers, TimeSpan? timeout)
    {
        //
        // verify that values of namedParameters is supported type
        //
        foreach (var keyValuePair in namedParameters)
        {
            var key = keyValuePair.Key;
            var value = keyValuePair.Value;
            if (value is not string and not int and not float and not bool)
            {
                throw new ArgumentException($"The parameter '{key}' has unsupported type '{value.GetType()}'. " +
                                            $"The supported types are 'string', 'bool', 'int' and 'float'.");
            }
        }

        var metadata = ((IFlightSqlClient)this).PrepareHeadersMetadata(headers);

        var ticket = ((IFlightSqlClient)this).PrepareFlightTicket(query, database, queryType, namedParameters);

        DateTime? deadline = null;
        if (timeout is not null)
        {
            deadline = DateTime.UtcNow.Add(timeout.Value);
        }
        else if (_config.QueryOptions.Deadline is not null)
        {
            deadline = _config.QueryOptions.Deadline.Value;
        }
        else if (_config.QueryTimeout is not null)
        {
            deadline = DateTime.UtcNow.Add(_config.QueryTimeout.Value);
        }

        using var stream = _flightClient.GetStream(ticket, metadata, deadline);
        while (await stream.ResponseStream.MoveNext().ConfigureAwait(false))
        {
            yield return stream.ResponseStream.Current;
        }
    }

    FlightTicket IFlightSqlClient.PrepareFlightTicket(string query, string database, QueryType queryType,
        Dictionary<string, object> namedParameters)
    {
        // set query parameters
        var ticketData = new Dictionary<string, object>
        {
            { "database", database },
            { "sql_query", query },
            { "query_type", Enum.GetName(typeof(QueryType), queryType)!.ToLowerInvariant() },
        };

        //
        // serialize to json
        //
        var json = SerializeDictionary(ticketData);
        //
        // serialize named parameters
        //
        if (namedParameters.Count > 0)
        {
            json = json.TrimEnd('}') + $",\"params\": {SerializeDictionary(namedParameters)}}}";
        }

        var flightTicket = new FlightTicket(json);
        return flightTicket;
    }

    Metadata IFlightSqlClient.PrepareHeadersMetadata(Dictionary<string, string> headers)
    {
        var metadata = new Metadata();

        // user-agent
        metadata.Add("user-agent", AssemblyHelper.GetUserAgent());

        // authorization by token
        if (!string.IsNullOrEmpty(_config.Token))
        {
            metadata.Add("Authorization", $"Bearer {_config.Token}");
        }

        // add request headers
        foreach (var header in headers.Where(header => !header.Key.ToLower().Equals("user-agent")))
        {
            metadata.Add(header.Key, header.Value);
        }
        // add config headers
        if (_config.Headers != null)
        {
            foreach (var header in _config.Headers.Where(header => !headers.ContainsKey(header.Key)))
            {
                if (!header.Key.ToLower().Equals("user-agent"))
                {
                    metadata.Add(header.Key, header.Value);
                }
            }
        }
        return metadata;
    }

    private string SerializeDictionary(Dictionary<string, object> ticketData)
    {
        using var memoryStream = new MemoryStream();
        _serializer.WriteObject(memoryStream, ticketData);
        return Encoding.UTF8.GetString(memoryStream.ToArray());
    }

    public void Dispose()
    {
        _channel.Dispose();
    }
}