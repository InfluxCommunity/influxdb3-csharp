using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using Grpc.Net.Compression;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Internal;
using InfluxDB3.Client.Query;

namespace InfluxDB3.Client.Test.Internal;

public class FlightSqlClientTest : MockServerTest
{
    private IFlightSqlClient _flightSqlClient;

    [SetUp]
    public void SetUp()
    {
        var config = new ClientConfig
        {
            Host = MockServerUrl,
            Timeout = TimeSpan.FromSeconds(45)
        };

        _flightSqlClient = new FlightSqlClient(config, InfluxDBClient.CreateAndConfigureHttpClient(config));
    }

    [TearDown]
    public new void TearDown()
    {
        _flightSqlClient.Dispose();
    }

    [Test]
    public void PrepareFlightTicket()
    {
        var prepareFlightTicket = _flightSqlClient.PrepareFlightTicket(
            "select * from mem",
            "my-db",
            QueryType.InfluxQL, new Dictionary<string, object>());

        const string ticket =
            @"{""database"":""my-db"",""sql_query"":""select * from mem"",""query_type"":""influxql""}";
        Assert.Multiple(() =>
        {
            Assert.That(prepareFlightTicket, Is.Not.Null);
            var actual = Encoding.UTF8.GetString(prepareFlightTicket.Ticket.ToByteArray());
            Console.WriteLine(actual);
            Assert.That(actual, Is.EqualTo(ticket));
        });
    }

    [Test]
    public void PrepareFlightTicketNamedParameters()
    {
        var prepareFlightTicket = _flightSqlClient.PrepareFlightTicket(
            "select * from cpu where location = $location and production = $production and count = $count and temperature = $temperature",
            "my-db",
            QueryType.SQL,
            new Dictionary<string, object>
            {
                { "location", "us" },
                { "production", true },
                { "count", 10 },
                { "temperature", 23.5 }
            });

        var ticket = "{\"database\":\"my-db\"," +
                     "\"sql_query\":\"select * from cpu where location = $location and production = $production and count = $count and temperature = $temperature\"," +
                     "\"query_type\":\"sql\"," +
                     "\"params\": {\"location\":\"us\",\"production\":true,\"count\":10,\"temperature\":23.5}}";
        Assert.Multiple(() =>
        {
            Assert.That(prepareFlightTicket, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(prepareFlightTicket.Ticket.ToByteArray()), Is.EqualTo(ticket));
        });
    }

    [Test]
    public void HeadersMetadataFromRequest()
    {
        var prepareHeadersMetadata =
            _flightSqlClient.PrepareHeadersMetadata(new Dictionary<string, string> { { "X-Tracing-Id", "987" } });

        Assert.Multiple(() =>
        {
            Assert.That(prepareHeadersMetadata, Is.Not.Null);
            Assert.That(prepareHeadersMetadata, Has.Count.EqualTo(2));
            Assert.That(prepareHeadersMetadata[0].Key, Is.EqualTo("user-agent"));
            Assert.That(prepareHeadersMetadata[0].Value, Is.EqualTo(AssemblyHelper.GetUserAgent()));
            Assert.That(prepareHeadersMetadata[1].Key, Is.EqualTo("x-tracing-id"));
            Assert.That(prepareHeadersMetadata[1].Value, Is.EqualTo("987"));
        });
    }

    [Test]
    public void HeadersMetadataFromConfig()
    {
        _flightSqlClient.Dispose();

        var config = new ClientConfig
        {
            Host = MockServerUrl,
            Timeout = TimeSpan.FromSeconds(45),
            Headers = new Dictionary<string, string>
            {
                { "X-Global-Tracing-Id", "123" }
            }
        };

        _flightSqlClient = new FlightSqlClient(config, InfluxDBClient.CreateAndConfigureHttpClient(config));

        var prepareHeadersMetadata =
            _flightSqlClient.PrepareHeadersMetadata(new Dictionary<string, string>());

        Assert.Multiple(() =>
        {
            Assert.That(prepareHeadersMetadata, Is.Not.Null);
            Assert.That(prepareHeadersMetadata, Has.Count.EqualTo(2));
            Assert.That(prepareHeadersMetadata[0].Key, Is.EqualTo("user-agent"));
            Assert.That(prepareHeadersMetadata[0].Value, Is.EqualTo(AssemblyHelper.GetUserAgent()));
            Assert.That(prepareHeadersMetadata[1].Key, Is.EqualTo("x-global-tracing-id"));
            Assert.That(prepareHeadersMetadata[1].Value, Is.EqualTo("123"));
        });
    }

    [Test]
    public void HeadersMetadataFromRequestArePreferred()
    {
        _flightSqlClient.Dispose();

        var config = new ClientConfig
        {
            Host = MockServerUrl,
            Timeout = TimeSpan.FromSeconds(45),
            Headers = new Dictionary<string, string>
            {
                { "X-Tracing-Id", "ABC" }
            }
        };

        _flightSqlClient = new FlightSqlClient(config, InfluxDBClient.CreateAndConfigureHttpClient(config));

        var prepareHeadersMetadata =
            _flightSqlClient.PrepareHeadersMetadata(new Dictionary<string, string> { { "X-Tracing-Id", "258" } });

        Assert.Multiple(() =>
        {
            Assert.That(prepareHeadersMetadata, Is.Not.Null);
            Assert.That(prepareHeadersMetadata, Has.Count.EqualTo(2));
            Assert.That(prepareHeadersMetadata[0].Key, Is.EqualTo("user-agent"));
            Assert.That(prepareHeadersMetadata[0].Value, Is.EqualTo(AssemblyHelper.GetUserAgent()));
            Assert.That(prepareHeadersMetadata[1].Key, Is.EqualTo("x-tracing-id"));
            Assert.That(prepareHeadersMetadata[1].Value, Is.EqualTo("258"));
        });
    }

    [Test]
    public void UserAgentHeaderNotChanged()
    {
        _flightSqlClient.Dispose();

        var config = new ClientConfig
        {
            Host = MockServerUrl,
            Timeout = TimeSpan.FromSeconds(45),
            Headers = new Dictionary<string, string>
            {
                { "User-Agent", "some/user-agent" }
            }
        };

        _flightSqlClient = new FlightSqlClient(config, InfluxDBClient.CreateAndConfigureHttpClient(config));

        var prepareHeadersMetadata =
            _flightSqlClient.PrepareHeadersMetadata(new Dictionary<string, string> { { "user-agent", "another/user-agent" } });

        Assert.Multiple(() =>
        {
            Assert.That(prepareHeadersMetadata, Is.Not.Null);
            Assert.That(prepareHeadersMetadata, Has.Count.EqualTo(1));
            Assert.That(prepareHeadersMetadata[0].Key, Is.EqualTo("user-agent"));
            Assert.That(prepareHeadersMetadata[0].Value, Is.EqualTo(AssemblyHelper.GetUserAgent()));
        });
    }

    [Test]
    public void TestGrpcCallOptions()
    {
        var config = new ClientConfig
        {
            Host = MockServerUrl,
            QueryOptions = {
                Deadline = DateTime.Now.AddMinutes(5),
                MaxReceiveMessageSize = 8_388_608,
                MaxSendMessageSize = 10000,
                CompressionProviders = ImmutableArray<ICompressionProvider>.Empty
            }
        };

        Assert.DoesNotThrow(() =>
            _flightSqlClient = new FlightSqlClient(config, InfluxDBClient.CreateAndConfigureHttpClient(config)));
    }

}