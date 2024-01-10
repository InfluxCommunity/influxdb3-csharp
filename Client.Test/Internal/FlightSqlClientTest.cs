using System;
using System.Collections.Generic;
using System.Text;
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

    [TearDownAttribute]
    public void TearDownAttribute()
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
            "select * from cpu where location = $location",
            "my-db",
            QueryType.SQL,
            new Dictionary<string, object>
            {
                { "location", "us" }
            });

        const string ticket =
            @"{""database"":""my-db"",""sql_query"":""select * from cpu where location = $location"",""query_type"":""sql"",""params"": {""location"":""us""}}";
        Assert.Multiple(() =>
        {
            Assert.That(prepareFlightTicket, Is.Not.Null);
            var actual = Encoding.UTF8.GetString(prepareFlightTicket.Ticket.ToByteArray());
            Console.WriteLine(actual);
            Assert.That(actual, Is.EqualTo(ticket));
        });
    }
}