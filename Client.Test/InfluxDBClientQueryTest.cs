using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using InfluxDB3.Client.Internal;
using InfluxDB3.Client.Query;
using Moq;

namespace InfluxDB3.Client.Test;

public class InfluxDBClientQueryTest : MockServerTest
{
    private InfluxDBClient _client;

    [TearDown]
    public new void TearDown()
    {
        _client?.Dispose();
    }

    [Test]
    public void AlreadyDisposed()
    {
        _client = new InfluxDBClient(MockServerUrl);
        _client.Dispose();
        var ae = Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await foreach (var unused in _client.Query("SELECT 1"))
            {
            }
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo($"Cannot access a disposed object.{Environment.NewLine}Object name: 'InfluxDBClient'."));
    }

    [Test]
    public void NotSpecifiedDatabase()
    {
        _client = new InfluxDBClient(MockServerUrl);
        var ae = Assert.Throws<InvalidOperationException>(() => { _client.QueryBatches("SELECT 1"); });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo("Please specify the 'database' as a method parameter or use default configuration at 'ClientConfig.Database'."));
    }

    [Test]
    public async Task PassNamedParametersToFlightClient()
    {
        //
        // Mock the FlightSqlClient
        //
        var mockFlightSqlClient = new Mock<IFlightSqlClient>();
        mockFlightSqlClient
            .Setup(m => m.Execute(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<QueryType>(),
                It.IsAny<Dictionary<string, object>>(), It.IsAny<Dictionary<string, string>>()))
            .Returns(new List<RecordBatch>().ToAsyncEnumerable());

        //
        // Setup the client with the mocked FlightSqlClient
        //
        _client = new InfluxDBClient(MockServerUrl);
        _client.FlightSqlClient.Dispose();
        _client.FlightSqlClient = mockFlightSqlClient.Object;

        const string query = "select * from cpu where location = $location and core_count = $core-count and production = $production and max_frequency > $max-frequency";
        const QueryType queryType = QueryType.SQL;
        var namedParameters = new Dictionary<string, object>
        {
            { "location", "us" },
            { "core-count", 4 },
            { "production", true },
            { "max-frequency", 3.5 }
        };

        _ = await _client.QueryPoints(query, database: "my-db", queryType: queryType, namedParameters: namedParameters)
            .ToListAsync();
        mockFlightSqlClient.Verify(m => m.Execute(query, "my-db", queryType, namedParameters, new Dictionary<string, string>()), Times.Exactly(1));
    }

    [Test]
    public void NotSupportedQueryParameterType()
    {
        _client = new InfluxDBClient(MockServerUrl);
        var ae = Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            _ = await _client
                .Query("select * from cpu where location = $location", database: "my-db",
                    queryType: QueryType.SQL, namedParameters: new Dictionary<string, object>
                    {
                        { "location", DateTime.UtcNow }
                    })
                .ToListAsync();
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message,
            Is.EqualTo(
                "The parameter 'location' has unsupported type 'System.DateTime'. The supported types are 'string', 'bool', 'int' and 'float'."));
    }

    [Test]
    public async Task PassHeadersToFlightClient()
    {
        //
        // Mock the FlightSqlClient
        //
        var mockFlightSqlClient = new Mock<IFlightSqlClient>();
        mockFlightSqlClient
            .Setup(m => m.Execute(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<QueryType>(),
                It.IsAny<Dictionary<string, object>>(), It.IsAny<Dictionary<string, string>>()))
            .Returns(new List<RecordBatch>().ToAsyncEnumerable());

        //
        // Setup the client with the mocked FlightSqlClient
        //
        _client = new InfluxDBClient(MockServerUrl);
        _client.FlightSqlClient.Dispose();
        _client.FlightSqlClient = mockFlightSqlClient.Object;

        const string query = "select * from cpu";
        const QueryType queryType = QueryType.SQL;

        var headers = new Dictionary<string, string>{
        {
            "X-Tracing-Id", "123"
        }};
        _ = await _client.QueryPoints(query, database: "my-db", queryType: queryType, headers: headers)
            .ToListAsync();
        mockFlightSqlClient.Verify(m => m.Execute(query, "my-db", queryType, new Dictionary<string, object>(), headers), Times.Exactly(1));
    }
}