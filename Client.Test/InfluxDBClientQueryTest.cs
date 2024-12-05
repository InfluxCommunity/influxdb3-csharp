using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
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

    // todo: remove
    // [Test]
    // public async Task bar()
    // {
        // var hostCloud = "https://us-east-1-1.aws.cloud2.influxdata.com";
        // var adminTokenCloud =
        //     "xWh3VQCb3pMJPw7T2lnEwFLXO-pb4OWzfNN76UTpmRKtlg83yJlz6maLC3AL0B6M6gMWWZY2QApzSdEeEopWlQ==";
        // using var client = new InfluxDBClient(
        //     hostCloud,
        //     token: adminTokenCloud,
        //     organization: "admin",
        //     database: "admin");
        // const string query = "select * from host10";
        // var a = await client.QueryPoints(query).ToListAsync();
    //     var meta = new Dictionary<string, string>
    //     {
    //         {
    //             "iox::column::type", "iox::column_type::field::integer"
    //         }
    //     };
    //
    //     Field intField = new Field("column2", Int32Type.Default, true, meta);
    //     
    // }
    
    // todo: remove
    // [Test]
    // public void fooTest()
    // {
    //     var meta = new Dictionary<string, string>
    //     {
    //         {
    //             "iox::column::type", "iox::column_type::field::integer"
    //         }
    //     };
    //
    //     Field intField = new Field("column2", Int32Type.Default, true, meta);
    //     Int32Array intArray = new Int32Array.Builder().Append(1).Append(2).AppendNull().Append(4).Build();
    //     Schema schema1 = new Schema(new[] { intField, }, null);
    //     RecordBatch recordBatch = new RecordBatch(schema1, new IArrowArray[] { intArray }, intArray.Length);
    //
    //     var rowCount = recordBatch.Column(0).Length;
    //     for (var i = 0; i < rowCount; i++)
    //     {
    //         for (var j = 0; j < recordBatch.ColumnCount; j++)
    //         {
    //             var schema = recordBatch.Schema.FieldsList[j];
    //             var type = schema.Metadata["iox::column::type"];
    //             var parts = type.Split(new[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
    //             var valueType = parts[2];
    //             
    //             Console.WriteLine(valueType);
    //         }
    //     }
    //
    // }
}