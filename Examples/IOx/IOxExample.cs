using System;
using System.Threading.Tasks;
using InfluxDB3.Client;
using InfluxDB3.Client.Query;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Examples.IOx;

public class IOxExample
{
    static async Task Main(string[] args)
    {
        const string hostUrl = "https://us-east-1-1.aws.cloud2.influxdata.com";
        const string database = "my-database";
        const string authToken = "my-token";

        using var client = new InfluxDBClient(hostUrl, authToken: authToken, database: database);

        //
        // Write by Point
        //
        var point = PointData.Measurement("temperature")
            .AddTag("location", "west")
            .AddField("value", 55.15)
            .SetTimestamp(DateTime.UtcNow.AddSeconds(-10));
        await client.WritePointAsync(point: point);

        //
        // Write by LineProtocol
        //
        const string record = "temperature,location=north value=60.0";
        await client.WriteRecordAsync(record: record);

        //
        // Query by SQL
        //
        const string sql = "select time,location,value from temperature order by time desc limit 10";
        Console.WriteLine("{0,-30}{1,-15}{2,-15}", "time", "location", "value");
        await foreach (var row in client.Query(query: sql))
        {
            Console.WriteLine("{0,-30}{1,-15}{2,-15}", row[0], row[1], row[2]);
        }

        Console.WriteLine();

        //
        // Query by InfluxQL
        //
        const string influxQL =
            "select MEAN(value) from temperature group by time(1d) fill(none) order by time desc limit 10";
        Console.WriteLine("{0,-30}{1,-15}", "time", "mean");
        await foreach (var row in client.Query(query: influxQL, queryType: QueryType.InfluxQL))
        {
            Console.WriteLine("{0,-30}{1,-15}", row[1], row[2]);
        }
    }
}