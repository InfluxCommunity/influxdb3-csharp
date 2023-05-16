using System;
using System.Threading.Tasks;
using InfluxDB3.Client;
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
            .Tag("location", "west")
            .Field("value", 55.15)
            .Timestamp(DateTime.UtcNow.AddSeconds(-10));
        await client.WritePointAsync(point);

        //
        // Write by LineProtocol
        //
        const string record = "temperature,location=north value=60.0";
        await client.WriteRecordAsync(record);

        //
        // Query data
        //
        Console.Write("{0,-30}{1,-15}{2,-15}", "time", "location", "value");
        Console.WriteLine();
        await foreach
            (var row in client.Query("select time,location,value from temperature order by time asc limit 10"))
        {
            Console.Write("{0,-30}{1,-15}{2,-15}", row[0], row[1], row[2]);
            Console.WriteLine();
        }
    }
}