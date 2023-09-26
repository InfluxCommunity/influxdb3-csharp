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
        const string host = "https://us-east-1-1.aws.cloud2.influxdata.com";
        const string token = "pIKPXB_0XuG6bL1Nu959JUNvoHiXtZNANp4FMABnDFIVjOD-ZdJjaV7IrmiBPL4ARspL8Y4dWD0qVvUZfcApgg==";
        const string database = "database";

        using var client = new InfluxDBClient(host: host, token: token, database: database);

        //
        // Write by Point
        //
        var point = PointData.Measurement("temperature")
            .SetTag("location", "west")
            .SetField("value", 55.15)
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

        //
        // SQL Query all PointDataValues
        //
        const string sql2 = "select *, 'temperature' as measurement from temperature order by time desc limit 5";
        Console.WriteLine();
        Console.WriteLine("simple query to points with measurement manually specified");
        await foreach (var row in client.QueryPoints(query: sql2, queryType: QueryType.SQL))
        {
            Console.WriteLine(row.AsPoint().ToLineProtocol());
        }

        //
        // SQL Query windows
        //
        const string sql3 = @"
            SELECT
            date_bin('5 minutes', ""time"") as time,
            AVG(""value"") as avgvalue
            FROM ""temperature""
            WHERE
            ""time"" >= now() - interval '1 hour'
            GROUP BY time
            ORDER BY time DESC
            limit 3
            ;
        ";
        Console.WriteLine();
        Console.WriteLine("more complex query to points WITHOUT measurement manually specified");
        await foreach (var row in client.QueryPoints(query: sql3, queryType: QueryType.SQL))
        {
            Console.WriteLine(row.AsPoint("measurement").ToLineProtocol());
        }

        Console.WriteLine();
        Console.WriteLine("simple InfluxQL query to points. InfluxQL sends measurement in query");
        await foreach (var row in client.QueryPoints(query: influxQL, queryType: QueryType.InfluxQL))
        {
            Console.WriteLine(row.AsPoint().ToLineProtocol());
        }
    }
}