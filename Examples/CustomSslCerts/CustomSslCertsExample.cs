using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using InfluxDB3.Client;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Query;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Examples.CustomSslCerts;

public class CustomSslCertsExample
{
    static async Task Main(string[] args)
    {
        var host = Environment.GetEnvironmentVariable("INFLUXDB_URL") ??
                   "https://us-east-1-1.aws.cloud2.influxdata.com";
        var token = Environment.GetEnvironmentVariable("INFLUXDB_TOKEN") ?? "my-token";
        var database = Environment.GetEnvironmentVariable("INFLUXDB_DATABASE") ?? "my-database";
        var sslRootsFilePath = Environment.GetEnvironmentVariable("INFLUXDB_SSL_ROOTS_FILE_PATH") ?? null;
        var proxyUrl = Environment.GetEnvironmentVariable("INFLUXDB_PROXY_URL") ?? null;

        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = host,
            Token = token,
            Database = database,
            SslRootsFilePath = sslRootsFilePath,
            Proxy = proxyUrl == null
                ? null
                : new WebProxy
                {
                    Address = new Uri(proxyUrl),
                    BypassProxyOnLocal = false
                }
        });

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



        await foreach (var row in client.Query(query: "SELECT 1"))
        {
            Console.WriteLine(row);
        }
        
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
        // Query by parametrized SQL
        //
        const string sqlParams =
            "select time,location,value from temperature where location=$location order by time desc limit 10";
        Console.WriteLine("Query by parametrized SQL");
        Console.WriteLine("{0,-30}{1,-15}{2,-15}", "time", "location", "value");
        await foreach (var row in client.Query(query: sqlParams,
                           namedParameters: new Dictionary<string, object> { { "location", "west" } }))
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

    public static async Task Run()
    {
        await Main(Array.Empty<string>());
    }
}