using System;
using System.Threading;
using System.Threading.Tasks;
using InfluxDB3.Client;

namespace InfluxDB3.Examples.Downsampling;

public class DownsamplingExample
{
    static async Task Main(string[] args)
    {
        var host = Environment.GetEnvironmentVariable("INFLUXDB_URL") ?? "https://us-east-1-1.aws.cloud2.influxdata.com";
        var token = Environment.GetEnvironmentVariable("INFLUXDB_TOKEN") ?? "my-token";
        var database = Environment.GetEnvironmentVariable("INFLUXDB_DATABASE") ?? "my-database";

        using var client = new InfluxDBClient(host: host, token: token, database: database);

        //
        // Write data
        //
        await client.WriteRecordAsync("stat,unit=temperature avg=24.5,max=45.0");
        Thread.Sleep(1_000);

        await client.WriteRecordAsync("stat,unit=temperature avg=28,max=40.3");
        Thread.Sleep(1_000);

        await client.WriteRecordAsync("stat,unit=temperature avg=20.5,max=49.0");
        Thread.Sleep(1_000);

        //
        // Query downsampled data
        //
        const string downsamplingQuery = @"SELECT
            date_bin('5 minutes', ""time"") as window_start,
            AVG(""avg"") as avg,
            MAX(""max"") as max
        FROM ""stat""
        WHERE
              ""time"" >= now() - interval '1 hour'
        GROUP BY window_start
            ORDER BY window_start ASC;
        ";

        //
        // Execute downsampling query into pointValues
        //
        await foreach (var row in client.QueryPoints(downsamplingQuery))
        {
            var timestamp = row.GetField<DateTimeOffset>("window_start") ?? throw new InvalidOperationException();
            Console.WriteLine($"{timestamp}: avg is {row.GetDoubleField("avg")}, max is {row.GetDoubleField("max")}");

            //
            // write back downsampled date to 'stat_downsampled' measurement
            //
            var downsampledPoint = row
                .AsPoint("stat_downsampled")
                .RemoveField("window_start")
                .SetTimestamp(timestamp);

            await client.WritePointAsync(downsampledPoint);
        }
    }

    public static async Task Run()
    {
        await Main(Array.Empty<string>());
    }
}