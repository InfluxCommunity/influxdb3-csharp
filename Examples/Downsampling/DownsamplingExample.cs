using System.Threading.Tasks;
using InfluxDB3.Client;

namespace InfluxDB3.Examples.Downsampling;

public class DownsamplingExample
{
    static async Task Main(string[] args)
    {
        const string host = "https://us-east-1-1.aws.cloud2.influxdata.com";
        const string token = "my-token";
        const string database = "my-database";

        using var client = new InfluxDBClient(host: host, token: token, database: database);
    }
}