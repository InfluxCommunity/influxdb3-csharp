using System.Threading.Tasks;
using InfluxDB3.Client.Config;
using NUnit.Framework;

namespace InfluxDB3.Client.Test.Integration;

public class ApiTest : IntegrationTest
{
    [Test]
    public async Task GetServerVersion()
    {
        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Host,
            Token = Token,
            Database = Database
        });

        var result = await client.GetServerVersion();
        Assert.That(result, Is.Not.Null);
    }
}