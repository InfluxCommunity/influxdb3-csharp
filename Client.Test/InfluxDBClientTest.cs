namespace InfluxDB3.Client.Test;

public class InfluxDBClientTest
{
    [Test]
    public void Test1()
    {
        var client = new InfluxDBClient();

        Assert.That(client, Is.Not.Null);
    }
}