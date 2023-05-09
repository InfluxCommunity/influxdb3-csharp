namespace InfluxDB3.Client.Test;

public class InfluxDBClientTest
{
    [Test]
    public void NotNull()
    {
        var client = new InfluxDBClient();
        client.Dispose();

        Assert.That(client, Is.Not.Null);
    }
}