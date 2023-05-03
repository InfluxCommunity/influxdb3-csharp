namespace InfluxDB3.Client.Test;

public class InfluxDBClientTest
{
    [Test]
    public void NotNull()
    {
        var client = new InfluxDBClient();
        client.Dummy();

        Assert.That(client, Is.Null);
    }
}