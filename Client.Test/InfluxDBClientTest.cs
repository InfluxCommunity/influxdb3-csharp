using System;

namespace InfluxDB3.Client.Test;

public class InfluxDBClientTest
{
    [Test]
    public void Create()
    {
        var client = new InfluxDBClient("http://localhost:8086");

        Assert.That(client, Is.Not.Null);
    }

    [Test]
    public void RequiredHost()
    {
        // ReSharper disable once ObjectCreationAsStatement
        var ae = Assert.Throws<ArgumentException>(() => { new InfluxDBClient(host: null); });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo("The hostname or IP address of the InfluxDB server has to be defined."));
    }
}