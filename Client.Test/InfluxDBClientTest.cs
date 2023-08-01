using System;

// ReSharper disable ObjectCreationAsStatement
// ReSharper disable AssignNullToNotNullAttribute

namespace InfluxDB3.Client.Test;

public class InfluxDBClientTest
{
    [Test]
    public void Create()
    {
        using var client = new InfluxDBClient("http://localhost:8086", organization: "org", bucket: "database");

        Assert.That(client, Is.Not.Null);
    }

    [Test]
    public void RequiredHost()
    {
        var ae = Assert.Throws<ArgumentException>(() => { new InfluxDBClient(host: null); });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo("The URL of the InfluxDB server has to be defined."));
    }

    [Test]
    public void RequiredConfigs()
    {
        var ae = Assert.Throws<ArgumentException>(() => { new InfluxDBClient(null); });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo("The configuration of the client has to be defined."));
    }
}