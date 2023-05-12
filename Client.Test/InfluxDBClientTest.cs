using System;

namespace InfluxDB3.Client.Test;

public class InfluxDBClientTest
{
    [Test]
    public void Create()
    {
        using var client = new InfluxDBClient("http://localhost:8086", "database");

        Assert.That(client, Is.Not.Null);
    }

    [Test]
    public void RequiredHost()
    {
        // ReSharper disable once ObjectCreationAsStatement
        // ReSharper disable once AssignNullToNotNullAttribute
        var ae = Assert.Throws<ArgumentException>(() => { new InfluxDBClient(host: null, database: "database"); });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo("The hostname or IP address of the InfluxDB server has to be defined."));
    }

    [Test]
    public void RequiredDatabase()
    {
        // ReSharper disable once ObjectCreationAsStatement
        // ReSharper disable once AssignNullToNotNullAttribute
        var ae = Assert.Throws<ArgumentException>(() => { new InfluxDBClient(host: "localhost", database: null); });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo("The database to be used for InfluxDB operations has to be defined."));
    }
}