using System;
using System.Collections.Generic;
using InfluxDB3.Client.Test.Utils;

// ReSharper disable ObjectCreationAsStatement
// ReSharper disable AssignNullToNotNullAttribute

namespace InfluxDB3.Client.Test;

public class InfluxDBClientTest
{
    [Test]
    public void Create()
    {
        using var client = new InfluxDBClient("http://localhost:8086", token: "my-token", organization: "my-org",
            database: "my-database");

        Assert.That(client, Is.Not.Null);
    }

    [Test]
    public void CreateFromConnectionString()
    {
        using var client = new InfluxDBClient("http://localhost:8086?token=my-token&org=my-org&database=my-db");

        Assert.That(client, Is.Not.Null);
    }

    [Test]
    public void CreateFromEnv()
    {
        var env = new Dictionary<String, String>
        {
            { "INFLUX_HOST", "http://localhost:8086" },
            { "INFLUX_TOKEN", "my-token" },
            { "INFLUX_ORG", "my-org" },
            { "INFLUX_DATABASE", "my-database" },
        };
        TestUtils.SetEnv(env);

        using var client = new InfluxDBClient();

        Assert.That(client, Is.Not.Null);
    }

    [Test]
    public void IPv6()
    {
        var correctUrls = new List<String>();
        correctUrls.Add("http://[2001:db8::1]/");
        correctUrls.Add("http://[2001:db8:a0b:12f0::1]/index.html");
        correctUrls.Add("http://[2001:db8:a0b:12f0::1]:80/index.html");
        correctUrls.Add("https://[2001:db8:a0b:12f0::1%25eth0]:15000/");
        correctUrls.Add("http://[2607:f8b0:4005:802::1007]/");

        foreach (var url in correctUrls)
        {
            using var client = new InfluxDBClient(
                host: url,
                token: "my-token",
                organization: "my-org",
                database: "my-database"
            );
            Assert.That(client, Is.Not.Null);
        }

        var incorrectUrls = new List<String>();
        incorrectUrls.Add("http://2001:db8::1");
        incorrectUrls.Add("http://2001:db8::1:8080/");
        foreach (var url in incorrectUrls)
        {
            _ = Assert.Throws<UriFormatException>(() =>
            {
                new InfluxDBClient(host: url, token: "my-token", organization: "my-org", database: "my-database");
            });
        }
    }

    [Test]
    public void RequiredHost()
    {
        var ae = Assert.Throws<ArgumentException>(() => { new InfluxDBClient(host: null, token: ""); });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo("The URL of the InfluxDB server has to be defined"));
    }

    [TearDown]
    public void Cleanup()
    {
        TestUtils.CleanupEnv();
    }
}