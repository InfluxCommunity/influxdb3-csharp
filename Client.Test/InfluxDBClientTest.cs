using System;
using System.Collections.Generic;
using InfluxDB3.Client.Config;

// ReSharper disable ObjectCreationAsStatement
// ReSharper disable AssignNullToNotNullAttribute

namespace InfluxDB3.Client.Test;

public class InfluxDBClientTest
{
    [Test]
    public void Create()
    {
        using var client = new InfluxDBClient("http://localhost:8086", token: "my-token", organization: "my-org", database: "my-database");

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
            {"INFLUX_HOST", "http://localhost:8086"},
            {"INFLUX_TOKEN", "my-token"},
            {"INFLUX_ORG", "my-org"},
            {"INFLUX_DATABASE", "my-database"},
        };
        SetEnv(env);

        using var client = new InfluxDBClient();

        Assert.That(client, Is.Not.Null);
    }

    [Test]
    public void CreateFromConfigString()
    {
        var clientConfig = new ClientConfig("http://localhost:8086?token=my-token&org=my-org&database=my-db");
        using var client = new InfluxDBClient(clientConfig);
        Assert.That(client, Is.Not.Null);
    }

    [Test]
    public void CreateFromConfigEnv()
    {
        var env = new Dictionary<String, String>
        {
            {"INFLUX_HOST", "http://localhost:8086"},
            {"INFLUX_TOKEN", "my-token"},
            {"INFLUX_ORG", "my-org"},
            {"INFLUX_DATABASE", "my-database"},
        };
        var clientConfig = new ClientConfig(env);
        using var client = new InfluxDBClient(clientConfig);
        Assert.That(client, Is.Not.Null);
    }

    [Test]
    public void RequiredHost()
    {
        var ae = Assert.Throws<ArgumentException>(() => { new InfluxDBClient(host: null, token: ""); });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo("The URL of the InfluxDB server has to be defined"));
    }

    private static void SetEnv(IDictionary<String, String> dict)
    {
        foreach (var entry in dict)
        {
            Environment.SetEnvironmentVariable(entry.Key, entry.Value, EnvironmentVariableTarget.Process);
        }
    }

    [TearDown]
    public void Cleanup()
    {
        var env = Environment.GetEnvironmentVariables(EnvironmentVariableTarget.Process);
        foreach (var key in env.Keys)
        {
            if (((string)key).StartsWith("INFLUX_"))
            {
                Environment.SetEnvironmentVariable((string)key, null, EnvironmentVariableTarget.Process);
            }
        }
    }
}