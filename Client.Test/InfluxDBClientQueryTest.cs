using System;

namespace InfluxDB3.Client.Test;

public class InfluxDBClientQueryTest : MockServerTest
{
    private InfluxDBClient _client;

    [TearDown]
    public new void TearDown()
    {
        _client?.Dispose();
    }

    [Test]
    public void AlreadyDisposed()
    {
        _client = new InfluxDBClient(MockServerUrl);
        _client.Dispose();
        var ae = Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await foreach (var unused in _client.Query("SELECT 1"))
            {
            }
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo($"Cannot access a disposed object.{Environment.NewLine}Object name: 'InfluxDBClient'."));
    }

    [Test]
    public void NotSpecifiedDatabase()
    {
        _client = new InfluxDBClient(MockServerUrl);
        var ae = Assert.Throws<InvalidOperationException>(() => { _client.QueryBatches("SELECT 1"); });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo("Please specify the 'database' as a method parameter or use default configuration at 'ClientConfig.Database'."));
    }
}