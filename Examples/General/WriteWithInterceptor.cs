using System.Net.Http.Headers;
using InfluxDB3.Client;
using InfluxDB3.Client.Config;

namespace InfluxDB3.Examples.General;

public class WriteWithInterceptor
{
    static async Task Main(string[] args)
    {
        var headerInterceptor = new HeaderInterceptorHandler();
        var httpClient = new HttpClient(headerInterceptor);
        httpClient.Timeout = TimeSpan.FromSeconds(10);

        using var client = new InfluxDBClient(new ClientConfig
        {
            Host = Environment.GetEnvironmentVariable("INFLUXDB_URL") ??
                   "https://us-east-1-1.aws.cloud2.influxdata.com",
            Token = Environment.GetEnvironmentVariable("INFLUXDB_TOKEN") ?? "my-token",
            Database = Environment.GetEnvironmentVariable("INFLUXDB_DATABASE") ?? "my-database",
            HttpClient = httpClient,
        });

        await client.WriteRecordAsync("weather,type=test value=42.0");
        httpClient.Dispose();
    }

    public static async Task Run()
    {
        await Main(Array.Empty<string>());
    }
}

internal class HeaderInterceptorHandler() : DelegatingHandler(new HttpClientHandler())
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        var cacheControlHeaderValue = new CacheControlHeaderValue();
        cacheControlHeaderValue.MaxAge = TimeSpan.FromSeconds(10);
        request.Headers.CacheControl = cacheControlHeaderValue;
        var response = await base.SendAsync(request, cancellationToken);
        return response;
    }
}