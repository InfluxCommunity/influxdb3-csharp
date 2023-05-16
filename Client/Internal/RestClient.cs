using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using InfluxDB3.Client.Config;

namespace InfluxDB3.Client.Internal;

internal class RestClient
{
    private static readonly string[] ErrorHeaders =
        { "X-Platform-Error-Code", "X-Influx-Error", "X-InfluxDb-Error" };

    private readonly InfluxDBClientConfigs _configs;
    private readonly HttpClient _httpClient;

    internal RestClient(InfluxDBClientConfigs configs, HttpClient httpClient)
    {
        _configs = configs;
        _httpClient = httpClient;
    }

    internal async Task Request(string path, HttpMethod method, HttpContent? content = null,
        Dictionary<string, string?>? queryParams = null,
        CancellationToken cancellationToken = default)
    {
        var builder = new UriBuilder(new Uri($"{_configs.HostUrl}{path}"));
        if (queryParams is not null)
        {
            var query = queryParams
                .Select(param =>
                {
                    if (string.IsNullOrEmpty(param.Key) || string.IsNullOrEmpty(param.Value))
                    {
                        return "";
                    }
                    
                    var key = HttpUtility.UrlEncode(param.Key);
                    var value = HttpUtility.UrlEncode(param.Value ?? "");

                    return $"{key}={value}";
                })
                .Where(part => !string.IsNullOrEmpty(part));
            builder.Query = string.Join("&", query);
        }

        var request = new HttpRequestMessage
        {
            Method = method,
            RequestUri = builder.Uri,
            Content = content,
        };

        var result = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        if (!result.IsSuccessStatusCode)
        {
            string? message = null;
            var body = await result.Content.ReadAsStringAsync().ConfigureAwait(true);
            // error message in body
            if (!string.IsNullOrEmpty(body))
            {
                using var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(body));
                try
                {
                    if (new DataContractJsonSerializer(typeof(ErrorBody)).ReadObject(memoryStream) is ErrorBody
                        errorBody)
                    {
                        message = errorBody.Message;
                    }
                }
                catch (SerializationException se)
                {
                    Debug.WriteLine($"Cannot parse error response as JSON: {body}. {se}");
                }
            }

            // from header
            if (string.IsNullOrEmpty(message))
            {
                message = result.Headers?
                    .Where(header => ErrorHeaders.Contains(header.Key, StringComparer.OrdinalIgnoreCase))
                    .Select(header => header.Value.FirstOrDefault()?.ToString())
                    .FirstOrDefault();
            }

            // whole body
            if (string.IsNullOrEmpty(message))
            {
                message = body;
            }

            // reason
            if (string.IsNullOrEmpty(message))
            {
                message = result.ReasonPhrase;
            }

            throw new InfluxDBApiException(message ?? "Cannot write data to InfluxDB.", result);
        }
    }
}

[DataContract]
internal class ErrorBody
{
    [DataMember(Name = "message")] public string? Message { get; set; }
}