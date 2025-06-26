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

    private readonly ClientConfig _config;
    private readonly HttpClient _httpClient;

    internal RestClient(ClientConfig config, HttpClient httpClient)
    {
        _config = config;
        _httpClient = httpClient;
    }

    internal async Task<HttpResponseMessage> Request(string path, HttpMethod method, HttpContent? content = null,
        Dictionary<string, string?>? queryParams = null, Dictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default)
    {
        var builder = new UriBuilder(new Uri($"{_config.Host}{path}"));
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
        // add request headers
        if (headers is not null)
        {
            foreach (var header in headers)
            {
                request.Headers.Add(header.Key, header.Value);
            }
        }

        // add config headers
        if (_config.Headers != null)
        {
            foreach (var header in _config.Headers)
            {
                if (headers == null || !headers.ContainsKey(header.Key))
                {
                    request.Headers.Add(header.Key, header.Value);
                }
            }
        }

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
                        if (!string.IsNullOrEmpty(errorBody.Message)) // Cloud
                        {
                            message = errorBody.Message;
                        }
                        else if ((errorBody.Data is not null) && !string.IsNullOrEmpty(errorBody.Data.ErrorMessage)) // Edge
                        {
                            message = errorBody.Data.ErrorMessage;
                        }
                        else if (!string.IsNullOrEmpty(errorBody.Error)) // Edge
                        {
                            message = errorBody.Error;
                        }
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

        return result;
    }
}

[DataContract]
internal class ErrorBody
{
    [DataMember(Name = "message")]
    public string? Message { get; set; }

    [DataMember(Name = "error")]
    public string? Error { get; set; }

    [DataMember(Name = "data")]
    public ErrorData? Data { get; set; }

    [DataContract]
    internal class ErrorData
    {
        [DataMember(Name = "error_message")]
        public string? ErrorMessage { get; set; }
    }
}

[DataContract]
internal class VersionBody
{
    [DataMember(Name = "version")]
    public string? Version { get; set; }
}