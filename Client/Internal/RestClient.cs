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
            var contentType = result.Content?.Headers?.ContentType?.ToString();
            message = FormatErrorMessage(body, contentType);

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

    private static string? FormatErrorMessage(string body, string? contentType)
    {
        if (string.IsNullOrEmpty(body))
        {
            return null;
        }
        if (!string.IsNullOrEmpty(contentType) &&
            !contentType.StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        string? message = null;
        try
        {
            using var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(body));
            if (new DataContractJsonSerializer(typeof(ErrorBody)).ReadObject(memoryStream) is ErrorBody errorBody)
            {
                if (!string.IsNullOrEmpty(errorBody.Message)) // Cloud
                {
                    message = errorBody.Message;
                }
                else if ((errorBody.Data is not null) && !string.IsNullOrEmpty(errorBody.Data.ErrorMessage)) // v3/Core/Enterprise (legacy object form)
                {
                    message = errorBody.Data.ErrorMessage;
                }
                else if (!string.IsNullOrEmpty(errorBody.Error)) // v3/Core/Enterprise
                {
                    message = errorBody.Error;
                }
            }
        }
        catch (SerializationException se)
        {
            Debug.WriteLine($"Cannot parse error response as legacy JSON format: {body}. {se}");
        }

        try
        {
            using var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(body));
            if (new DataContractJsonSerializer(typeof(V3ErrorBody)).ReadObject(memoryStream) is V3ErrorBody v3ErrorBody)
            {
                var v3Message = BuildV3ErrorMessage(v3ErrorBody);
                var hasV3Details = v3ErrorBody.Data?.Any(detail => !string.IsNullOrEmpty(detail.ErrorMessage)) == true;
                if (!string.IsNullOrEmpty(v3Message) && (string.IsNullOrEmpty(message) || hasV3Details))
                {
                    message = v3Message;
                }
            }
        }
        catch (SerializationException se)
        {
            Debug.WriteLine($"Cannot parse error response as v3 JSON format: {body}. {se}");
        }

        return message;
    }

    private static string? BuildV3ErrorMessage(V3ErrorBody v3ErrorBody)
    {
        if (string.IsNullOrEmpty(v3ErrorBody.Error))
        {
            return null;
        }

        var message = new StringBuilder(v3ErrorBody.Error);
        var hasDetails = false;
        foreach (var detail in v3ErrorBody.Data ?? new List<V3ErrorBody.V3ErrorData>())
        {
            if (!string.IsNullOrEmpty(detail.ErrorMessage))
            {
                if (!hasDetails)
                {
                    message.Append(':');
                    hasDetails = true;
                }
                var lineNumber = detail.LineNumber?.ToString() ?? "?";
                message.Append($"\n\tline {lineNumber}: {detail.ErrorMessage}");
                if (!string.IsNullOrEmpty(detail.OriginalLine))
                    message.Append($" ({detail.OriginalLine})");
            }
        }
        return message.ToString();
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
internal class V3ErrorBody
{
    [DataMember(Name = "error")]
    public string? Error { get; set; }

    [DataMember(Name = "data")]
    public List<V3ErrorData>? Data { get; set; }

    [DataContract]
    internal class V3ErrorData
    {
        [DataMember(Name = "error_message")]
        public string? ErrorMessage { get; set; }

        [DataMember(Name = "line_number")]
        public int? LineNumber { get; set; }

        [DataMember(Name = "original_line")]
        public string? OriginalLine { get; set; }
    }
}

[DataContract]
internal class VersionBody
{
    [DataMember(Name = "version")]
    public string? Version { get; set; }
}
