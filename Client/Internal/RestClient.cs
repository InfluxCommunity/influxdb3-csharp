using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
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
            var body = await result.Content.ReadAsStringAsync().ConfigureAwait(false);
            var contentType = result.Content?.Headers?.ContentType?.ToString();
            var parsed = ParseErrorMessage(body, contentType);
            var message = parsed.Message;
            var partialLineErrors = parsed.PartialLineErrors;

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

            if (partialLineErrors?.Count > 0)
            {
                throw new InfluxDBPartialWriteException(message ?? "Cannot write data to InfluxDB.", result, partialLineErrors);
            }

            throw new InfluxDBApiException(message ?? "Cannot write data to InfluxDB.", result);
        }

        return result;
    }

    private static ParsedErrorMessage ParseErrorMessage(string body, string? contentType)
    {
        if (string.IsNullOrWhiteSpace(body))
        {
            return ParsedErrorMessage.Empty;
        }

        if (!string.IsNullOrEmpty(contentType) &&
            !contentType.StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
        {
            return ParsedErrorMessage.Empty;
        }

        try
        {
            using var document = JsonDocument.Parse(body);
            var root = document.RootElement;

            var cloudMessage = GetStringProperty(root, "message");
            var topLevelError = GetStringProperty(root, "error");
            var data = GetProperty(root, "data");

            var legacyDataMessage = GetLegacyDataErrorMessage(data);
            var message = cloudMessage ?? topLevelError ?? legacyDataMessage;
            var partial = ParsePartialWrite(topLevelError, data);
            if (partial is not null)
            {
                return partial;
            }

            return new ParsedErrorMessage(message, null);
        }
        catch (JsonException se)
        {
            Debug.WriteLine($"Cannot parse error response as JSON format: {body}. {se}");
            return ParsedErrorMessage.Empty;
        }
    }

    private static ParsedErrorMessage? ParsePartialWrite(string? topLevelError, JsonElement? data)
    {
        if (string.IsNullOrEmpty(topLevelError) || !IsPartialWriteError(topLevelError))
        {
            return null;
        }
        if (data is null)
        {
            return null;
        }

        if (data.Value.ValueKind == JsonValueKind.Array)
        {
            if (TryParseTypedLineErrors(data.Value, out var lineErrors))
            {
                return new ParsedErrorMessage(BuildPartialWriteMessage(topLevelError, lineErrors), lineErrors);
            }

            var details = data.Value.EnumerateArray()
                .Where(item => item.ValueKind != JsonValueKind.Null)
                .Select(ToDetailString)
                .Where(item => !string.IsNullOrEmpty(item))
                .ToList();

            if (details.Count > 0)
            {
                return new ParsedErrorMessage($"{topLevelError}:\n\t{string.Join("\n\t", details)}", null);
            }

            return null;
        }

        if (data.Value.ValueKind == JsonValueKind.Object)
        {
            if (TryParseTypedLineError(data.Value, out var lineError))
            {
                var lineErrors = new List<InfluxDBPartialWriteException.LineError> { lineError };
                return new ParsedErrorMessage(BuildPartialWriteMessage(topLevelError, lineErrors), lineErrors);
            }

            return null;
        }

        return null;
    }

    private static string BuildPartialWriteMessage(
        string baseMessage,
        IEnumerable<InfluxDBPartialWriteException.LineError> lineErrors)
    {
        var message = new StringBuilder(baseMessage);
        var hasDetails = false;
        foreach (var detail in lineErrors)
        {
            if (!hasDetails)
            {
                message.Append(':');
                hasDetails = true;
            }
            if (detail.LineNumber != 0 && !string.IsNullOrEmpty(detail.OriginalLine))
            {
                message.Append($"\n\tline {detail.LineNumber}: {detail.ErrorMessage} ({detail.OriginalLine})");
            }
            else if (!string.IsNullOrEmpty(detail.ErrorMessage))
            {
                message.Append($"\n\t{detail.ErrorMessage}");
            }
            else
            {
                message.Append($"\n\tline {detail.LineNumber}: {detail.ErrorMessage}");
            }
        }

        return message.ToString();
    }

    private static bool IsPartialWriteError(string topLevelError)
    {
        return topLevelError.IndexOf("partial write of line protocol occurred", StringComparison.OrdinalIgnoreCase) >= 0 ||
               topLevelError.IndexOf("parsing failed for write_lp endpoint", StringComparison.OrdinalIgnoreCase) >= 0;
    }

    private static bool TryParseTypedLineErrors(
        JsonElement data,
        out List<InfluxDBPartialWriteException.LineError> lineErrors)
    {
        lineErrors = new List<InfluxDBPartialWriteException.LineError>();
        if (data.ValueKind != JsonValueKind.Array)
        {
            return false;
        }

        foreach (var item in data.EnumerateArray())
        {
            if (!TryParseTypedLineError(item, out var lineError))
            {
                lineErrors.Clear();
                return false;
            }

            lineErrors.Add(lineError);
        }

        return lineErrors.Count > 0;
    }

    private static bool TryParseTypedLineError(JsonElement item, out InfluxDBPartialWriteException.LineError lineError)
    {
        lineError = null!;
        if (item.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        var errorMessage = GetProperty(item, "error_message");
        var lineNumber = GetProperty(item, "line_number");
        var originalLine = GetProperty(item, "original_line");
        if (errorMessage?.ValueKind != JsonValueKind.String || string.IsNullOrEmpty(errorMessage.Value.GetString()))
        {
            return false;
        }

        var number = 0;
        if (lineNumber is not null)
        {
            if (lineNumber.Value.ValueKind != JsonValueKind.Number || !lineNumber.Value.TryGetInt32(out number))
            {
                return false;
            }
        }

        string? original = null;
        if (originalLine is not null)
        {
            if (originalLine.Value.ValueKind != JsonValueKind.String)
            {
                return false;
            }

            original = originalLine.Value.GetString();
        }

        lineError = new InfluxDBPartialWriteException.LineError(
            number,
            errorMessage.Value.GetString()!,
            original);
        return true;
    }

    private static JsonElement? GetProperty(JsonElement element, string propertyName)
    {
        return element.TryGetProperty(propertyName, out var value) ? value : null;
    }

    private static string? GetStringProperty(JsonElement element, string propertyName)
    {
        return GetProperty(element, propertyName) is { ValueKind: JsonValueKind.String } value ? value.GetString() : null;
    }

    private static string? GetLegacyDataErrorMessage(JsonElement? data)
    {
        if (data is null || data.Value.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        var errorMessage = GetProperty(data.Value, "error_message");
        return errorMessage?.ValueKind == JsonValueKind.String ? errorMessage?.GetString() : null;
    }

    private static string ToDetailString(JsonElement element)
    {
        return element.ValueKind == JsonValueKind.String ? (element.GetString() ?? "") : element.GetRawText();
    }

    private sealed class ParsedErrorMessage
    {
        internal static readonly ParsedErrorMessage Empty = new(null, null);

        internal ParsedErrorMessage(string? message, List<InfluxDBPartialWriteException.LineError>? partialLineErrors)
        {
            Message = message;
            PartialLineErrors = partialLineErrors;
        }

        internal string? Message { get; }
        internal List<InfluxDBPartialWriteException.LineError>? PartialLineErrors { get; }
    }
}

[DataContract]
internal class VersionBody
{
    [DataMember(Name = "version")]
    public string? Version { get; set; }
}
