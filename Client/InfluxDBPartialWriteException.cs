using System.Collections.Generic;
using System.Net.Http;

namespace InfluxDB3.Client;

/// <summary>
/// API exception with structured per-line details for v3 partial-write errors.
/// </summary>
public class InfluxDBPartialWriteException : InfluxDBApiException
{
    public sealed class LineError
    {
        public LineError(int lineNumber, string errorMessage, string? originalLine)
        {
            LineNumber = lineNumber;
            ErrorMessage = errorMessage;
            OriginalLine = originalLine;
        }

        public int LineNumber { get; }

        public string ErrorMessage { get; }

        public string? OriginalLine { get; }
    }

    internal InfluxDBPartialWriteException(
        string message,
        HttpResponseMessage httpResponseMessage,
        IReadOnlyList<LineError> lineErrors) : base(message, httpResponseMessage)
    {
        LineErrors = lineErrors;
    }

    /// <summary>
    /// Structured line-level errors returned by the v3 write endpoint.
    /// </summary>
    public IReadOnlyList<LineError> LineErrors { get; }
}
