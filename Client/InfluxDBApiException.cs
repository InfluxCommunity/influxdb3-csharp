using System;
using System.Net.Http;

namespace InfluxDB3.Client;

/// <summary>
/// The generic API exception.
/// </summary>
public class InfluxDBApiException : Exception
{
    internal InfluxDBApiException(string message, HttpResponseMessage httpResponseMessage) : base(message)
    {
        HttpResponseMessage = httpResponseMessage;
    }

    public HttpResponseMessage? HttpResponseMessage { get; private set; }
}