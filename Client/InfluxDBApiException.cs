using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;

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

    public HttpResponseHeaders? Headers
    {
        get
        {
            return HttpResponseMessage?.Headers;
        }
    }

    public HttpStatusCode StatusCode
    {
        get
        {
            return HttpResponseMessage?.StatusCode ?? 0;
        }
    }

}