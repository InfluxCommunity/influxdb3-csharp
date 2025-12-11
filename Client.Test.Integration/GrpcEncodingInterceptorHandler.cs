using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace InfluxDB3.Client.Test.Integration;

/// <summary>
/// Custom DelegatingHandler to intercept HTTP requests and responses and capture grpc encoding headers.
/// </summary>
internal class GrpcEncodingInterceptorHandler : DelegatingHandler
{
    /// <summary>
    /// Captured grpc-accept-encoding request header values.
    /// </summary>
    public List<string> GrpcAcceptEncodingValues { get; } = new();

    /// <summary>
    /// Captured grpc-encoding response header values.
    /// </summary>
    public List<string> GrpcEncodingValues { get; } = new();

    public GrpcEncodingInterceptorHandler() : base(new HttpClientHandler
    {
        ServerCertificateCustomValidationCallback = (_, _, _, _) => true
    })
    {
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        // Capture grpc-accept-encoding header from request
        if (request.Headers.TryGetValues("grpc-accept-encoding", out var requestValues))
        {
            GrpcAcceptEncodingValues.AddRange(requestValues);
        }

        var response = await base.SendAsync(request, cancellationToken);

        // Capture grpc-encoding header from response
        if (response.Headers.TryGetValues("grpc-encoding", out var responseValues))
        {
            GrpcEncodingValues.AddRange(responseValues);
        }

        return response;
    }

    /// <summary>
    /// Gets the last captured grpc-accept-encoding request header value (comma-separated).
    /// Trailing commas are trimmed.
    /// </summary>
    public string? LastGrpcAcceptEncoding => GrpcAcceptEncodingValues.LastOrDefault()?.TrimEnd(',');

    /// <summary>
    /// Gets the last captured grpc-encoding response header value.
    /// </summary>
    public string? LastGrpcEncoding => GrpcEncodingValues.LastOrDefault();
}