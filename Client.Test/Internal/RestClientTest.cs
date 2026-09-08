using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Internal;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;

namespace InfluxDB3.Client.Test.Internal;

public class RestClientTest : MockServerTest
{
    private RestClient _client;
    private HttpClient _httpClient;

    [TearDown]
    public new void TearDown()
    {
        _httpClient?.Dispose();
    }

    [Test]
    public async Task Authorization()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token"
        });
        await DoRequest();

        var requests = MockServer.LogEntries.ToList();

        Assert.That(requests[0].RequestMessage.Headers?["Authorization"][0], Is.EqualTo("Token my-token"));
    }

    [Test]
    public async Task AuthorizationCustomScheme()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
            Token = "my-token",
            AuthScheme = "my-scheme"
        });
        await DoRequest();

        var requests = MockServer.LogEntries.ToList();

        Assert.That(requests[0].RequestMessage.Headers?["Authorization"][0], Is.EqualTo("my-scheme my-token"));
    }

    [Test]
    public async Task UserAgent()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });
        await DoRequest();

        var requests = MockServer.LogEntries.ToList();

        Assert.That(requests[0].RequestMessage.Headers?["User-Agent"][0], Does.StartWith("influxdb3-csharp/1."));
        Assert.That(requests[0].RequestMessage.Headers?["User-Agent"][0], Does.EndWith(".0.0"));
    }

    [Test]
    public async Task Url()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });
        await DoRequest();

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Url, Is.EqualTo($"{MockServerUrl}/api"));
    }

    [Test]
    public async Task UrlWithBackslash()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = $"{MockServerUrl}/",
        });
        await DoRequest();

        var requests = MockServer.LogEntries.ToList();
        Assert.That(requests[0].RequestMessage.Url, Is.EqualTo($"{MockServerUrl}/api"));
    }

    private async Task DoRequest()
    {
        MockServer
            .Given(Request.Create().WithPath("/api").UsingGet())
            .RespondWith(Response.Create().WithStatusCode(204));

        await _client.Request("api", HttpMethod.Get);
    }

    [Test]
    public void ErrorHeader()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("X-Influx-Error", "line protocol poorly formed and no points were written")
                .WithStatusCode(400));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.HttpResponseMessage, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo("line protocol poorly formed and no points were written"));
        });
    }

    [Test]
    public void ErrorBody()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithBody("no token was sent and they are required")
                .WithStatusCode(403));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.HttpResponseMessage, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo("no token was sent and they are required"));
        });
    }

    [Test]
    public void ErrorJsonBodyCloud()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("Content-Type", "application/json")
                .WithHeader("X-Influx-Error", "not used")
                .WithBody("{\"message\":\"token does not have sufficient permissions\"}")
                .WithStatusCode(401));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.HttpResponseMessage, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo("token does not have sufficient permissions"));
        });
    }

    [Test]
    public void ErrorJsonBodyIgnoredForNonJsonContentType()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("Content-Type", "text/plain")
                .WithBody("{\"message\":\"token does not have sufficient permissions\"}")
                .WithStatusCode(401));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.HttpResponseMessage, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo("{\"message\":\"token does not have sufficient permissions\"}"));
        });
    }

    [Test]
    public void ErrorJsonBodyV3WithDataObject()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("Content-Type", "application/json")
                .WithHeader("X-Influx-Error", "not used")
                .WithBody("{\"error\":\"parsing failed\",\"data\":" +
                          "{\"error_message\":\"invalid field value in line protocol for field 'value' on line 0\"}}")
                .WithStatusCode(401));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.HttpResponseMessage, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo(
                "parsing failed:\n\tinvalid field value in line protocol for field 'value' on line 0"));
        });
    }

    [Test]
    public void ErrorJsonBodyV3WithoutData()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("Content-Type", "application/json")
                .WithHeader("X-Influx-Error", "not used")
                .WithBody("{\"error\":\"token does not have sufficient permissions\"}")
                .WithStatusCode(401));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.HttpResponseMessage, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo("token does not have sufficient permissions"));
        });
    }

    [Test]
    public void ErrorJsonBodyV3WithDataErrorMessageCombinesMessages()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("Content-Type", "application/json")
                .WithBody("{\"error\":\"parsing failed\",\"data\":{\"error_message\":\"invalid field value\"}}")
                .WithStatusCode(400));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae!.Message, Is.EqualTo("parsing failed:\n\tinvalid field value"));
    }

    [Test]
    public void ErrorJsonBodyWithNonObjectRootFallsBackToHeaders()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("Content-Type", "application/json")
                .WithHeader("X-Influx-Error", "fallback header message")
                .WithBody("[1,2,3]")
                .WithStatusCode(400));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae!.Message, Is.EqualTo("fallback header message"));
    }

    [Test]
    public void ErrorPartialJsonBodyV3WithDataArray()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api/v3/write_lp").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("Content-Type", "application/json")
                .WithHeader("X-Influx-Error", "not used")
                .WithBody("{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":\"invalid column type for column 'temp', expected iox::column_type::field::float, got iox::column_type::field::string\",\"line_number\":2,\"original_line\":\"home,room=Sunroom temp=hi 1735549200\"}]}")
                .WithStatusCode(400));

        var ae = Assert.ThrowsAsync<InfluxDBPartialWriteException>(async () =>
        {
            var queryParams = new Dictionary<string, string>();
            queryParams.Add("accept_partial", true.ToString().ToLowerInvariant());
            await _client.Request("api/v3/write_lp", HttpMethod.Post, null, queryParams);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.LineErrors, Has.Count.EqualTo(1));
            Assert.That(ae.LineErrors[0].LineNumber, Is.EqualTo(2));
            Assert.That(ae.LineErrors[0].ErrorMessage, Is.EqualTo("invalid column type for column 'temp', expected iox::column_type::field::float, got iox::column_type::field::string"));
            Assert.That(ae.LineErrors[0].OriginalLine, Is.EqualTo("home,room=Sunroom temp=hi 1735549200"));
            Assert.That(ae.HttpResponseMessage, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo("partial write of line protocol occurred:\n\tline 2: invalid column type for column 'temp', expected iox::column_type::field::float, got iox::column_type::field::string (home,room=Sunroom temp=hi 1735549200)"));
        });
    }

    [Test]
    public void ErrorPartialJsonBodyV3WithDataArrayUntypedFallback()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api/v3/write_lp").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("Content-Type", "application/json")
                .WithBody("{\"error\":\"partial write of line protocol occurred\",\"data\":[\"bad line\",true,3]}")
                .WithStatusCode(400));

        var ae = Assert.ThrowsAsync<InfluxDBPartialWriteException>(async () =>
        {
            var queryParams = new Dictionary<string, string>();
            queryParams.Add("accept_partial", true.ToString().ToLowerInvariant());
            await _client.Request("api/v3/write_lp", HttpMethod.Post, null, queryParams);
        });

        Assert.That(ae.Message, Is.EqualTo("partial write of line protocol occurred:\n\t\"bad line\"\n\ttrue\n\t3"));
    }

    [Test]
    public void ErrorJsonBodyV3ParsingFailedWriteLpWithDataObject()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("Content-Type", "application/json")
                .WithBody("{\"error\":\"parsing failed for write_lp endpoint\",\"data\":{\"error_message\":\"invalid field value\",\"line_number\":2,\"original_line\":\"home,room=Sunroom temp=hi 1735549200\"}}")
                .WithStatusCode(400));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo("parsing failed for write_lp endpoint:\n\tline 2: invalid field value (home,room=Sunroom temp=hi 1735549200)"));
        });
    }

    [Test]
    public void ErrorJsonBodyV3ParsingFailedWriteLpWithInvalidLineNumberDataObject()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("Content-Type", "application/json")
                .WithBody(
                    "{\"error\":\"parsing failed for write_lp endpoint\",\"data\":{\"error_message\":\"invalid field value\",\"line_number\":\"aa\",\"original_line\":\"home,room=Sunroom temp=hi 1735549200\"}}")
                .WithStatusCode(400));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo("parsing failed for write_lp endpoint:\n\tinvalid field value"));
        });
    }
    
    [Test]
    public void ErrorJsonBodyV3PartialWriteWithDataObjectErrorMessageOnly()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api/v3/write_lp").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("Content-Type", "application/json")
                .WithBody("{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":\"invalid field value\"}]}")
                .WithStatusCode(400));

        var ae = Assert.ThrowsAsync<InfluxDBPartialWriteException>(async () =>
        {
            var queryParams = new Dictionary<string, string>();
            queryParams.Add("accept_partial", true.ToString().ToLowerInvariant());
            await _client.Request("api/v3/write_lp", HttpMethod.Post, null, queryParams);
        });

        using (Assert.EnterMultipleScope())
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.LineErrors, Has.Count.EqualTo(1));
            Assert.That(ae.LineErrors[0].LineNumber, Is.Null);
            Assert.That(ae.LineErrors[0].ErrorMessage, Is.EqualTo("invalid field value"));
            Assert.That(ae.LineErrors[0].OriginalLine, Is.Null);
            Assert.That(ae.Message, Is.EqualTo("partial write of line protocol occurred:\n\tinvalid field value"));
        }
    }

    [Test]
    public void ErrorJsonBodyV3PartialWriteWithLineNumberWithoutOriginalLine()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api/v3/write_lp").UsingPost())
            .RespondWith(Response.Create()
                .WithHeader("Content-Type", "application/json")
                .WithBody("{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":\"invalid field value\",\"line_number\":2}]}")
                .WithStatusCode(400));

        var ae = Assert.ThrowsAsync<InfluxDBPartialWriteException>(async () =>
        {
            var queryParams = new Dictionary<string, string>();
            queryParams.Add("accept_partial", true.ToString().ToLowerInvariant());
            await _client.Request("api/v3/write_lp", HttpMethod.Post, null, queryParams);
        });

        using (Assert.EnterMultipleScope())
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.LineErrors, Has.Count.EqualTo(1));
            Assert.That(ae.LineErrors[0].LineNumber, Is.EqualTo(2));
            Assert.That(ae.LineErrors[0].ErrorMessage, Is.EqualTo("invalid field value"));
            Assert.That(ae.LineErrors[0].OriginalLine, Is.Null);
            Assert.That(ae.Message, Is.EqualTo("partial write of line protocol occurred:\n\tline 2: invalid field value"));
        }
    }

    [Test]
    public void ErrorReason()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
        });

        MockServer
            .Given(Request.Create().WithPath("/api").UsingPost())
            .RespondWith(Response.Create()
                .WithStatusCode(409));

        var ae = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
        {
            await _client.Request("api", HttpMethod.Post);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.HttpResponseMessage, Is.Not.Null);
            Assert.That(ae.Message, Is.EqualTo("Conflict"));
        });
    }

    [Test]
    public void AllowHttpRedirects()
    {
        CreateAndConfigureRestClient(new ClientConfig
        {
            Host = MockServerUrl,
            AllowHttpRedirects = true
        });

        Assert.That(_client, Is.Not.Null);
    }

    private const string RejectedLine = "home,room=Sunroom temp=\"hi\" 1735545610";
    private const string RejectedLineJson = "home,room=Sunroom temp=\\\"hi\\\" 1735545610";

    private const string LineError = "invalid column type for column 'temp', expected " +
                                     "iox::column_type::field::float, got iox::column_type::field::string";

    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":\"" + LineError +
        "\",\"line_number\":2,\"original_line\":\"" + RejectedLineJson + "\"}]}",
        false, true, "partial write of line protocol occurred:\n\tline 2: " + LineError + " (" + RejectedLine + ")",
        true,
        2, LineError, RejectedLine,
        TestName = "V3 accept partial with renamed error and non-empty array")]
    [TestCase(HttpStatusCode.BadRequest, null,
        "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":\"" + LineError +
        "\",\"line_number\":2,\"original_line\":\"" + RejectedLineJson + "\"}]}",
        false, true, "partial write of line protocol occurred:\n\tline 2: " + LineError + " (" + RejectedLine + ")",
        true,
        2, LineError, RejectedLine,
        TestName = "V3 accept partial without content type")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"line_number\":\"invalid\",\"original_line\":\"" +
        RejectedLineJson + "\"}]}",
        false, true,
        "partial write of line protocol occurred:\n\t{\"line_number\":\"invalid\",\"original_line\":\"" +
        RejectedLineJson + "\"}", true,
        TestName = "V3 accept partial with malformed non-empty array")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"partial write of line protocol occurred\",\"data\":[1,{\"error_message\":\"" + LineError +
        "\",\"line_number\":2,\"original_line\":\"" + RejectedLineJson + "\"}]}",
        false, true,
        "partial write of line protocol occurred:\n\t1\n\t{\"error_message\":\"" + LineError +
        "\",\"line_number\":2,\"original_line\":\"" + RejectedLineJson + "\"}", true,
        2, LineError, RejectedLine,
        TestName = "V3 accept partial with mixed primitive and typed entries")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"partial write of line protocol occurred\",\"data\":[\"" + RejectedLineJson + "\"]}",
        false, true, "partial write of line protocol occurred:\n\t\"" + RejectedLineJson + "\"", true,
        TestName = "V3 accept partial with string entries")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":\"" + LineError + "\"}]}",
        false, true, "partial write of line protocol occurred:\n\t" + LineError, true,
        null, LineError, null,
        TestName = "V3 accept partial with error message only")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":\"" + LineError +
        "\",\"line_number\":2}]}",
        false, true, "partial write of line protocol occurred:\n\tline 2: " + LineError, true,
        2, LineError, null,
        TestName = "V3 accept partial with line number but no original line")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"line_number\":2,\"original_line\":\"" +
        RejectedLineJson + "\"}]}",
        false, true,
        "partial write of line protocol occurred:\n\t{\"line_number\":2,\"original_line\":\"" + RejectedLineJson +
        "\"}", true,
        TestName = "V3 accept partial with entry missing error message")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"write failed\",\"data\":[]}",
        false, true, "write failed", false,
        TestName = "V3 accept partial with empty array")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"partial write of line protocol occurred\",\"data\":{\"error_message\":\"" + LineError +
        "\",\"line_number\":2,\"original_line\":\"" + RejectedLineJson + "\"}}",
        false, true, "partial write of line protocol occurred:\n\tline 2: " + LineError + " (" + RejectedLine + ")",
        false,
        TestName = "V3 accept partial with object details remains generic")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"line protocol parsing error\",\"data\":{\"error_message\":\"" + LineError +
        "\",\"line_number\":2,\"original_line\":\"" + RejectedLineJson + "\"}}",
        false, false, "line protocol parsing error:\n\tline 2: " + LineError + " (" + RejectedLine + ")", false,
        TestName = "V3 reject partial with object details")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":\"" + LineError +
        "\",\"line_number\":2,\"original_line\":\"" + RejectedLineJson + "\"}]}",
        true, true, "partial write of line protocol occurred", false,
        TestName = "V2 never returns partial write error")]
    [TestCase(HttpStatusCode.InternalServerError, "application/json",
        "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":\"" + LineError +
        "\",\"line_number\":2,\"original_line\":\"" + RejectedLineJson + "\"}]}",
        false, true, "partial write of line protocol occurred", false,
        TestName = "V3 non-400 never returns partial write error")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"write failed\",\"data\":\"invalid\"}",
        false, true, "write failed", false,
        TestName = "V3 scalar data remains generic")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"write failed\",\"data\":{}}",
        false, true, "write failed", false,
        TestName = "V3 empty object data remains generic")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"write failed\",\"data\":null}",
        false, true, "write failed", false,
        TestName = "V3 null data remains generic")]
    [TestCase(HttpStatusCode.BadRequest, "application/json",
        "{\"error\":\"write failed\"",
        false, true, "{\"error\":\"write failed\"", false,
        TestName = "V3 malformed JSON preserves raw response")]
    public Task TestWriteErrorClassification(
        HttpStatusCode statusCode,
        string? contentType,
        string responseBody,
        bool useV2Api,
        bool acceptPartial,
        string expectedMsg,
        bool expectPartial,
        int? expectedLineNumber = null,
        string? expectedErrorMessage = null,
        string? expectedOriginalLine = null)
    {
        try
        {
            CreateAndConfigureRestClient(new ClientConfig
            {
                Host = MockServerUrl,
            });

            var path = useV2Api ? "api/v2/write_lp" : "api/v3/write_lp";
            var response = Response.Create()
                .WithStatusCode(statusCode)
                .WithBody(responseBody);
            if (contentType != null)
            {
                response.WithHeader("Content-Type", contentType);
            }

            MockServer
                .Given(Request.Create().WithPath("/" + path).UsingPost())
                .RespondWith(response);

            var queryParams = new Dictionary<string, string>();
            queryParams.Add("accept_partial", acceptPartial.ToString().ToLowerInvariant());

            if (expectPartial)
            {
                var ex = Assert.ThrowsAsync<InfluxDBPartialWriteException>(async () =>
                {
                    await _client.Request(path, HttpMethod.Post, null, queryParams);
                });
                Assert.That(ex, Is.Not.Null);
                Assert.That(ex!.Message, Is.EqualTo(expectedMsg));
                if (expectedErrorMessage != null || expectedLineNumber != null || expectedOriginalLine != null)
                {
                    using (Assert.EnterMultipleScope())
                    {
                        Assert.That(ex.LineErrors[0].LineNumber, Is.EqualTo(expectedLineNumber));
                        Assert.That(ex.LineErrors[0].ErrorMessage, Is.EqualTo(expectedErrorMessage));
                        Assert.That(ex.LineErrors[0].OriginalLine, Is.EqualTo(expectedOriginalLine));
                    }
                }
            }
            else
            {
                var ex = Assert.ThrowsAsync<InfluxDBApiException>(async () =>
                {
                    await _client.Request(path, HttpMethod.Post, null, queryParams);
                });
                Assert.That(ex, Is.InstanceOf<InfluxDBApiException>());
                Assert.That(ex!.Message, Is.EqualTo(expectedMsg));
            }

            return Task.CompletedTask;
        }
        catch (Exception exception)
        {
            return Task.FromException(exception);
        }
    }

    private void CreateAndConfigureRestClient(ClientConfig config)
    {
        _httpClient = InfluxDBClient.CreateOrGetHttpClient(config);
        _client = new RestClient(config, _httpClient);
    }

    private static T GetDeclaredField<T>(IReflect type, object instance, string fieldName)
    {
        const BindingFlags bindFlags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic
                                       | BindingFlags.Static | BindingFlags.DeclaredOnly;
        var field = type.GetField(fieldName, bindFlags);
        return (T)field?.GetValue(instance);
    }
}
