using System;
using System.Linq;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace InfluxDB3.Client.Test.Utils.FlightMock;

public class TestWebFactory : IDisposable
{
    private readonly IHost _host;
    private readonly int _port;

    public TestWebFactory(SimpleProducer simpleProducer)
    {
        _host = WebHostBuilder(simpleProducer).Build(); //Create the server
        _host.Start();
        var addressInfo = _host.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        if (addressInfo == null)
        {
            throw new Exception("No address info could be found for configured server");
        }

        var address = addressInfo.Addresses.First();
        var addressUri = new Uri(address);
        _port = addressUri.Port;
        AppContext.SetSwitch(
            "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    private static IHostBuilder WebHostBuilder(SimpleProducer simpleProducer)
    {
        return Host.CreateDefaultBuilder()
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder
                    .ConfigureKestrel(c => { c.ListenAnyIP(0, l => l.Protocols = HttpProtocols.Http2); })
                    .UseStartup<Startup>()
                    .ConfigureServices(services => { services.AddSingleton(simpleProducer); });
                ;
            });
    }

    public string GetAddress()
    {
        return $"http://127.0.0.1:{_port}";
    }

    private void Stop()
    {
        _host.StopAsync().Wait();
    }

    public void Dispose()
    {
        Stop();
    }
}