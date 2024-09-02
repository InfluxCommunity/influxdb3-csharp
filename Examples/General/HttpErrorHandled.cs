using System.Collections.Frozen;
using InfluxDB3.Client;
using Console = System.Console;

namespace InfluxDB3.Examples.General;

public class HttpErrorHandled
{
    public static async Task Run()
    {
        Console.WriteLine("HttpErrorHandled");
        using var client = new InfluxDBClient(host: Runner.Host, 
            token: Runner.Token, 
            database: Runner.Database);

        Console.WriteLine("Writing record");

        try
        {
            await client.WriteRecordAsync("vehicle,id=harfa vel=89.7,load=355i,state=");
        }
        catch (Exception ex)
        {
            if (ex is InfluxDBApiException)
            {
                InfluxDBApiException apiEx = (InfluxDBApiException)ex;
                Console.WriteLine("Caught ApiException: {0} \"{1}\"", 
                    apiEx.GetStatusCode(), apiEx.Message);
                var headers = apiEx.GetHeaders().ToFrozenDictionary();
                Console.WriteLine("Headers:");
                foreach (var header in headers)
                {
                    Console.WriteLine("   {0}: {1}", header.Key, header.Value.First());
                }
            }
            else
            {
                throw new Exception($"Unexpected Exception: {ex.Message}", ex);
            }
        }
    }
}