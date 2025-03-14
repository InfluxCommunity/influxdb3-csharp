# Examples

- [General](General/Runner.cs) - for running other examples from the commandline.
- [HttpErrorHandled](General/HttpErrorHandled.cs) - Accessing HTTP headers when an InfluxDBApiException is thrown.
- [IOxExample](IOx/IOxExample.cs) - How to use write and query data from InfluxDB IOx
- [Downsampling](Downsampling/DownsamplingExample.cs) - How to use queries to structure data for downsampling
- [CustomSslCerts](CustomSslCerts/CustomSslCertsExample.cs) - How to configure custom SSL root certificates and proxy in
  client

## General Runner

Examples can be run from the directory `Examples/General` by simply calling `dotnet run`

For example:

```bash
Examples/General/$ dotnet run "HttpErrorHandled"
```
