<p align="center">
    <img src="net_logo.svg" alt=".NET Logo" width="150px">
</p>
<p align="center">
    <a href="https://www.nuget.org/packages/InfluxDB3.Client">
        <img src="https://buildstats.info/nuget/InfluxDB3.Client" alt="NuGet Badge">
    </a>
    <a href="https://influxcommunity.github.io/influxdb3-csharp/">
        <img src="https://img.shields.io/badge/-docfx-blue?logo=csharp&logoColor=white" alt="docfx">
    </a>
    <a href="https://github.com/InfluxCommunity/influxdb3-csharp/actions/workflows/codeql-analysis.yml">
        <img src="https://github.com/InfluxCommunity/influxdb3-csharp/actions/workflows/codeql-analysis.yml/badge.svg?branch=main" alt="CodeQL analysis">
    </a>
    <a href="https://github.com/InfluxCommunity/influxdb3-csharp/actions/workflows/linter.yml">
        <img src="https://github.com/InfluxCommunity/influxdb3-csharp/actions/workflows/linter.yml/badge.svg" alt="Lint Code Base">
    </a>
    <a href="https://dl.circleci.com/status-badge/redirect/gh/InfluxCommunity/influxdb3-csharp/tree/main">
        <img src="https://dl.circleci.com/status-badge/img/gh/InfluxCommunity/influxdb3-csharp/tree/main.svg?style=svg" alt="CircleCI">
    </a>
    <a href="https://codecov.io/gh/InfluxCommunity/influxdb3-csharp">
        <img src="https://codecov.io/gh/InfluxCommunity/influxdb3-csharp/branch/main/graph/badge.svg" alt="Code Cov"/>
    </a>
    <a href="https://app.slack.com/huddle/TH8RGQX5Z/C02UDUPLQKA">
        <img src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" alt="Community Slack">
    </a>
</p>

# InfluxDB 3 C# .NET Client

The C# .NET client that provides an easy and convenient way to interact with InfluxDB 3.
This package supports both writing data to InfluxDB and querying data using the FlightSQL client,
which allows you to execute SQL queries against InfluxDB IOx.

We offer this [Getting Started: InfluxDB 3.0 C# Client Library](https://www.youtube.com/watch?v=l2e4lXilvLA) video to learn more about the library.

## Installation

Add the latest version of the client to your project:

```sh
dotnet add package InfluxDB3.Client
```

## Usage

To start with the client, import the `InfluxDB3.Client` package and create a `InfluxDBClient` by constructor initializer:

```csharp
using System.Threading.Tasks;
using InfluxDB3.Client;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Examples.IOx;

public class IOxExample
{
    static async Task Main(string[] args)
    {
        const string host = "https://us-east-1-1.aws.cloud2.influxdata.com";
        const string token = "my-token";
        const string database = "my-database";

        using var client = new InfluxDBClient(host, token: token, database: database);
    }
}
```

to insert data, you can use code like this:

```csharp
//
// Write by Point
//
var point = PointData.Measurement("temperature")
    .SetTag("location", "west")
    .SetField("value", 55.15)
    .SetTimestamp(DateTime.UtcNow.AddSeconds(-10));
await client.WritePointAsync(point: point);

//
// Write by LineProtocol
//
const string record = "temperature,location=north value=60.0";
await client.WriteRecordAsync(record: record);
```

to query your data, you can use code like this:

```csharp
//
// Query by SQL
//
const string sql = "select time,location,value from temperature order by time desc limit 10";
Console.WriteLine("{0,-30}{1,-15}{2,-15}", "time", "location", "value");
await foreach (var row in client.Query(query: sql))
{
    Console.WriteLine("{0,-30}{1,-15}{2,-15}", row[0], row[1], row[2]);
}
Console.WriteLine();

//
// Query by parametrized SQL
//
const string sqlParams = "select time,location,value from temperature where location=$location order by time desc limit 10";
Console.WriteLine("Query by parametrized SQL");
Console.WriteLine("{0,-30}{1,-15}{2,-15}", "time", "location", "value");
await foreach (var row in client.Query(query: sqlParams, namedParameters: new Dictionary<string, object> { { "location", "west" } }))
{
    Console.WriteLine("{0,-30}{1,-15}{2,-15}", row[0], row[1], row[2]);
}
Console.WriteLine();

//
// Query by InfluxQL
//
const string influxQL =
    "select MEAN(value) from temperature group by time(1d) fill(none) order by time desc limit 10";
Console.WriteLine("{0,-30}{1,-15}", "time", "mean");
await foreach (var row in client.Query(query: influxQL, queryType: QueryType.InfluxQL))
{
    Console.WriteLine("{0,-30}{1,-15}", row[1], row[2]);
}
```

## Feedback

If you need help, please use our [Community Slack](https://app.slack.com/huddle/TH8RGQX5Z/C02UDUPLQKA)
or [Community Page](https://community.influxdata.com/).

New features and bugs can be reported on GitHub: <https://github.com/InfluxCommunity/influxdb3-csharp>

## Contribution

If you would like to contribute code you can do through GitHub by forking the repository and sending a pull request into
the `main` branch.

## License

The InfluxDB 3 C# .NET Client is released under the [MIT License](https://opensource.org/licenses/MIT).
