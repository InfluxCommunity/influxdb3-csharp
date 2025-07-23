## 1.3.0 [unreleased]

### Features

1. [#164](https://github.com/InfluxCommunity/influxdb3-csharp/pull/164): Add function to get InfluxDB version.
1. [#168](https://github.com/InfluxCommunity/influxdb3-csharp/pull/168): Run integration tests against a locally started InfluxDB 3 Core server.

## 1.2.0 [2025-05-22]

### Features

1. [#155](https://github.com/InfluxCommunity/influxdb3-csharp/pull/155): Allows setting grpc options.
1. [#157](https://github.com/InfluxCommunity/influxdb3-csharp/pull/157): Fix: always clone `DefaultOptions` to keep it
   immutable.
1. [#158](https://github.com/InfluxCommunity/influxdb3-csharp/pull/158): Support fast writes without waiting for WAL
   persistence:
    - New write option (`WriteOptions.NoSync`) added: `true` value means faster write but without the confirmation that
      the data was persisted. Default value: `false`.
    - **Supported by self-managed InfluxDB 3 Core and Enterprise servers only!**
    - Also configurable via connection string query parameter (`writeNoSync`).
    - Also configurable via environment variable (`INFLUX_WRITE_NO_SYNC`).
    - Long precision string values added from v3 HTTP API: `"nanosecond"`, `"microsecond"`, `"millisecond"`,
      `"second"` (
      in addition to the existing `"ns"`, `"us"`, `"ms"`, `"s"`).

## 1.1.0 [2025-03-26]

### Features

1. [#153](https://github.com/InfluxCommunity/influxdb3-csharp/pull/153): Add custom SSL root certificate support.
   - New configuration items:
      - `SslRootsFilePath`
      - `DisableCertificateRevocationListCheck`
   - **Disclaimer:** Using custom SSL root certificate configurations is recommended for development and testing
     purposes
     only. For production deployments, ensure custom certificates are added to the operating system's trusted
     certificate store.

## 1.0.0 [2025-01-22]

### Features

1. [#132](https://github.com/InfluxCommunity/influxdb3-csharp/pull/132): Respect iox::column_type::field metadata when
   mapping query results into values.
    - iox::column_type::field::integer: => Long
    - iox::column_type::field::uinteger: => Long
    - iox::column_type::field::float: => Double
    - iox::column_type::field::string: => String
    - iox::column_type::field::boolean: => Boolean

## 0.8.0 [2024-09-13]

### Features

1.[#118](https://github.com/InfluxCommunity/influxdb3-csharp/pull/118): Simplify getting response headers and status code from `InfluxDBApiException`.  Includes example runnable through `Examples/General`.

## 0.7.0 [2024-08-12]

### Migration Notice

- `InfluxDBClient` constructor with connection options has new option `authScheme` with `null` default value:

```diff
- public InfluxDBClient(string host, string token, string? organization = null, string? database = null);
+ public InfluxDBClient(string host, string token, string? organization = null, string? database = null, string? authScheme = null)
```

  This new option is used for Edge (OSS) authentication.

### Features

1. [#101](https://github.com/InfluxCommunity/influxdb3-csharp/pull/101): Add standard `user-agent` header to all calls.
1. [#111](https://github.com/InfluxCommunity/influxdb3-csharp/pull/111): Add InfluxDB Edge (OSS) authentication support.

### Bug Fixes

1. [#110](https://github.com/InfluxCommunity/influxdb3-csharp/pull/110): InfluxDB Edge (OSS) error handling.

## 0.6.0 [2024-04-16]

### Features

1. [#90](https://github.com/InfluxCommunity/influxdb3-csharp/pull/90): Custom `HTTP/gRPC` headers can be specified globally by config or per request

## 0.5.0 [2024-03-01]

### Features

1. [#71](https://github.com/InfluxCommunity/influxdb3-csharp/pull/71): Add support for named query parameters

### Others

1. [#80](https://github.com/InfluxCommunity/influxdb3-csharp/pull/80): Use net8.0 as a default target framework in Tests and Examples

## 0.4.0 [2023-12-08]

### Features

1. [#66](https://github.com/InfluxCommunity/influxdb3-csharp/pull/66): Default Tags for Writes

## 0.3.0 [2023-10-02]

### Features

1. [#36](https://github.com/InfluxCommunity/influxdb3-csharp/pull/46): Add client creation from connection string
and environment variables.
1. [#52](https://github.com/InfluxCommunity/influxdb3-csharp/pull/52): Add structured query support

### Docs

1. [#52](https://github.com/InfluxCommunity/influxdb3-csharp/pull/52): Add downsampling example

## 0.2.0 [2023-08-11]

### Features

1. [#33](https://github.com/InfluxCommunity/influxdb3-csharp/pull/33): Add GZIP support
1. [#34](https://github.com/InfluxCommunity/influxdb3-csharp/pull/34): Add HTTP proxy and custom HTTP headers support

### Breaking Changes

1. [#35](https://github.com/InfluxCommunity/influxdb3-csharp/pull/35): Renamed config types and some options

## 0.1.0 [2023-06-09]

- initial release of new client version
