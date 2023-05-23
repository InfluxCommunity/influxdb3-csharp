Contents
--------

- Key.public.snk: Public key to verify strong name of `InfluxDB3.Client`.
- Key.snk: Signing key to provide strong name of `InfluxDB3.Client`.

[Microsoft guidance: Strong-named assemblies](https://msdn.microsoft.com/en-us/library/wd40t7ad(v=vs.110).aspx)

Docker
------

```shell
# Get Public Key
docker run -it -v "${PWD}:/opt/app" -w "/opt/app" mono:latest sn -tp Keys/Key.public.snk
```
