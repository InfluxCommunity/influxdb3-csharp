using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Grpc.Net.Compression;
using InfluxDB3.Client.Test.Config;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Client.Config.Test;

public class ClientConfigTest
{
    [Test]
    public void RequiredConfig()
    {
        var ae = Assert.Throws<ArgumentNullException>(() => { new InfluxDBClient((ClientConfig)null); });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo("Value cannot be null. (Parameter 'config')"));
    }

    [Test]
    public void CreateFromConnectionStringMinimal()
    {
        var cfg = new ClientConfig("http://localhost:8086?token=my-token");
        Assert.That(cfg, Is.Not.Null);
        cfg.Validate();
        Assert.Multiple(() =>
        {
            Assert.That(cfg.Host, Is.EqualTo("http://localhost:8086/"));
            Assert.That(cfg.Token, Is.EqualTo("my-token"));
            Assert.That(cfg.Organization, Is.EqualTo(null));
            Assert.That(cfg.Database, Is.EqualTo(null));
            Assert.That(cfg.WriteOptions, Is.EqualTo(null));
        });
    }

    [Test]
    public void CreateFromConnectionStringBasic()
    {
        var cfg = new ClientConfig("http://localhost:8086?token=my-token&org=my-org&database=my-database");
        Assert.That(cfg, Is.Not.Null);
        cfg.Validate();
        Assert.Multiple(() =>
        {
            Assert.That(cfg.Host, Is.EqualTo("http://localhost:8086/"));
            Assert.That(cfg.Token, Is.EqualTo("my-token"));
            Assert.That(cfg.Organization, Is.EqualTo("my-org"));
            Assert.That(cfg.Database, Is.EqualTo("my-database"));
            Assert.That(cfg.WriteOptions, Is.EqualTo(null));
        });
    }

    [Test]
    public void CreateFromConnectionStringWithAuthScheme()
    {
        var cfg = new ClientConfig("http://localhost:8086?token=my-token&org=my-org&authScheme=my-scheme");
        Assert.That(cfg, Is.Not.Null);
        cfg.Validate();
        Assert.Multiple(() =>
        {
            Assert.That(cfg.Host, Is.EqualTo("http://localhost:8086/"));
            Assert.That(cfg.Token, Is.EqualTo("my-token"));
            Assert.That(cfg.AuthScheme, Is.EqualTo("my-scheme"));
            Assert.That(cfg.WriteOptions, Is.EqualTo(null));
        });
    }

    [Test]
    public void CreateFromConnectionStringWithWriteOptions()
    {
        var cfg = new ClientConfig("http://localhost:8086?token=my-token&org=my-org&database=my-database&precision=s&gzipThreshold=64");
        Assert.That(cfg, Is.Not.Null);
        cfg.Validate();
        Assert.Multiple(() =>
        {
            Assert.That(cfg.Host, Is.EqualTo("http://localhost:8086/"));
            Assert.That(cfg.Token, Is.EqualTo("my-token"));
            Assert.That(cfg.Organization, Is.EqualTo("my-org"));
            Assert.That(cfg.Database, Is.EqualTo("my-database"));
            Assert.That(cfg.WriteOptions.Precision, Is.EqualTo(WritePrecision.S));
            Assert.That(cfg.WriteOptions.GzipThreshold, Is.EqualTo(64));
        });
    }

    [Test]
    public void CreateFromConnectionStringPrecisions()
    {
        var precisions = new[]
        {
            ("ns", WritePrecision.Ns),
            ("us", WritePrecision.Us),
            ("ms", WritePrecision.Ms),
            ("s", WritePrecision.S),
        };
        foreach (var precision in precisions)
        {
            var cfg = new ClientConfig($"http://localhost:8086?token=my-token&precision={precision.Item1}");
            Assert.That(cfg, Is.Not.Null);
            cfg.Validate();
            Assert.Multiple(() =>
            {
                Assert.That(cfg.Host, Is.EqualTo("http://localhost:8086/"));
                Assert.That(cfg.Token, Is.EqualTo("my-token"));
                Assert.That(cfg.WriteOptions.Precision, Is.EqualTo(precision.Item2));
            });
        }
    }

    [Test]
    public void CreateFromConnectionStringInvalidPrecision()
    {
        var ae = Assert.Throws<ArgumentException>(() => { new ClientConfig("http://localhost:8086?token=my-token&precision=xs"); });

        Assert.That(ae, Is.Not.Null);
        Assert.That(ae.Message, Is.EqualTo("Unsupported precision 'xs'"));
    }

    [Test]
    public void CreateFromEnvMinimal()
    {
        var env = new Dictionary<String, String>
        {
            {"INFLUX_HOST", "http://localhost:8086"},
            {"INFLUX_TOKEN", "my-token"},
        };
        SetEnv(env);
        var cfg = new ClientConfig(env);
        Assert.That(cfg, Is.Not.Null);
        cfg.Validate();
        Assert.Multiple(() =>
        {
            Assert.That(cfg.Host, Is.EqualTo("http://localhost:8086/"));
            Assert.That(cfg.Token, Is.EqualTo("my-token"));
            Assert.That(cfg.Organization, Is.EqualTo(null));
            Assert.That(cfg.Database, Is.EqualTo(null));
            Assert.That(cfg.WriteOptions, Is.EqualTo(null));
        });
    }

    [Test]
    public void CreateFromEnvBasic()
    {
        var env = new Dictionary<String, String>
        {
            {"INFLUX_HOST", "http://localhost:8086"},
            {"INFLUX_TOKEN", "my-token"},
            {"INFLUX_ORG", "my-org"},
            {"INFLUX_DATABASE", "my-database"},
        };
        SetEnv(env);
        var cfg = new ClientConfig(env);
        Assert.That(cfg, Is.Not.Null);
        cfg.Validate();
        Assert.Multiple(() =>
        {
            Assert.That(cfg.Host, Is.EqualTo("http://localhost:8086/"));
            Assert.That(cfg.Token, Is.EqualTo("my-token"));
            Assert.That(cfg.Organization, Is.EqualTo("my-org"));
            Assert.That(cfg.Database, Is.EqualTo("my-database"));
            Assert.That(cfg.WriteOptions, Is.EqualTo(null));
        });
    }

    [Test]
    public void CreateFromEnvWithAuthScheme()
    {
        var env = new Dictionary<String, String>
        {
            {"INFLUX_HOST", "http://localhost:8086"},
            {"INFLUX_TOKEN", "my-token"},
            {"INFLUX_AUTH_SCHEME", "my-scheme"},
        };
        SetEnv(env);
        var cfg = new ClientConfig(env);
        Assert.That(cfg, Is.Not.Null);
        cfg.Validate();
        Assert.Multiple(() =>
        {
            Assert.That(cfg.Host, Is.EqualTo("http://localhost:8086/"));
            Assert.That(cfg.Token, Is.EqualTo("my-token"));
            Assert.That(cfg.AuthScheme, Is.EqualTo("my-scheme"));
            Assert.That(cfg.WriteOptions, Is.EqualTo(null));
        });
    }

    [Test]
    public void CreateFromEnvWithWriteOptions()
    {
        var env = new Dictionary<String, String>
        {
            {"INFLUX_HOST", "http://localhost:8086"},
            {"INFLUX_TOKEN", "my-token"},
            {"INFLUX_ORG", "my-org"},
            {"INFLUX_DATABASE", "my-database"},
            {"INFLUX_PRECISION", "s"},
            {"INFLUX_GZIP_THRESHOLD", "64"},
        };
        SetEnv(env);
        var cfg = new ClientConfig(env);
        Assert.That(cfg, Is.Not.Null);
        cfg.Validate();
        Assert.Multiple(() =>
        {
            Assert.That(cfg.Host, Is.EqualTo("http://localhost:8086/"));
            Assert.That(cfg.Token, Is.EqualTo("my-token"));
            Assert.That(cfg.Organization, Is.EqualTo("my-org"));
            Assert.That(cfg.Database, Is.EqualTo("my-database"));
            Assert.That(cfg.WriteOptions.Precision, Is.EqualTo(WritePrecision.S));
            Assert.That(cfg.WriteOptions.GzipThreshold, Is.EqualTo(64));
        });
    }

    private static void SetEnv(IDictionary<String, String> dict)
    {
        foreach (var entry in dict)
        {
            Environment.SetEnvironmentVariable(entry.Key, entry.Value, EnvironmentVariableTarget.Process);
        }
    }

    [Test]
    public void DefaultQueryOptionsValue()
    {
        var config = new ClientConfig();
        Assert.That(config.QueryOptions, Is.EqualTo(QueryOptions.DefaultOptions).Using(new QueryOptionsComparer()));
    }

    [Test]
    public void CustomQueryOptions()
    {
        var config = new ClientConfig();
        var options = new QueryOptions
        {
            Deadline = DateTime.Now.AddMinutes(5),
            MaxReceiveMessageSize = 8_388_608,
            MaxSendMessageSize = 10000,
            CompressionProviders = ImmutableArray<ICompressionProvider>.Empty
        };

        config.QueryOptions = options;
        Assert.That(config.QueryOptions, Is.EqualTo(options));
    }


    [TearDown]
    public void Cleanup()
    {
        var envVars = new List<String>
        {
            ClientConfig.EnvInfluxHost,
            ClientConfig.EnvInfluxToken,
            ClientConfig.EnvInfluxOrg,
            ClientConfig.EnvInfluxDatabase,
            ClientConfig.EnvInfluxPrecision,
            ClientConfig.EnvInfluxGzipThreshold
        };
        foreach (var envVar in envVars)
        {
            Environment.SetEnvironmentVariable(envVar, null, EnvironmentVariableTarget.Process);
        }
    }
}
