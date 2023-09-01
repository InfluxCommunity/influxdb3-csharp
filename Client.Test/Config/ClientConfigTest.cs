using System;
using System.Collections.Generic;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Client.Config.Test;

public class ClientConfigTest
{
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
    public void CreateeFromConnectionStringBasic()
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
    public void CreateFromEnvMinimal()
    {
        var env = new Dictionary<String,String>
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
        var env = new Dictionary<String,String>
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
    public void CreateFromEnvWithWriteOptions()
    {
        var env = new Dictionary<String,String>
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

    private static void SetEnv(IDictionary<String,String> dict)
    {
        foreach (var entry in dict)
        {
            Environment.SetEnvironmentVariable(entry.Key, entry.Value, EnvironmentVariableTarget.Process);
        }
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
