using System.Collections.Generic;
using InfluxDB3.Client.Config;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Client.Test.Config;

[TestFixture]
public class WriteOptionsTest
{
    [Test]
    public void UseNewerTagsWhenBothHaveValues()
    {
        ClientConfig config = new ClientConfig
        {
            WriteOptions = new WriteOptions
            {
                DefaultTags = new Dictionary<string, string>()
                {
                    {"key1", "valueA"},
                    {"key2", "valueB"}
                }
            }
        };

        WriteOptions oneOffOptions = new WriteOptions
        {
            DefaultTags = new Dictionary<string, string>()
            {
                { "clef1", "valeurA" },
                { "clef2", "valeurB" }
            }
        };

        Dictionary<string, string> resultTags = oneOffOptions.GetDefaultTagsSafe(config.WriteOptions);

        foreach (var tag in config.WriteOptions.DefaultTags)
        {
            Assert.That(resultTags, Does.Not.ContainKey(tag.Key));
            Assert.That(resultTags, Does.Not.ContainValue(tag.Value));
        }

        foreach (var tag in oneOffOptions.DefaultTags)
        {
            Assert.That(resultTags, Contains.Key(tag.Key));
            Assert.That(resultTags, Contains.Value(tag.Value));
        }
    }

    [Test]
    public void ConfigDefaultTagsNull()
    {
        ClientConfig config = new ClientConfig
        {
            WriteOptions = new WriteOptions
            {
                AcceptPartial = true
            }
        };

        WriteOptions oneOffOptions = new WriteOptions
        {
            DefaultTags = new Dictionary<string, string>()
            {
                { "clef1", "valeurA" },
                { "clef2", "valeurB" }
            }
        };

        Dictionary<string, string> resultTags = oneOffOptions.GetDefaultTagsSafe(config.WriteOptions);

        Assert.That(resultTags.Count, Is.EqualTo(oneOffOptions.DefaultTags.Count));
        foreach (var tag in oneOffOptions.DefaultTags)
        {
            Assert.That(resultTags, Contains.Key(tag.Key));
            Assert.That(resultTags, Contains.Value(tag.Value));
        }
    }

    [Test]
    public void OneOffOptionDefaultTagsNull()
    {
        ClientConfig config = new ClientConfig
        {
            WriteOptions = new WriteOptions
            {
                DefaultTags = new Dictionary<string, string>()
                {
                    {"key1", "valueA"},
                    {"key2", "valueB"}
                }
            }
        };

        WriteOptions oneOffOptions = new WriteOptions
        {
            Precision = WritePrecision.S
        };

        Dictionary<string, string> resultTags = oneOffOptions.GetDefaultTagsSafe(config.WriteOptions);

        Assert.That(resultTags.Count, Is.EqualTo(config.WriteOptions.DefaultTags.Count));
        foreach (var tag in config.WriteOptions.DefaultTags)
        {
            Assert.That(resultTags, Contains.Key(tag.Key));
            Assert.That(resultTags, Contains.Value(tag.Value));
        }
    }

    [Test]
    public void BothDefaultTagsNull()
    {
        ClientConfig config = new ClientConfig
        {
            WriteOptions = new WriteOptions
            {
                AcceptPartial = true
            }
        };
        WriteOptions oneOffOptions = new WriteOptions
        {
            Precision = WritePrecision.S
        };

        Dictionary<string, string> resultTags = oneOffOptions.GetDefaultTagsSafe(config.WriteOptions);
        Assert.That(resultTags, Is.Null);

    }
}