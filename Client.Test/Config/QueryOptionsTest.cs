using System;
using System.Collections.Immutable;
using Grpc.Net.Compression;
using InfluxDB3.Client.Config;

namespace InfluxDB3.Client.Test.Config;

public class QueryOptionsTest
{
    [TestFixture]
    public class QueryOptionsTests
    {
        [Test]
        public void Clone_CreatesExactCopy()
        {
            var options = new QueryOptions
            {
                Deadline = DateTime.Now,
                MaxReceiveMessageSize = 1000,
                MaxSendMessageSize = 1000,
                CompressionProviders = ImmutableArray<ICompressionProvider>.Empty
            };

            var clone = (QueryOptions)options.Clone();

            Assert.That(clone.Deadline, Is.EqualTo(options.Deadline));
            Assert.That(clone.MaxReceiveMessageSize, Is.EqualTo(options.MaxReceiveMessageSize));
            Assert.That(clone.MaxSendMessageSize, Is.EqualTo(options.MaxSendMessageSize));
            Assert.That(clone.CompressionProviders, Is.EqualTo(options.CompressionProviders));
        }

        [Test]
        public void Clone_CreatesIndependentCopy()
        {
            var options = new QueryOptions
            {
                Deadline = DateTime.Now,
                MaxReceiveMessageSize = 1000,
                MaxSendMessageSize = 1000,
                CompressionProviders = ImmutableArray<ICompressionProvider>.Empty
            };

            var clone = (QueryOptions)options.Clone();
            options.Deadline = DateTime.Now.AddDays(1);
            options.MaxReceiveMessageSize = 2000;
            options.MaxSendMessageSize = 2000;
            options.CompressionProviders = null;

            Assert.That(clone.Deadline, Is.Not.EqualTo(options.Deadline));
            Assert.That(clone.MaxReceiveMessageSize, Is.Not.EqualTo(options.MaxReceiveMessageSize));
            Assert.That(clone.MaxSendMessageSize, Is.Not.EqualTo(options.MaxSendMessageSize));
            Assert.That(clone.CompressionProviders, Is.Not.EqualTo(options.CompressionProviders));
        }
    }
}