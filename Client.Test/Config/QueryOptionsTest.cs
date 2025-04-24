using System;
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
                MaxReceiveMessageSize = 1000
            };

            var clone = (QueryOptions)options.Clone();

            Assert.That(clone.Deadline, Is.EqualTo(options.Deadline));
            Assert.That(clone.MaxReceiveMessageSize, Is.EqualTo(options.MaxReceiveMessageSize));
        }

        [Test]
        public void Clone_CreatesIndependentCopy()
        {
            var options = new QueryOptions
            {
                Deadline = DateTime.Now,
                MaxReceiveMessageSize = 1000
            };

            var clone = (QueryOptions)options.Clone();
            options.Deadline = DateTime.Now.AddDays(1);
            options.MaxReceiveMessageSize = 2000;

            Assert.That(clone.Deadline, Is.Not.EqualTo(options.Deadline));
            Assert.That(clone.MaxReceiveMessageSize, Is.Not.EqualTo(options.MaxReceiveMessageSize));
        }
    }
}