using System;
using InfluxDB3.Client.Internal;

namespace InfluxDB3.Client.Test.Internal
{
    [TestFixture]
    public class AssemblyHelperTest
    {
        [Test]
        public void GetAssemblyVersion()
        {
            var version = AssemblyHelper.GetVersion();
            Assert.Multiple(() =>
            {
                Assert.That(Version.Parse(version).Major, Is.EqualTo(1));
                Assert.That(Version.Parse(version).Minor, Is.GreaterThanOrEqualTo(0));
                Assert.That(Version.Parse(version).Build, Is.EqualTo(0));
                Assert.That(Version.Parse(version).Revision, Is.EqualTo(0));
            });
        }

        [Test]
        public void GetUserAgent()
        {
            var version = AssemblyHelper.GetVersion();
            var userAgent = AssemblyHelper.GetUserAgent();
            Assert.That(userAgent, Is.EqualTo($"influxdb3-csharp/{version}"));
        }
    }
}