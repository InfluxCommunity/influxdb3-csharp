using System;
using InfluxDB3.Client.Write;

namespace InfluxDB3.Client.Test.Write
{
    public class WritePrecisionConverterTest
    {
        [TestCase(WritePrecision.Ns, "ns")]
        [TestCase(WritePrecision.Us, "us")]
        [TestCase(WritePrecision.Ms, "ms")]
        [TestCase(WritePrecision.S, "s")]
        public void ToV2ApiString_ValidPrecision(WritePrecision precision, string expectedString)
        {
            string result = WritePrecisionConverter.ToV2ApiString(precision);
            Assert.That(expectedString, Is.EqualTo(result));
        }

        [Test]
        public void ToV2ApiString_InvalidPrecision()
        {
            var ae = Assert.Throws<ArgumentException>(() =>
            {
                WritePrecisionConverter.ToV2ApiString((WritePrecision)999);
            });
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.Message, Does.Contain("Unsupported precision"));
        }

        [TestCase(WritePrecision.Ns, "nanosecond")]
        [TestCase(WritePrecision.Us, "microsecond")]
        [TestCase(WritePrecision.Ms, "millisecond")]
        [TestCase(WritePrecision.S, "second")]
        public void ToV3ApiString_ValidPrecision(WritePrecision precision, string expectedString)
        {
            string result = WritePrecisionConverter.ToV3ApiString(precision);
            Assert.That(expectedString, Is.EqualTo(result));
        }

        [Test]
        public void ToV3ApiString_InvalidPrecision()
        {
            var ae = Assert.Throws<ArgumentException>(() =>
            {
                WritePrecisionConverter.ToV3ApiString((WritePrecision)999);
            });
            Assert.That(ae, Is.Not.Null);
            Assert.That(ae.Message, Does.Contain("Unsupported precision"));
        }
    }
}