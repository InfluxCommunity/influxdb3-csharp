using System;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

[assembly: InternalsVisibleTo("InfluxDB3.Client.Test, PublicKey=0024000004800000940000000602000000240000" +
                              "525341310004000001000100054b3efef02968d05c3dd8481e23fb40ade1fae377f18cf5fa48c67369414" +
                              "0f7c00dc0b38d43be297256824dc8489c5224647e77f861ef600514607159b151cf71b094a0ef5736c420" +
                              "cbaa14100acc3b3694e3815597a5e89cf8090ed22bfdad2d5eec49250d88da1345d670b5e131ed9611eed" +
                              "141e04c31d79f166db39cb4a5")]
namespace InfluxDB3.Client.Internal
{
    /// <summary>
    /// Functions for parameter validation.
    ///
    /// <para>
    /// Inspiration from InfluxDB java - <a href="https://github.com/influxdata/influxdb-java/">thanks</a>
    /// </para>
    /// </summary>
    internal static class Arguments
    {
        private const string DurationPattern = @"([-+]?)([0-9]+(\\.[0-9]*)?[a-z]+)+|inf|-inf";

        /// <summary>
        /// Enforces that the string is not empty.
        /// </summary>
        /// <param name="value">the string to test</param>
        /// <param name="name">the variable name for reporting</param>
        /// <exception cref="ArgumentException">if the string is empty</exception>
        internal static void CheckNonEmptyString(string value, string name)
        {
            if (string.IsNullOrEmpty(value))
            {
                throw new ArgumentException("Expecting a non-empty string for " + name);
            }
        }

        /// <summary>
        /// Enforces that the string is duration literal.
        /// </summary>
        /// <param name="value">the string to test</param>
        /// <param name="name">the variable name for reporting</param>
        /// <exception cref="ArgumentException">if the string is not duration literal</exception>
        internal static void CheckDuration(string value, string name)
        {
            if (string.IsNullOrEmpty(value) || !Regex.Match(value, DurationPattern).Success)
            {
                throw new ArgumentException("Expecting a duration string for " + name + ". But got: " + value);
            }
        }

        /// <summary>
        /// Enforces that the number is larger than 0.
        /// </summary>
        /// <param name="number">the number to test</param>
        /// <param name="name">the variable name for reporting</param>
        /// <exception cref="ArgumentException">if the number is less or equal to 0</exception>
        internal static void CheckPositiveNumber(int number, string name)
        {
            if (number <= 0)
            {
                throw new ArgumentException("Expecting a positive number for " + name);
            }
        }

        /// <summary>
        /// Enforces that the number is not negative.
        /// </summary>
        /// <param name="number">the number to test</param>
        /// <param name="name">the variable name for reporting</param>
        /// <exception cref="ArgumentException">if the number is less or equal to 0</exception>
        internal static void CheckNotNegativeNumber(int number, string name)
        {
            if (number < 0)
            {
                throw new ArgumentException("Expecting a positive or zero number for " + name);
            }
        }

        /// <summary>
        /// Checks that the specified object reference is not null.
        /// </summary>
        /// <param name="obj">the object to test</param>
        /// <param name="name">the variable name for reporting</param>
        /// <exception cref="NullReferenceException">if the object is null</exception>
        internal static void CheckNotNull(object obj, string name)
        {
            if (obj == null)
            {
                throw new NullReferenceException("Expecting a not null reference for " + name);
            }
        }
    }
}