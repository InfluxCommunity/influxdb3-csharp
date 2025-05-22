using System;

namespace InfluxDB3.Client.Write;

/// <summary>
/// Defines WritePrecision
/// </summary>
public enum WritePrecision
{
    /// <summary>
    /// Enum Ms for value: ms
    /// </summary>
    Ms = 1,

    /// <summary>
    /// Enum S for value: s
    /// </summary>
    S = 2,

    /// <summary>
    /// Enum Us for value: us
    /// </summary>
    Us = 3,

    /// <summary>
    /// Enum Ns for value: ns
    /// </summary>
    Ns = 4
}

public static class WritePrecisionConverter
{
    public static string ToV2ApiString(WritePrecision precision)
    {
        return precision switch
        {
            WritePrecision.Ns => "ns",
            WritePrecision.Us => "us",
            WritePrecision.Ms => "ms",
            WritePrecision.S => "s",
            _ => throw new ArgumentException($"Unsupported precision '{precision}'"),
        };
    }

    public static string ToV3ApiString(WritePrecision precision)
    {
        return precision switch
        {
            WritePrecision.Ns => "nanosecond",
            WritePrecision.Us => "microsecond",
            WritePrecision.Ms => "millisecond",
            WritePrecision.S => "second",
            _ => throw new ArgumentException($"Unsupported precision '{precision}'"),
        };
    }
}