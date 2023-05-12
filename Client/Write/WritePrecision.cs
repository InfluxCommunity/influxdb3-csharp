using System.Runtime.Serialization;

namespace InfluxDB3.Client.Write;

/// <summary>
/// Defines WritePrecision
/// </summary>
public enum WritePrecision
{
    /// <summary>
    /// Enum Ms for value: ms
    /// </summary>
    [EnumMember(Value = "ms")] Ms = 1,

    /// <summary>
    /// Enum S for value: s
    /// </summary>
    [EnumMember(Value = "s")] S = 2,

    /// <summary>
    /// Enum Us for value: us
    /// </summary>
    [EnumMember(Value = "us")] Us = 3,

    /// <summary>
    /// Enum Ns for value: ns
    /// </summary>
    [EnumMember(Value = "ns")] Ns = 4
}