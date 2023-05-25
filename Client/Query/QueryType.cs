namespace InfluxDB3.Client.Query;

/// <summary>
/// Defines type of query sent to InfluxDB.
/// </summary>
public enum QueryType
{
    /// <summary>
    /// Query by SQL.
    /// </summary>
    SQL = 1,

    /// <summary>
    /// Query by InfluxQL.
    /// </summary>
    InfluxQL = 2,
}