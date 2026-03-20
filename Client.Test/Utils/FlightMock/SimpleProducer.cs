using System.Collections.Generic;
using Apache.Arrow;

namespace InfluxDB3.Client.Test.Utils.FlightMock;

/// <summary>
/// Use this class to wrap a Schema and a list of RecordBatches.
/// This class mainly uses with FlightServerMock for testing. This class will be injected into FlightServerMock.
/// FlightServerMock.DoGet will get data from this class to return data for query apis.
/// </summary>
public class SimpleProducer
{
    public Schema Schema { get; set; }

    public List<RecordBatch> RecordBatches { get; set; }
}