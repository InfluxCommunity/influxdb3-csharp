using System.Collections.Generic;
using Apache.Arrow;

namespace InfluxDB3.Client.Test.Utils;

public class SimpleProducer
{
    public Schema Schema { get; set; }

    public List<RecordBatch> RecordBatches { get; set; }
}