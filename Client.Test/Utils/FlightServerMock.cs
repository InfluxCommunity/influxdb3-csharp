using System;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Server;
using Grpc.Core;

namespace InfluxDB3.Client.Test.Utils;

public class FlightServerMock : FlightServer
{
    private readonly SimpleProducer _simpleProducer;

    public FlightServerMock(SimpleProducer simpleProducer)
    {
        _simpleProducer = simpleProducer;
    }

    public override Task DoPut(FlightServerRecordBatchStreamReader requestStream,
        IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context)
    {
        throw new NotImplementedException();
    }

    public override async Task DoGet(FlightTicket ticket, FlightServerRecordBatchStreamWriter responseStream,
        ServerCallContext context)
    {
        await responseStream.SetupStream(_simpleProducer.Schema);
        foreach (var batch in _simpleProducer.RecordBatches)
        {
            await responseStream.WriteAsync(batch);
        }
    }

    public override Task ListFlights(FlightCriteria request, IAsyncStreamWriter<FlightInfo> responseStream,
        ServerCallContext context)
    {
        throw new NotImplementedException();
    }

    public override Task ListActions(IAsyncStreamWriter<FlightActionType> responseStream, ServerCallContext context)
    {
        throw new NotImplementedException();
    }

    public override Task DoAction(FlightAction request, IAsyncStreamWriter<FlightResult> responseStream,
        ServerCallContext context)
    {
        throw new NotImplementedException();
    }

    public override Task<Schema> GetSchema(FlightDescriptor request, ServerCallContext context)
    {
        throw new NotImplementedException();
    }

    public override Task<FlightInfo> GetFlightInfo(FlightDescriptor request, ServerCallContext context)
    {
        throw new NotImplementedException();
    }

    public override Task Handshake(IAsyncStreamReader<FlightHandshakeRequest> requestStream,
        IAsyncStreamWriter<FlightHandshakeResponse> responseStream, ServerCallContext context)
    {
        throw new NotImplementedException();
    }

    public override Task DoExchange(FlightServerRecordBatchStreamReader requestStream,
        FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context)
    {
        throw new NotImplementedException();
    }
}