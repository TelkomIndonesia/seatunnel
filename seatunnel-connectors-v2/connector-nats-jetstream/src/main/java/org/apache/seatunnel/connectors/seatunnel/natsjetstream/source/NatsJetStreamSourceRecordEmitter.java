package org.apache.seatunnel.connectors.seatunnel.natsjetstream.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;

import io.nats.client.Message;

public class NatsJetStreamSourceRecordEmitter
        implements RecordEmitter<Message, SeaTunnelRow, NatsJetStreamSourceSplit> {

    @Override
    public void emitRecord(Message arg0, Collector<SeaTunnelRow> arg1, NatsJetStreamSourceSplit arg2) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'emitRecord'");

    }

}
