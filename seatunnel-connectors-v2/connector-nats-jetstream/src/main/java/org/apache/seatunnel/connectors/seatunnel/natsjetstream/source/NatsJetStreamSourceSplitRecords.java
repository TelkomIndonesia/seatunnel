package org.apache.seatunnel.connectors.seatunnel.natsjetstream.source;

import java.util.Set;

import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;

import io.nats.client.Message;

public class NatsJetStreamSourceSplitRecords implements RecordsWithSplitIds<Message> {

    @Override
    public Set<String> finishedSplits() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'finishedSplits'");
    }

    @Override
    public Message nextRecordFromSplit() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'nextRecordFromSplit'");
    }

    @Override
    public String nextSplit() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'nextSplit'");
    }

}
