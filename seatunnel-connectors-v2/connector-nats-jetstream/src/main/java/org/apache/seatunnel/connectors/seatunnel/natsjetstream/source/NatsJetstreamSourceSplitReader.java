package org.apache.seatunnel.connectors.seatunnel.natsjetstream.source;

import java.io.IOException;

import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsChange;

import io.nats.client.Message;

public class NatsJetstreamSourceSplitReader implements SplitReader<Message, NatsJetStreamSourceSplit> {

    @Override
    public void close() throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

    @Override
    public RecordsWithSplitIds<Message> fetch() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'fetch'");
    }

    @Override
    public void handleSplitsChanges(SplitsChange<NatsJetStreamSourceSplit> arg0) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handleSplitsChanges'");
    }

    @Override
    public void wakeUp() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'wakeUp'");
    }

}