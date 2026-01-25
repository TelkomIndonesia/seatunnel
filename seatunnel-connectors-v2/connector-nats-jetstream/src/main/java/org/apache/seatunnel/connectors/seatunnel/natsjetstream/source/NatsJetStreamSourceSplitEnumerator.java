package org.apache.seatunnel.connectors.seatunnel.natsjetstream.source;

import java.io.IOException;
import java.util.List;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

public class NatsJetStreamSourceSplitEnumerator
        implements SourceSplitEnumerator<NatsJetStreamSourceSplit, NatsJetStreamSourceState> {

    @Override
    public void notifyCheckpointComplete(long arg0) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'notifyCheckpointComplete'");
    }

    @Override
    public void addSplitsBack(List<NatsJetStreamSourceSplit> arg0, int arg1) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addSplitsBack'");
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

    @Override
    public int currentUnassignedSplitSize() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'currentUnassignedSplitSize'");
    }

    @Override
    public void handleSplitRequest(int arg0) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handleSplitRequest'");
    }

    @Override
    public void open() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'open'");
    }

    @Override
    public void registerReader(int arg0) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'registerReader'");
    }

    @Override
    public void run() throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'run'");
    }

    @Override
    public NatsJetStreamSourceState snapshotState(long arg0) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'snapshotState'");
    }

}
