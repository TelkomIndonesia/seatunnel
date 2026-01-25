package org.apache.seatunnel.connectors.seatunnel.natsjetstream.source;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;

import io.nats.client.Message;

public class NatsJetStreamSourceFetchManager
        extends SingleThreadFetcherManager<Message, NatsJetStreamSourceSplit> {

    public NatsJetStreamSourceFetchManager(BlockingQueue<RecordsWithSplitIds<Message>> elementsQueue,
            Supplier<SplitReader<Message, NatsJetStreamSourceSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
        // TODO Auto-generated constructor stub
    }

    public NatsJetStreamSourceFetchManager(BlockingQueue<RecordsWithSplitIds<Message>> elementsQueue,
            Supplier<SplitReader<Message, NatsJetStreamSourceSplit>> splitReaderSupplier,
            Consumer<Collection<String>> splitFinishedHook) {
        super(elementsQueue, splitReaderSupplier, splitFinishedHook);
        // TODO Auto-generated constructor stub
    }

}
