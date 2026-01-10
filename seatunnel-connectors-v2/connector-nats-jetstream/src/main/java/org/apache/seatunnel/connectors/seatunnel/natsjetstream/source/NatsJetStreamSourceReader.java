/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.natsjetstream.source;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SingleThreadFetcherManager;

import io.nats.client.Message;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class NatsJetStreamSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
                Message, SeaTunnelRow, NatsJetStreamSourceSplit, NatsJetStreamSourceSplitState> {

    public NatsJetStreamSourceReader(
            BlockingQueue<RecordsWithSplitIds<Message>> elementsQueue,
            SingleThreadFetcherManager<Message, NatsJetStreamSourceSplit> splitFetcherManager,
            RecordEmitter<Message, SeaTunnelRow, NatsJetStreamSourceSplitState> recordEmitter,
            SourceReaderOptions options,
            Context context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, options, context);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void notifyCheckpointComplete(long arg0) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'notifyCheckpointComplete'");
    }

    @Override
    protected NatsJetStreamSourceSplitState initializedState(NatsJetStreamSourceSplit arg0) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'initializedState'");
    }

    @Override
    protected void onSplitFinished(Map<String, NatsJetStreamSourceSplitState> arg0) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'onSplitFinished'");
    }

    @Override
    protected NatsJetStreamSourceSplit toSplitType(
            String arg0, NatsJetStreamSourceSplitState arg1) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'toSplitType'");
    }
}
