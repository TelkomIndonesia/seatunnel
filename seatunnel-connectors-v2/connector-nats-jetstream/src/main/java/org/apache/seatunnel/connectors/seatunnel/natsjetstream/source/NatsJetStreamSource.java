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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SourceSplitEnumerator.Context;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.config.NatsJetStreamBaseOptions;

import java.util.List;

public class NatsJetStreamSource
        implements SeaTunnelSource<SeaTunnelRow, NatsJetStreamSourceSplit, NatsJetStreamSourceState> {

    private final ReadonlyConfig readonlyConfig;
    private JobContext jobContext;

    public NatsJetStreamSource(ReadonlyConfig readonlyConfig) {
        this.readonlyConfig = readonlyConfig;
    }

    @Override
    public String getPluginName() {
        return NatsJetStreamBaseOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.UNBOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, NatsJetStreamSourceSplit> createReader(
            org.apache.seatunnel.api.source.SourceReader.Context arg0) throws Exception {
        return new NatsJetStreamSourceReader(null, null, null, null, null);
    }

    @Override
    public SourceSplitEnumerator<NatsJetStreamSourceSplit, NatsJetStreamSourceState> createEnumerator(
            Context<NatsJetStreamSourceSplit> arg0) throws Exception {
        return new NatsJetStreamSourceSplitEnumerator();
    }

    @Override
    public SourceSplitEnumerator<NatsJetStreamSourceSplit, NatsJetStreamSourceState> restoreEnumerator(
            Context<NatsJetStreamSourceSplit> arg0, NatsJetStreamSourceState arg1)
            throws Exception {
        return new NatsJetStreamSourceSplitEnumerator();
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        // TODO Auto-generated method stub
        return SeaTunnelSource.super.getProducedCatalogTables();
    }
}
