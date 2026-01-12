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

package org.apache.seatunnel.connectors.seatunnel.natsjetstream.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.config.NatsJetStreamBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.natsjetstream.config.NatsJetStreamMessageFormat;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PublishOptions;
import io.nats.client.impl.Headers;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class NatsJetStreamSinkWriter
        implements SinkWriter<SeaTunnelRow, NatsJetStreamSinkCommitInfo, NatsJetStreamSinkState> {

    private final Connection connection;
    private final JetStream jetStream;
    private final String defaultSubject;

    private final SeaTunnelRowType rowType;
    private final SerializationSchema serializationSchema;
    private final Map<String, String> nativeFields;

    public NatsJetStreamSinkWriter(Context context, SeaTunnelRowType seaTunnelRowType, ReadonlyConfig pluginConfig)
            throws IOException {

        String url = pluginConfig.get(NatsJetStreamBaseOptions.URL);
        Options.Builder builder = Options.builder().server(url);
        String username = pluginConfig.get(NatsJetStreamBaseOptions.USERNAME);
        String password = pluginConfig.get(NatsJetStreamBaseOptions.PASSWORD);
        if (username != null && password != null) {
            builder.userInfo(username, password);
        }
        String token = pluginConfig.get(NatsJetStreamBaseOptions.TOKEN);
        if (token != null) {
            builder.token(token.toCharArray());
        }
        try {
            this.connection = Nats.connect(builder.build());
            this.jetStream = connection.jetStream();
        } catch (InterruptedException e) {
            throw new IOException("connect nats error", e);
        }
        defaultSubject = pluginConfig.get(NatsJetStreamBaseOptions.SUBJECT);

        NatsJetStreamMessageFormat format = pluginConfig.get(NatsJetStreamBaseOptions.FORMAT);
        rowType = seaTunnelRowType;
        switch (format) {
            case JSON:
                serializationSchema = new JsonSerializationSchema(seaTunnelRowType);
                nativeFields = null;
                break;
            default:
                serializationSchema = null;
                nativeFields = pluginConfig.get(NatsJetStreamBaseOptions.NATIVE_FIELDS);
                break;
        }
    }

    @Override
    public void abortPrepare() {
    }

    @Override
    public void close() throws IOException {
        try {
            connection.close();
        } catch (InterruptedException e) {
            throw new IOException("close nats error", e);
        }
    }

    @Override
    public Optional<NatsJetStreamSinkCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void write(SeaTunnelRow arg0) throws IOException {
        PublishOptions.Builder opts = PublishOptions.builder();

        String subject = defaultSubject;
        Headers headers = new Headers();
        byte[] data;
        if (nativeFields != null) {
            Object sobj = arg0.getField(rowType.indexOf("subject"));
            if (sobj != null) {
                subject = (String) sobj;
            }

            Object iobj = arg0.getField(rowType.indexOf("id"));
            if (iobj != null) {
                opts.messageId((String) iobj);
            }

            Object hobj = arg0.getField(rowType.indexOf("headers"));
            if (hobj != null) {
                ((Map<String, String>) hobj).forEach((t, u) -> {
                    headers.add(t, u);
                });
            }

            data = (byte[]) arg0.getField(rowType.indexOf("data"));

        } else {
            data = serializationSchema.serialize(arg0);
        }

        try {
            jetStream.publish(subject, headers, data, opts.build());
        } catch (JetStreamApiException e) {
            throw new IOException("publish nats error", e);
        }
    }
}
