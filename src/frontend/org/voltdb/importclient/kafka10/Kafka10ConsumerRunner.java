/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.importclient.kafka10;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importer.ImporterLifecycle;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public abstract class Kafka10ConsumerRunner implements Runnable {

    protected Consumer<byte[], byte[]> m_consumer;
    protected CSVParser m_csvParser = new CSVParser();
    protected Formatter m_formatter;
    protected Kafka10StreamImporterConfig m_config;
    protected ImporterLifecycle m_lifecycle;

    private static final VoltLogger m_log = new VoltLogger("KAFKAIMPORTER");

    public Kafka10ConsumerRunner(ImporterLifecycle lifecycle, Kafka10StreamImporterConfig config, Consumer<byte[], byte[]> consumer) throws Exception {
        m_lifecycle = lifecycle;
        m_consumer = consumer;
        m_config = config;
        if (config.getFormatterBuilder() != null) {
            m_formatter = config.getFormatterBuilder().create();
        }
    }

    ProcedureCallback procedureCallback = new ProcedureCallback() {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            m_log.warn("In clientCallback! clientResponse=" + clientResponse.getStatusString());
        }
    };

    protected void subscribe() {
        m_consumer.subscribe(Arrays.asList(m_config.getTopics()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                m_log.warn("Partitions revoked: " + partitions);
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                m_log.warn("Partitions assigned: " + partitions);
            }
        });
    }

    void shutdown() {
        m_log.warn("Shutdown called for consumer: " + m_consumer + " on thread:" + Thread.currentThread());
        m_consumer.wakeup();
        m_consumer.close(m_config.getConsumerTimeoutMillis(), TimeUnit.MILLISECONDS);
    }

    public abstract void invoke(String rawMessage, long offset, Object[] params, ProcedureCallback procedureCallback) throws Exception;

    @Override
    public void run() {
        String smsg = null;
        try {

            m_log.warn("Starting consumer: " + m_consumer + " on thread:" + Thread.currentThread());
            subscribe();

            while (m_lifecycle.shouldRun()) {

                ConsumerRecords<byte[], byte[]> records = m_consumer.poll(m_config.getConsumerTimeoutMillis());
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    byte[] msg  = record.value();
                    long offset = record.offset();

                    smsg = new String(msg);
                    Object params[];
                    if (m_formatter != null) {
                        try {
                            params = m_formatter.transform(ByteBuffer.wrap(msg));
                        }
                        catch (FormatException badMsg) {
                            m_log.warn("Failed to transform message " + smsg + " at offset " + offset + ", error message: " + badMsg.getMessage());
                            continue;
                        }
                    }
                    else {
                        params = m_csvParser.parseLine(smsg);
                    }
                    if (params == null) {
                        continue;
                    }

                    invoke(smsg, offset, params, procedureCallback);
                }
            }
        }
        catch (IllegalArgumentException e) {
            m_log.error("Failed subscribing to the topic " + m_config.getTopics(), e);
        }
        catch (WakeupException e) {
            m_log.debug("Consumer signalled to terminate ", e);
        }
        catch (IOException e) {
            if (m_formatter == null) {
                m_log.error("Failed to parse message" + smsg);
            }
            else {
                m_log.error("Error seen when processing message ", e);
            }
        }
        catch (Throwable t) {
            m_log.error("Error seen during poll", t);
        }
        finally {
            try {
                m_log.warn("Starting normal termination for consumer: " + m_consumer + " on thread:" + Thread.currentThread());
                m_consumer.close();
                m_log.warn("Finished normal termination for consumer: " + m_consumer + " on thread:" + Thread.currentThread());
            }
            catch (Exception e) {
                m_log.warn("Exception while cleaning up Kafka consumer, ignoring.", e);
            }
        }
    }
}
