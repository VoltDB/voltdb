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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.Invocation;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public class Kafka10ConsumerRunner implements Runnable {

    private Consumer<byte[], byte[]> m_consumer;
    private CSVParser m_csvParser = new CSVParser();
    private Formatter m_formatter;
    private AtomicBoolean m_closed = new AtomicBoolean(false);
    private Kafka10StreamImporterConfig m_config;
    private AbstractImporter m_importer;

    private static final VoltLogger m_log = new VoltLogger("KAFKA10IMPORTER");

    public Kafka10ConsumerRunner(AbstractImporter importer, Kafka10StreamImporterConfig config, Consumer<byte[], byte[]> consumer) throws Exception {
        m_importer = importer;
        m_consumer = consumer;
        m_config = config;
    }

    ProcedureCallback procedureCallback = new ProcedureCallback() {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            System.out.println("In clientCallback! clientResponse=" + clientResponse.getStatusString());
        }
    };

    protected void subscribe() {
        m_consumer.subscribe(Arrays.asList(m_config.getTopics()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("partitions revoked: " + partitions);
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("partitions assigned: " + partitions);
            }
        });
    }

    @Override
    public void run() {
        String smsg = null;
        try {
            subscribe();
            while (!m_closed.get()) {
                ConsumerRecords<byte[], byte[]> records = m_consumer.poll(0); // NEEDSWORK: pollTimedWaitInMilliSec);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    byte[] msg  = record.value();
                    long offset = record.offset();

                    smsg = new String(msg);
                    Object params[];
                    if (m_formatter != null) {
                        try {
                            params = m_formatter.transform(ByteBuffer.wrap(msg));
                        } catch (FormatException badMsg) {
                            m_log.warn("Failed to transform message " + smsg + " at offset " + offset
                                    + ", error message: " + badMsg.getMessage());
                            continue;
                        }
                    } else {
                        params = m_csvParser.parseLine(smsg);
                    }
                    if (params == null) {
                        continue;
                    }
                    m_importer.callProcedure(new Invocation(m_config.getProcedure(), params), procedureCallback);

                }
            }
        }
        catch (IllegalArgumentException invalidTopic) {
            m_closed.set(true);
            m_log.error("Failed subscribing to the topic " + m_config.getTopics(), invalidTopic);
        }
        catch (WakeupException wakeup) {
            m_closed.set(true);
            m_log.debug("Consumer signalled to terminate ", wakeup);
        }
        catch (IOException ioExcp) {
            m_closed.set(true);
            if (m_formatter == null) {
                m_log.error("Failed to parse message" + smsg);
            } else {
                m_log.error("Error seen when processing message ", ioExcp);
            }
        }
        catch (Throwable terminate) {
            terminate.printStackTrace();
            m_closed.set(true);
            m_log.error("Error seen during poll", terminate);
        }
        finally {
            try {
                m_consumer.close();
            }
            catch (Exception ignore) {}
           // NEEDSWORK:  notifyShutdown();
        }
    }
}
