/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importer.ImporterLifecycle;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;
import org.voltdb.utils.CSVDataLoader;
import org.voltdb.utils.RowWithMetaData;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public class KafkaExternalConsumerRunner extends KafkaConsumerRunner {

    private static final VoltLogger LOGGER = new VoltLogger("KAFKALOADER10");

    private final CSVDataLoader m_loader;
    private Formatter m_formatter = null;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public KafkaExternalConsumerRunner(ImporterLifecycle lifecycle,
            KafkaLoaderConfig config, Consumer<ByteBuffer, ByteBuffer> consumer, CSVDataLoader loader) throws Exception {
        super(lifecycle, config, consumer);
        m_loader = loader;
        if (config.getFormatterProperties() != null) {
            String formatter = config.getFormatterProperties().getProperty("formatter");
            if (formatter != null && !formatter.trim().isEmpty()) {
                String format = config.getFormatterProperties().getProperty("format", "csv");
                Class classz = Class.forName(formatter);
                Class[] ctorParmTypes = new Class[]{ String.class, Properties.class };
                Constructor ctor = classz.getDeclaredConstructor(ctorParmTypes);
                Object[] ctorParms = new Object[]{ format, config.getFormatterProperties() };
                m_formatter = (Formatter )ctor.newInstance(ctorParms);
            }
        }
    }

    @Override
    public boolean invoke(String rawMessage, long offset, String topic, Object[] params, ProcedureCallback procedureCallback) throws Exception {
        m_loader.insertRow(new RowWithMetaData(rawMessage, offset, procedureCallback), params);
        return true;
    }
    protected void subscribe() {
        LOGGER.info("Kafka consumer subscribes topics:" + m_config.getTopics());
        m_consumer.subscribe(Arrays.asList(m_config.getTopics().split(",")), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                LOGGER.info("Kafka consumer drops topic and partitions: " + partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                LOGGER.info("Kafka topics and partitions join this consumer: " + partitions);
            }
        });
    }

    protected void shutdown() {
        if (m_consumer == null) {
            return;
        }
        if (m_done.compareAndSet(false, true)) {
            m_consumer.wakeup();
        }
    }

    @Override
    public void run() {
        CSVParser csvParser = new CSVParser();
        try {
            subscribe();
            while (!m_done.get()) {
                ConsumerRecords<ByteBuffer, ByteBuffer> records = null;
                try {
                    records = m_consumer.poll(m_config.getPollTimeout());
                } catch (WakeupException we) {
                    if (m_done.get()) {
                        break;
                    }
                }
                for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
                    long offset = record.offset();
                    Object params[];
                    String smsg = new String(record.value().array(), StandardCharsets.UTF_8);
                    if (m_formatter != null) {
                        try {
                            params = m_formatter.transform(ByteBuffer.wrap(smsg.getBytes()));
                        } catch (FormatException badMsg) {
                            LOGGER.warn("Failed to transform message " + smsg + " at offset " + offset
                                    + ", error: " + badMsg.getMessage());
                            continue;
                        }
                    } else {
                        params = csvParser.parseLine(smsg);
                    }
                    if (params == null) {
                        continue;
                    }
                    m_loader.insertRow(new RowWithMetaData(smsg, offset), params);
                }
            }
        } catch (IOException e) {
            m_done.set(true);
            LOGGER.error("Fail to process message:" + m_config.getTopics(), e);
        }  catch (Throwable terminate) {
            m_done.set(true);
            LOGGER.error("Error seen during poll", terminate);
        } finally {
            try {
                m_consumer.close();
                m_consumer = null;
            } catch (Exception e) {
                LOGGER.warn("Exception while cleaning up Kafka consumer.", e);
            }
            ((KafkaLoader)m_lifecycle).notifyShutdown();
        }
    }
}
