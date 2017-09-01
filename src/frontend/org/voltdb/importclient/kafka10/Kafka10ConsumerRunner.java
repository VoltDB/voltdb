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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importclient.kafka.util.DurableTracker;
import org.voltdb.importclient.kafka.util.PendingWorkTracker;
import org.voltdb.importer.CommitTracker;
import org.voltdb.importer.ImporterLifecycle;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;

import com.google.common.collect.Maps;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Lists;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public abstract class Kafka10ConsumerRunner implements Runnable {

    protected Consumer<byte[], byte[]> m_consumer;
    protected Kafka10StreamImporterConfig m_config;
    protected ImporterLifecycle m_lifecycle;

    private final AtomicReference<ImmutableMap<TopicPartition, AtomicLong>> m_offsetMap =
            new AtomicReference<ImmutableMap<TopicPartition, AtomicLong>>();

    private final AtomicReference<ImmutableMap<TopicPartition, CommitTracker>> m_trackerMap =
            new AtomicReference<ImmutableMap<TopicPartition, CommitTracker>>();

    private static final VoltLogger LOGGER = new VoltLogger("KAFKAIMPORTER");

    private final int m_waitSleepMs = 1;
    private final AtomicBoolean m_done = new AtomicBoolean(false);

    public Kafka10ConsumerRunner(ImporterLifecycle lifecycle, Kafka10StreamImporterConfig config, Consumer<byte[], byte[]> consumer) throws Exception {
        m_lifecycle = lifecycle;
        m_consumer = consumer;
        m_config = config;
    }

    protected void subscribe() {
        m_consumer.subscribe(Arrays.asList(m_config.getTopics()), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                LOGGER.info("Partitions revoked: " + partitions);
                commitOffsets();
                //remove Tracker
                ImmutableMap<TopicPartition, CommitTracker> expectedTrackers =  m_trackerMap.get();
                Map<TopicPartition, CommitTracker> filteredTrackers = expectedTrackers.entrySet().stream().filter(map -> !partitions.contains(map.getKey()))
                        .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
                ImmutableMap<TopicPartition, CommitTracker> updatedTrackers = ImmutableMap.copyOf(filteredTrackers);
                m_trackerMap.compareAndSet(expectedTrackers, updatedTrackers);

                //remove offset
                ImmutableMap<TopicPartition, AtomicLong> expectedOffsets =  m_offsetMap.get();
                Map<TopicPartition, AtomicLong>  filteredOffsets = expectedOffsets.entrySet().stream().filter(map -> !partitions.contains(map.getKey()))
                        .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
                ImmutableMap<TopicPartition, AtomicLong> updatedOffsets = ImmutableMap.copyOf(filteredOffsets);
                m_offsetMap.compareAndSet(expectedOffsets, updatedOffsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                LOGGER.info("Partitions assigned: " + partitions);
                ImmutableMap<TopicPartition, CommitTracker> expectedTrackers =  m_trackerMap.get();
                Map<TopicPartition, CommitTracker> trackers = Maps.newHashMap();
                trackers.putAll(expectedTrackers);

                ImmutableMap<TopicPartition, AtomicLong> expectedOffsets =  m_offsetMap.get();
                Map<TopicPartition, AtomicLong> offsets = Maps.newHashMap();
                offsets.putAll(expectedOffsets);

                for (TopicPartition tp : partitions) {
                    trackers.put(tp, new DurableTracker(Integer.getInteger("KAFKA_IMPORT_GAP_LEAD", 32_768), tp.topic(),tp.partition()));
                    OffsetAndMetadata offsetAndMetaData = m_consumer.committed(tp);
                    long startOffset = offsetAndMetaData != null ? offsetAndMetaData.offset() : -1L;
                    offsets.put(tp, new AtomicLong(startOffset));
                    if(startOffset >= 0) {
                        m_consumer.seek(tp, startOffset);
                    }
                }
                m_trackerMap.compareAndSet(expectedTrackers, ImmutableMap.copyOf(trackers));
                m_offsetMap.compareAndSet(expectedOffsets, ImmutableMap.copyOf(offsets));
            }
        });
    }

    void shutdown() {
        LOGGER.info("Shutdown Kafka consumer");
        m_consumer.wakeup();
        m_consumer.close(m_config.getConsumerTimeoutMillis(), TimeUnit.MILLISECONDS);
    }

    public abstract boolean invoke(String rawMessage, long offset, Object[] params, ProcedureCallback procedureCallback) throws Exception;

    @Override
    public void run() {
        LOGGER.info("Starting Kafka consumer");
        Formatter formatter = null;
        CSVParser csvParser = null;
        PendingWorkTracker pendingWorkTracker = new PendingWorkTracker();
        long submitCount = 0;
        try {
            subscribe();

            if (m_config.getFormatterBuilder() != null) {
                formatter = m_config.getFormatterBuilder().create();
            } else {
                csvParser = new CSVParser();
            }

            while (m_lifecycle.shouldRun()) {
                ConsumerRecords<byte[], byte[]> records = m_consumer.poll(m_config.getConsumerTimeoutMillis());
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<byte[], byte[]>> messages = Lists.newArrayList();
                    messages.addAll(records.records(partition));
                    messages.sort(Comparator.comparingLong(ConsumerRecord::offset));
                    int count = messages.size();
                    for (int i = 0; i < count; i++) {
                        ConsumerRecord<byte[], byte[]> record = messages.get(i);
                        byte[] msg  = record.value();
                        long offset = record.offset();
                        long nextOffSet = -1;
                        if (i != (count -1)) {
                            nextOffSet = messages.get(i + 1).offset();
                        }
                        String smsg = new String(msg, "UTF-8");
                        Object params[] = null;
                        try {
                            if (formatter != null) {
                                params = formatter.transform(ByteBuffer.wrap(msg));
                            } else {
                                params = csvParser.parseLine(smsg);
                            }
                        } catch (FormatException | IOException e) {
                            LOGGER.warn("Failed to transform message " + smsg + " at offset " + offset + ", error message: " + e.getMessage());
                        }
                        if (params != null) {
                            submitCount++;
                            ProcedureCallback cb = new InvocationProcedureCallback(offset,
                                    nextOffSet, pendingWorkTracker, getDurabaleTracker(partition), m_done);

                            if (invoke(smsg, offset, params, cb)) {
                                pendingWorkTracker.produceWork();
                            }
                        }
                    }
                }
                if (!m_lifecycle.shouldRun()) {
                    break;
                }
                //wait if nothing fetched last time.
                if (records.isEmpty()) {
                    try { Thread.sleep(m_waitSleepMs);}
                    catch (InterruptedException ie) {}
                }
                commitOffsets();
            }
        } catch (IllegalArgumentException | WakeupException | IOException e) {
            LOGGER.error("Error seen when processing message " + m_config.getTopics(), e);
        } catch (Throwable t) {
            LOGGER.error("Error seen during poll", t);
        } finally {
            try {
                LOGGER.debug("Stopping Kafka consumer");
                m_consumer.close();
            }  catch (Exception e) {
                LOGGER.warn("Exception while cleaning up Kafka consumer, ignoring.", e);
            }
        }

        m_done.compareAndSet(false, true);
        StringBuilder builder = new StringBuilder();
        builder.append("Callback Rcvd: " + pendingWorkTracker.getCallbackCount());
        builder.append("Submitted: " + submitCount);
        m_offsetMap.get().entrySet().stream().forEach(e-> builder.append("\npartition:" + e.getKey() + " last commit:" + e.getValue().get()));
        LOGGER.info(builder.toString());
    }

    private CommitTracker getDurabaleTracker(TopicPartition tp) {
        return m_trackerMap.get().get(tp);
    }

    private void commitOffsets() {
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = Maps.newHashMap();
        for(Map.Entry<TopicPartition, AtomicLong> e : m_offsetMap.get().entrySet()) {
            if (e.getValue().get() > 0) {
                partitionToMetadataMap.put(e.getKey(), new OffsetAndMetadata(e.getValue().get() + 1));
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("committing offsets:" +  partitionToMetadataMap);
        }
        m_consumer.commitSync(partitionToMetadataMap);
    }
}
