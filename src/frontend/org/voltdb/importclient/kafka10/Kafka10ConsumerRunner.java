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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.EstTime;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importclient.kafka.util.DurableTracker;
import org.voltdb.importclient.kafka.util.KafkaImporterCommitPolicy;
import org.voltdb.importclient.kafka.util.PendingWorkTracker;
import org.voltdb.importclient.kafka.util.SimpleTracker;
import org.voltdb.importer.CommitTracker;
import org.voltdb.importer.ImporterLifecycle;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;
import org.voltdb.importer.formatter.FormatterBuilder;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public abstract class Kafka10ConsumerRunner implements Runnable {

    protected Consumer<ByteBuffer, ByteBuffer> m_consumer;
    protected Kafka10StreamImporterConfig m_config;
    protected ImporterLifecycle m_lifecycle;

    private final AtomicReference<Map<TopicPartition, AtomicLong>> m_currentOffSets = new AtomicReference<>();
    private final AtomicReference<Map<TopicPartition, AtomicLong>> m_lastCommittedOffSets = new AtomicReference<>();
    private final AtomicReference<Map<TopicPartition, CommitTracker>> m_trackerMap = new AtomicReference<>();

    private static final VoltLogger LOGGER = new VoltLogger("KAFKAIMPORTER");

    private final int m_waitSleepMs = 1;
    private final AtomicBoolean m_done = new AtomicBoolean(false);

    private final Map<String, Formatter>  m_formatters = new HashMap<String, Formatter>();

    //for commit policies.
    private long m_lastCommitTime = 0;

    private static int IMPORT_GAP_LEAD = Integer.getInteger("KAFKA_IMPORT_GAP_LEAD", 32_768);

    public Kafka10ConsumerRunner(ImporterLifecycle lifecycle, Kafka10StreamImporterConfig config, Consumer<ByteBuffer, ByteBuffer> consumer) throws Exception {
        m_lifecycle = lifecycle;
        m_consumer = consumer;
        m_config = config;
        m_currentOffSets.set(new HashMap<TopicPartition, AtomicLong>());
        m_lastCommittedOffSets.set(new HashMap<TopicPartition, AtomicLong>());
        m_trackerMap.set(new HashMap<TopicPartition, CommitTracker>());
    }

    protected void subscribe() {
        m_consumer.subscribe(Arrays.asList(m_config.getTopics()), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                if (partitions.isEmpty()) {
                    return;
                }
                LOGGER.info("Kafka topic and partitions dropped on this consumer: " + partitions);
                //commit offsets for the invoked partitions
                commitOffsets(partitions.stream().collect(Collectors.toList()));
                Map<TopicPartition, CommitTracker> trackers = new HashMap<TopicPartition, CommitTracker>();
                trackers.putAll(m_trackerMap.get());
                Map<TopicPartition, AtomicLong> currentOffSets = new HashMap<TopicPartition, AtomicLong>();
                currentOffSets.putAll(m_currentOffSets.get());
                Map<TopicPartition, AtomicLong> lastCommittedOffSets = new HashMap<TopicPartition, AtomicLong>();
                lastCommittedOffSets.putAll(m_lastCommittedOffSets.get());

                for (TopicPartition partition : partitions) {
                        trackers.remove(partition);
                        lastCommittedOffSets.remove(partition);
                        currentOffSets.remove(partition);
                }
                m_trackerMap.set(trackers);
                m_lastCommittedOffSets.set(lastCommittedOffSets);
                m_currentOffSets.set(currentOffSets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (partitions.isEmpty()) {
                    return;
                }
                LOGGER.info("New Kafaka topics and partitions join this consumer: " + partitions);
                calculateTrackers(partitions);
            }
        });
    }

    //add new trackers
    private void calculateTrackers(Collection<TopicPartition> partitions) {

        Map<TopicPartition, CommitTracker> trackers = new HashMap<TopicPartition, CommitTracker>();
        trackers.putAll(m_trackerMap.get());
        Map<TopicPartition, AtomicLong> currentOffSets = new HashMap<TopicPartition, AtomicLong>();
        currentOffSets.putAll(m_currentOffSets.get());
        Map<TopicPartition, AtomicLong> lastCommittedOffSets = new HashMap<TopicPartition, AtomicLong>();
        lastCommittedOffSets.putAll(m_lastCommittedOffSets.get());
        boolean newTopicPartition = false;
        for (TopicPartition partition : partitions) {
            if (m_trackerMap.get().get(partition) != null) {
                continue;
            }
            newTopicPartition = true;
            long startOffset = -1L;
            CommitTracker commitTracker = null;
            if (m_config.getCommitPolicy() == KafkaImporterCommitPolicy.TIME && m_config.getTriggerValue() > 0) {
                commitTracker = new SimpleTracker();
            } else {
                commitTracker = new DurableTracker(IMPORT_GAP_LEAD, partition.topic(), partition.partition());
            }
            trackers.put(partition, commitTracker);
            try {
                OffsetAndMetadata offsetAndMetaData = m_consumer.committed(partition);
                startOffset = offsetAndMetaData != null ? offsetAndMetaData.offset() : -1L;
                if (startOffset > -1L) {
                    commitTracker.resetTo(startOffset);
                }
            } catch (KafkaException e) {
                LOGGER.error("Failed to read committed offsets:" + partition + " " + e.getMessage());
            }
            lastCommittedOffSets.put(partition, new AtomicLong(startOffset));
            currentOffSets.put(partition, new AtomicLong(startOffset));
        }
        if (newTopicPartition) {
            m_trackerMap.set(trackers);
            m_lastCommittedOffSets.set(lastCommittedOffSets);
            m_currentOffSets.set(currentOffSets);
        }
    }

    void shutdown() {
        if (m_consumer == null) {
            return;
        }
        LOGGER.info("Shutdown Kafka consumer");
        try {
            m_consumer.wakeup();
            m_consumer.close();
            m_consumer = null;
        }  catch (Exception e) {
            LOGGER.warn("Exception while cleaning up Kafka consumer.", e);
        }
    }

    public abstract boolean invoke(String rawMessage, long offset, String topic, Object[] params, ProcedureCallback procedureCallback) throws Exception;


    @Override
    public void run() {
        LOGGER.info("Starting Kafka consumer");
        CSVParser csvParser = new CSVParser();
        PendingWorkTracker workTracker = new PendingWorkTracker();
        long submitCount = 0;
        try {
            subscribe();
            int sleepCounter = 1;
            while (m_lifecycle.shouldRun()) {
                try {
                    //point to correct offsets for topic and partitions
                    for(Map.Entry<TopicPartition, AtomicLong> entry : m_lastCommittedOffSets.get().entrySet()) {
                        long lastCommittedOffset = entry.getValue().longValue();
                        if (lastCommittedOffset > -1L) {
                            m_consumer.seek(entry.getKey(), lastCommittedOffset);
                        }
                    }
                    ConsumerRecords<ByteBuffer, ByteBuffer> records = m_consumer.poll(m_config.getConsumerTimeoutMillis());
                    //wait if nothing fetched last time.
                    if (records.isEmpty()) {
                        try { Thread.sleep(m_waitSleepMs);}
                        catch (InterruptedException ie) {}
                        continue;
                    }
                    calculateTrackers(records.partitions());
                    List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
                    for (TopicPartition partition : records.partitions()) {
                        Formatter formatter = getFormatter(partition.topic());
                        int partitionSubmittedCount = 0;
                        AtomicLong currentOffset = m_currentOffSets.get().get(partition);
                        CommitTracker commitTracker = getCommitTracker(partition);
                        //partition revoked
                        if (currentOffset == null || commitTracker == null) {
                            continue;
                        }
                        List<ConsumerRecord<ByteBuffer, ByteBuffer>> messages = records.records(partition);
                        int count = messages.size();

                        for (int i = 0; i < count; i++) {
                            ConsumerRecord<ByteBuffer, ByteBuffer> record = messages.get(i);
                            long offset = record.offset();

                            //if currentOffset is less means the record has been pushed.
                            if (offset < currentOffset.get()) {
                                continue;
                            }

                            long nextOffSet = -1;
                            if (i != (count -1)) {
                                nextOffSet = messages.get(i + 1).offset();
                            } else {
                                nextOffSet = offset + 1;
                            }

                            currentOffset.set(nextOffSet);
                            Object params[] = null;
                            String smsg = null;
                            try {
                                smsg = new String(record.value().array(), StandardCharsets.UTF_8);
                                if (formatter != null) {
                                    params = formatter.transform(ByteBuffer.wrap(smsg.getBytes()));
                                } else {
                                    params = csvParser.parseLine(smsg);
                                }
                                commitTracker.submit(nextOffSet);
                                submitCount++;
                                if (m_lifecycle.hasTransaction()) {
                                    ProcedureCallback cb = new InvocationCallback(offset, nextOffSet, workTracker, commitTracker, m_done);
                                    partitionSubmittedCount++;
                                    if (invoke(smsg, offset, partition.topic(), params, cb)) {
                                        workTracker.produceWork();
                                    } else {
                                        LOGGER.warn("Failed to push to database: " + smsg + " at offset " + offset + " partition:" + partition);
                                        commitTracker.commit(nextOffSet);
                                    }
                                } else {
                                    commitTracker.commit(nextOffSet);
                                }
                            } catch (FormatException | IOException e) {
                                LOGGER.warn("Failed to transform message " + smsg + " at offset " + offset + ", error: " + e.getMessage());
                                commitTracker.commit(nextOffSet);
                            }
                        }
                        if (partitionSubmittedCount > 0) {
                            topicPartitions.add(partition);
                        }
                    }
                    if (!m_lifecycle.shouldRun()) {
                        break;
                    }
                    if (shouldCommit()) {
                        commitOffsets(topicPartitions);
                    }
                } catch(KafkaException ex) {
                    LOGGER.error("Error seen when processing message " + m_config.getTopics(), ex);
                    sleepCounter = backoffSleep(sleepCounter);
                    continue;
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error seen when processing message " + m_config.getTopics(), e);
        } finally {
            shutdown();
        }

        m_done.compareAndSet(false, true);
        StringBuilder builder = new StringBuilder();
        builder.append("Callback Rcvd: " + workTracker.getCallbackCount());
        builder.append("Submitted: " + submitCount);
        Map<TopicPartition, AtomicLong> committedOffSets = m_lastCommittedOffSets.get();
        if (committedOffSets != null){
            committedOffSets.entrySet().stream().forEach(e-> builder.append("\npartition:" + e.getKey() + " last commit:" + e.getValue().get()));
        }
        LOGGER.info(builder.toString());
    }

    public boolean shouldCommit() {
        switch(m_config.getCommitPolicy()) {
        case TIME:
            return (EstTime.currentTimeMillis() > (m_lastCommitTime + m_config.getTriggerValue()));
        default:
            break;
        }
        return true;
    }

    private int backoffSleep(int fetchFailedCount) {
        try {
            Thread.sleep(1000 * fetchFailedCount++);
            if (fetchFailedCount > 10) fetchFailedCount = 1;
        } catch (InterruptedException ie) {
        }
        return fetchFailedCount;
    }

    private CommitTracker getCommitTracker(TopicPartition partition) {
        return m_trackerMap.get().get(partition);
    }

    private void commitOffsets(List<TopicPartition> topicPartitions) {
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (TopicPartition partition : topicPartitions) {
            CommitTracker commitTracker = getCommitTracker(partition);
            if (commitTracker != null) {
                long safe = commitTracker.commit(-1L);
                AtomicLong committedOffSet = m_lastCommittedOffSets.get().get(partition);
                if (committedOffSet != null) {
                    long lastCommittedOffset = committedOffSet.longValue();
                    if (safe > lastCommittedOffset) {
                        partitionToMetadataMap.put(partition, new OffsetAndMetadata(safe + 1));
                    }
                }
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("committing offsets:" +  partitionToMetadataMap);
            }
        }

        if (partitionToMetadataMap.isEmpty()) { return;}

        int retries = 3;
        while (retries > 0) {
            try {
                m_consumer.commitSync(partitionToMetadataMap);
                m_lastCommitTime = EstTime.currentTimeMillis();
                return;
            } catch (KafkaException e) {
                LOGGER.error("Failed to commit offsets:" + partitionToMetadataMap + " " + e.getMessage());
                retries--;
            }
        }
    }

    private Formatter getFormatter(String topic) {

        Formatter formatter = m_formatters.get(topic);
        if (formatter != null) {
            return formatter;
        }

        FormatterBuilder builder = m_config.getFormatterBuilder(topic);
        if (builder != null) {
            formatter = builder.create();
            m_formatters.put(topic, formatter);
            return formatter;
        }

        return null;
    }
}
