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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
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
    private static final VoltLogger LOGGER = new VoltLogger("KAFKAIMPORTER");

    protected Consumer<ByteBuffer, ByteBuffer> m_consumer;
    protected Kafka10StreamImporterConfig m_config;
    protected ImporterLifecycle m_lifecycle;

    private final AtomicReference<Map<TopicPartition, AtomicLong>> m_lastCommittedOffSets = new AtomicReference<>();
    private final AtomicReference<Map<TopicPartition, CommitTracker>> m_trackerMap = new AtomicReference<>();
    private final Map<TopicPartition, AtomicLong> m_lastSeekedOffSets = new ConcurrentHashMap<TopicPartition, AtomicLong>();
    private final Map<TopicPartition, AtomicLong> m_pauseOffsets = new ConcurrentHashMap<TopicPartition, AtomicLong>();
    private final Map<TopicPartition, PendingWorkTracker> m_workTrackers = new ConcurrentHashMap<TopicPartition, PendingWorkTracker>();

    private final int m_waitSleepMs = 1;
    protected final AtomicBoolean m_done = new AtomicBoolean(false);

    //Formatter by topic and partition.
    private final Map<String, Formatter>  m_formatters = new HashMap<String, Formatter>();

    //for commit policies.
    private long m_lastCommitTime = 0;

    private static int IMPORT_GAP_LEAD = Integer.getInteger("KAFKA_IMPORT_GAP_LEAD", 32_768);

    public Kafka10ConsumerRunner(ImporterLifecycle lifecycle, Kafka10StreamImporterConfig config, Consumer<ByteBuffer, ByteBuffer> consumer) throws Exception {
        m_lifecycle = lifecycle;
        m_consumer = consumer;
        m_config = config;
        m_lastCommittedOffSets.set(new HashMap<TopicPartition, AtomicLong>());
        m_trackerMap.set(new HashMap<TopicPartition, CommitTracker>());
    }

    protected void subscribe() {
        LOGGER.info("Kafka consumer subscribes topics:" + m_config.getTopics());
        m_consumer.subscribe(Arrays.asList(m_config.getTopics().split(",")), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                if (partitions.isEmpty()) {
                    return;
                }
                LOGGER.info("Kafka consumer drops topic and partitions: " + partitions);

                //commit offsets for the invoked partitions
                commitOffsets(partitions.stream().collect(Collectors.toList()));

                Map<TopicPartition, CommitTracker> trackers = new HashMap<TopicPartition, CommitTracker>();
                trackers.putAll(m_trackerMap.get());

                Map<TopicPartition, AtomicLong> lastCommittedOffSets = new HashMap<TopicPartition, AtomicLong>();
                lastCommittedOffSets.putAll(m_lastCommittedOffSets.get());

                for (TopicPartition partition : partitions) {
                    trackers.remove(partition);
                    lastCommittedOffSets.remove(partition);
                    m_pauseOffsets.remove(partition);
                    m_workTrackers.remove(partition);
                }
                m_trackerMap.set(trackers);
                m_lastCommittedOffSets.set(lastCommittedOffSets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                LOGGER.info("Kafka topics and partitions join this consumer: " + partitions);
            }
        });
    }

    //add trackers for new topic-partition in this importer
    private void calculateTrackers(Collection<TopicPartition> partitions) {

        Map<TopicPartition, CommitTracker> trackers = new HashMap<TopicPartition, CommitTracker>();
        trackers.putAll(m_trackerMap.get());

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
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Start offset of " + partition + ":" + startOffset);
                    }
                }
            } catch (KafkaException e) {
                LOGGER.error("Failed to read committed offsets:" + partition + " " + e.getMessage());
            }
            lastCommittedOffSets.put(partition, new AtomicLong(startOffset));
            m_pauseOffsets.put(partition, new AtomicLong(-1));
            m_workTrackers.put(partition, new PendingWorkTracker());
        }
        if (newTopicPartition) {
            m_trackerMap.set(trackers);
            m_lastCommittedOffSets.set(lastCommittedOffSets);
        }
    }

    protected void shutdown() {
        if (m_consumer == null) {
            return;
        }
        LOGGER.info("Shutdown Kafka consumer");
        m_done.set(true);
        try {
            m_consumer.wakeup();
        }  catch (Exception e) {
            LOGGER.warn("Exception while cleaning up Kafka consumer.", e);
        }
    }

    public abstract boolean invoke(String rawMessage, long offset, String topic, Object[] params, ProcedureCallback procedureCallback) throws Exception;

    @Override
    public void run() {
        LOGGER.info("Starting Kafka consumer for topics:" + m_config.getTopics() + ", brokers:" + m_config.getBrokers());
        long submitCount = 0;
        List<TopicPartition> topicPartitionsNeedOffsetResets = new ArrayList<TopicPartition>();
        CSVParser csvParser = new CSVParser();
        try {
            subscribe();
            int sleepCounter = 1;
            while (m_lifecycle.shouldRun()) {
                try {
                    seek(topicPartitionsNeedOffsetResets);
                    topicPartitionsNeedOffsetResets.clear();

                    //The consumer will poll messages from earliest or the committed offset on the first polling.
                    //The messages in next poll starts at the largest offset + 1 in the previous polled messages.
                    //Every message is polled only once.
                    ConsumerRecords<ByteBuffer, ByteBuffer> records = null;
                    try {
                        records = m_consumer.poll(m_config.getPollTimeout());
                    } catch (WakeupException we) {
                        if (m_done.get()) {
                            break;
                        }
                    }
                    if (records.isEmpty()) {
                        try { Thread.sleep(m_waitSleepMs);}
                        catch (InterruptedException ie) {}
                        List<TopicPartition> topicPartitions = m_lastCommittedOffSets.get().keySet().stream().collect(Collectors.toList());
                        commitOffsets(topicPartitions);
                        continue;
                    }
                    calculateTrackers(records.partitions());
                    List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
                    for (TopicPartition partition : records.partitions()) {
                        if (!m_lifecycle.shouldRun()) {
                            break;
                        }
                        Formatter formatter = getFormatter(partition.topic());
                        int partitionSubmittedCount = 0;
                        CommitTracker commitTracker = getCommitTracker(partition);
                        AtomicLong lastCommittedOffset = m_lastCommittedOffSets.get().get(partition);
                        //partition revoked?
                        if (commitTracker == null || lastCommittedOffset == null) {
                            continue;
                        }

                        List<ConsumerRecord<ByteBuffer, ByteBuffer>> messages = records.records(partition);
                        int count = messages.size();
                        for (int i = 0; i < count; i++) {
                            if (!m_lifecycle.shouldRun()) {
                                break;
                            }
                            ConsumerRecord<ByteBuffer, ByteBuffer> record = messages.get(i);
                            long offset = record.offset();

                            //Poll the partition next round after moving the position to the last committed offset
                            if (lastCommittedOffset.longValue() > -1L && offset < lastCommittedOffset.longValue()) {
                                topicPartitionsNeedOffsetResets.add(partition);
                                break;
                            }

                            long nextOffSet = offset + 1;
                            if (i != (count -1)) {
                                nextOffSet = messages.get(i + 1).offset();
                            }

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
                                    ProcedureCallback cb = new InvocationCallback(offset, nextOffSet, m_workTrackers.get(partition),
                                                                 commitTracker, m_done, m_pauseOffsets.get(partition));
                                    partitionSubmittedCount++;
                                    if (invoke(smsg, offset, partition.topic(), params, cb)) {
                                        m_workTrackers.get(partition).produceWork();
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
                            if (!m_lifecycle.shouldRun()) {
                                break;
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
            try {
                commitPauseOffSets();
                m_consumer.close();
                m_consumer = null;
            } catch (Exception e) {
                LOGGER.warn("Exception while cleaning up Kafka consumer.", e);
            }
        }

        m_done.set(true);
        StringBuilder builder = new StringBuilder();
        int cbCount = 0;
        for (PendingWorkTracker work : m_workTrackers.values()) {
            cbCount += work.getCallbackCount();
        }
        builder.append("Callback Received: " + cbCount);
        builder.append("Submitted: " + submitCount);
        Map<TopicPartition, AtomicLong> committedOffSets = m_lastCommittedOffSets.get();
        if (committedOffSets != null){
            committedOffSets.entrySet().stream().forEach(e-> builder.append("\npartition:" + e.getKey() + " last commit:" + e.getValue().get()));
        }
        LOGGER.info(builder.toString());
    }

    //Move offsets to correct positions for next poll
    private void seek(List<TopicPartition> topicPartitionsNeedOffsetResets) {
        for (TopicPartition tp : topicPartitionsNeedOffsetResets) {
            AtomicLong lastCommittedOffset = m_lastCommittedOffSets.get().get(tp);
            if (lastCommittedOffset != null && lastCommittedOffset.get() > -1L) {
                AtomicLong lastSeeked = m_lastSeekedOffSets.get(tp);

                //eliminate duplicate seek
                if (lastSeeked != null && lastSeeked.get() == lastCommittedOffset.get()) {
                    continue;
                }
                m_consumer.seek(tp, lastCommittedOffset.longValue());
                m_lastSeekedOffSets.put(tp, new AtomicLong(lastCommittedOffset.get()));

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Kafka consumer moves offset for topic-partition:" + tp + " to " + lastCommittedOffset);
                }
            }
        }
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

    private void commitPauseOffSets() {
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (Map.Entry<TopicPartition, AtomicLong> entry : m_lastCommittedOffSets.get().entrySet()) {
            PendingWorkTracker workTracker = m_workTrackers.get(entry.getKey());
            AtomicLong pauseOffset = m_pauseOffsets.get(entry.getKey());
            if (workTracker == null || pauseOffset == null) {
                continue;
            }
            long pausedOffSet = pauseOffset.get();
            boolean skipCommit = false;
            if (pausedOffSet != -1) {
                if (workTracker.waitForWorkToFinish() == false) {
                    if (pauseOffset.get() < entry.getValue().get()) {
                        LOGGER.warn("Committing paused offset even though a timeout occurred waiting for pending stored procedures to finish.");
                    } else {
                        LOGGER.warn("Refusing to commit paused offset because a timeout occurred waiting for pending stored procedures to finish.");
                        skipCommit = true;
                    }
                }
            }
            if (!skipCommit) {
                CommitTracker commitTracker = getCommitTracker(entry.getKey());
                if (commitTracker != null) {
                    long safe = commitTracker.commit(-1L);
                    AtomicLong committedOffSet = m_lastCommittedOffSets.get().get(entry.getKey());
                    if (committedOffSet != null) {
                        if (committedOffSet.get() != pausedOffSet && (safe > committedOffSet.get() || pausedOffSet != -1)) {
                            safe = (pausedOffSet != -1 ? pausedOffSet : safe);
                            partitionToMetadataMap.put(entry.getKey(), new OffsetAndMetadata(safe));
                        }
                    }
                }
            }
        }

        if (partitionToMetadataMap.isEmpty()) {
            return;
        }

        try {
            m_consumer.commitSync(partitionToMetadataMap);
            m_lastCommitTime = EstTime.currentTimeMillis();
            return;
        } catch (WakeupException e) {
            //committing while being shut down. retry...
            try {
                m_consumer.commitSync(partitionToMetadataMap);
                m_lastCommitTime = EstTime.currentTimeMillis();
            } catch(KafkaException ke) {
                LOGGER.warn("Commit offsets:" + ke.getMessage());
            }
        } catch (CommitFailedException ce) {
            LOGGER.warn("Commit offsets:" + ce.getMessage());
        }
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
                        partitionToMetadataMap.put(partition, new OffsetAndMetadata(safe));
                        committedOffSet.set(safe);
                    }
                }
            } else {
                LOGGER.debug("The topic-partion has been revoked during:" +  partition);
            }
        }

        if (partitionToMetadataMap.isEmpty()) {
            return;
        }

        if (LOGGER.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : partitionToMetadataMap.entrySet()) {
                builder.append(entry.getKey() + ":" + entry.getValue().offset() + ",");
            }
            LOGGER.debug("Committed offsets:" + builder.toString());
        }

        try {
            m_consumer.commitSync(partitionToMetadataMap);
            m_lastCommitTime = EstTime.currentTimeMillis();
            return;
        } catch (WakeupException e) {
            //committing while being shut down. retry...
            try {
                m_consumer.commitSync(partitionToMetadataMap);
                m_lastCommitTime = EstTime.currentTimeMillis();
            } catch(KafkaException ke) {
                LOGGER.warn("Commit offsets:" + ke.getMessage());
            }
        } catch (CommitFailedException ce) {
            LOGGER.warn("Commit offsets:" + ce.getMessage());
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
