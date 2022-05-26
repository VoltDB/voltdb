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
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.EstTime;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importclient.kafka.util.DurableTracker;
import org.voltdb.importclient.kafka.util.KafkaCommitPolicy;
import org.voltdb.importclient.kafka.util.KafkaConstants;
import org.voltdb.importclient.kafka.util.KafkaUtils;
import org.voltdb.importclient.kafka.util.PendingWorkTracker;
import org.voltdb.importclient.kafka.util.ProcedureInvocationCallback;
import org.voltdb.importclient.kafka.util.SimpleTracker;
import org.voltdb.importer.CommitTracker;
import org.voltdb.importer.ImporterLifecycle;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;
import org.voltdb.importer.formatter.FormatterBuilder;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public abstract class KafkaConsumerRunner implements Runnable {

    private static final VoltLogger LOGGER = new VoltLogger("KAFKAIMPORTER");

    protected Consumer<ByteBuffer, ByteBuffer> m_consumer;
    protected final KafkaStreamImporterConfig m_config;
    protected final ImporterLifecycle m_lifecycle;

    private final AtomicReference<Map<TopicPartition, AtomicLong>> m_lastCommittedOffSets = new AtomicReference<>();
    private final AtomicReference<Map<TopicPartition, CommitTracker>> m_trackerMap = new AtomicReference<>();
    private final Map<TopicPartition, AtomicLong> m_lastSeekedOffSets = new ConcurrentHashMap<>();
    private final Map<TopicPartition, AtomicLong> m_pauseOffsets = new ConcurrentHashMap<>();
    private final Map<TopicPartition, PendingWorkTracker> m_workTrackers = new ConcurrentHashMap<>();

    protected final AtomicBoolean m_done = new AtomicBoolean(false);

    //Formatter by topic and partition.
    private final Map<String, Formatter>  m_formatters = new HashMap<>();

    //for commit policies.
    private long m_lastCommitTime = 0;
    private final int m_waitSleepMs = 10;
    private final ReentrantLock m_offsetLock = new ReentrantLock();

    public KafkaConsumerRunner(ImporterLifecycle lifecycle, KafkaStreamImporterConfig config, Consumer<ByteBuffer, ByteBuffer> consumer) throws Exception {
        m_lifecycle = lifecycle;
        m_consumer = consumer;
        m_config = config;
        m_lastCommittedOffSets.set(new HashMap<>());
        m_trackerMap.set(new HashMap<>());
    }

    protected void subscribe() {
        m_consumer.subscribe(Arrays.asList(m_config.getTopics().split(",")), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                if (partitions.isEmpty()) {
                    return;
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Consumer group " + m_config.getGroupId() + " drops topic/partitions:" + partitions);
                }

                //This method is called before a rebalance operation starts and after the consumer stops fetching data.
                //So commit offsets for the partitions before they are revoked
                commitOffsets(partitions.stream().collect(Collectors.toList()));

                Map<TopicPartition, CommitTracker> trackers = new HashMap<>();
                trackers.putAll(m_trackerMap.get());

                Map<TopicPartition, AtomicLong> lastCommittedOffSets = new HashMap<>();
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
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Consumer group " + m_config.getGroupId() + " is assigned with topic/partition" + partitions);
                }
            }
        });
    }

    //add trackers for new topic-partition in this importer
    private void calculateTrackers(Collection<TopicPartition> partitions) {

        Map<TopicPartition, CommitTracker> trackers = new HashMap<>();
        trackers.putAll(m_trackerMap.get());

        Map<TopicPartition, AtomicLong> lastCommittedOffSets = new HashMap<>();
        lastCommittedOffSets.putAll(m_lastCommittedOffSets.get());

        boolean newTopicPartition = false;
        for (TopicPartition partition : partitions) {
            if (m_trackerMap.get().get(partition) != null) {
                continue;
            }
            newTopicPartition = true;
            long startOffset = -1L;
            CommitTracker commitTracker = null;
            if (m_config.getCommitPolicy() == KafkaCommitPolicy.TIME && m_config.getTriggerValue() > 0) {
                commitTracker = new SimpleTracker();
            } else {
                commitTracker = new DurableTracker(KafkaConstants.IMPORT_GAP_LEAD, partition.topic(),
                        partition.partition(), m_config.getGroupId());
            }
            trackers.put(partition, commitTracker);
            try {
                OffsetAndMetadata offsetAndMetaData = m_consumer.committed(partition);
                startOffset = offsetAndMetaData != null ? offsetAndMetaData.offset() : -1L;
                if (startOffset > -1L) {
                    commitTracker.resetTo(startOffset);
                }
            } catch (WakeupException we) {
                if (m_done.get()) {
                    return;
                }
            } catch (KafkaException e) {
                if (m_done.get()) {
                    LOGGER.info("Failed to read committed offsets for group " + m_config.getGroupId() + partition + " "
                            + e);
                } else {
                    LOGGER.error("Failed to read committed offsets for group " + m_config.getGroupId() + partition, e);
                }
            }
            lastCommittedOffSets.put(partition, new AtomicLong(startOffset));
            m_pauseOffsets.put(partition, new AtomicLong(-1));
            m_workTrackers.put(partition, new PendingWorkTracker());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Starting offset for group:" + m_config.getGroupId() + ":" + startOffset + " partition:" + partition);
            }
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Shutdown Kafka consumer for group " + m_config.getGroupId());
        }
        m_done.set(true);
        try {
            m_consumer.wakeup();
        }  catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Kafka wakeup interruption while cleaning up Kafka consumer:" + e.getMessage());
            }
        }
    }

    public abstract boolean invoke(String rawMessage, long offset, String topic, Object[] params, ProcedureCallback procedureCallback) throws Exception;

    @Override
    public void run() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Starting Kafka consumer for group:" + m_config.getGroupId() + " topics:" + m_config.getTopics());
        }
        List<TopicPartition> seekList = new ArrayList<>();
        Map<TopicPartition, AtomicLong> submitCounts = new HashMap<>();
        CSVParser csvParser = new CSVParser();
        try {
            subscribe();
            int sleepCounter = 1;
            while (m_lifecycle.shouldRun()) {
                try {
                    seek(seekList);
                    seekList.clear();

                    //The consumer will poll messages from earliest or the committed offset on the first polling.
                    //The messages in next poll starts at the largest offset + 1 in the previous polled messages.
                    //Every message is polled only once.
                    ConsumerRecords<ByteBuffer, ByteBuffer> records = null;
                    if (!m_lifecycle.shouldRun()) {
                        break;
                    }
                    try {
                        records = m_consumer.poll(m_config.getPollTimeout());
                    } catch (WakeupException we) {
                        if (m_done.get()) {
                            break;
                        }
                    } catch (Throwable t) {
                        if (m_done.get()) {
                            break;
                        }
                        LOGGER.warn("Errors seen when polling data from Kafka:" + t.getMessage());
                    }

                    if (records == null || records.isEmpty()) {
                        List<TopicPartition> topicPartitions = m_lastCommittedOffSets.get().keySet().stream().collect(Collectors.toList());
                        commitOffsets(topicPartitions);
                        try { Thread.sleep(m_waitSleepMs);}
                        catch (InterruptedException ie) {}
                        continue;
                    }
                    calculateTrackers(records.partitions());
                    List<TopicPartition> topicPartitions = new ArrayList<>();
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

                        AtomicLong submitCount = submitCounts.get(partition);
                        if (submitCount == null) {
                            submitCount = new AtomicLong(0);
                            submitCounts.put(partition, submitCount);
                        }

                        String topicIdentifer = "group " + m_config.getGroupId() + "-" + partition;
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
                                seekList.add(partition);
                                break;
                            }

                            //The Kafka message offsets are consecutive numbers. Kafka messages come in ascending order by the offsets
                            //A message record in Kafka 10, not as Kafka 8, does not have the offset of next message.
                            //Thus get next offset from next message for the purpose of tracking. The next offset for the last message in the batch
                            // will be the offset of the message + 1.
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
                                submitCount.incrementAndGet();
                                ProcedureCallback cb = new ProcedureInvocationCallback(offset, nextOffSet, m_workTrackers.get(partition),
                                        commitTracker, m_done, m_pauseOffsets.get(partition),
                                        topicIdentifer);
                                partitionSubmittedCount++;

                                if (invoke(smsg, offset, partition.topic(), params, cb)) {
                                    m_workTrackers.get(partition).produceWork();
                                } else {
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("Failed to process. possibly bad data: " + Arrays.toString(params));
                                    }
                                    commitTracker.commit(nextOffSet);
                                }
                            } catch (FormatException | IOException e) {
                                partitionSubmittedCount++;
                                LOGGER.rateLimitedLog(KafkaConstants.LOG_SUPPRESSION_INTERVAL_SECONDS, Level.WARN, e,
                                        "Failed to tranform data: %s" , Arrays.toString(params));
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
                    if (!topicPartitions.isEmpty() &&
                            KafkaCommitPolicy.shouldCommit(m_config.getCommitPolicy(), m_config.getTriggerValue(), m_lastCommitTime)) {
                        commitOffsets(topicPartitions);
                    }
                } catch(KafkaException ex) {
                    LOGGER.error("Error seen when processing message " + m_config.getTopics(), ex);
                    if (m_done.get()) {
                        break;
                    }
                    sleepCounter = KafkaUtils.backoffSleep(sleepCounter);
                    continue;
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to start topic partition fetcher for " + m_config.getTopics(), e);
        } finally {
            try {
                commitPauseOffsets();
                m_consumer.close();
                m_consumer = null;
            } catch (Exception ignore) {
                //ignore
            }
        }

        m_done.set(true);
        if (LOGGER.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder("Import detail for group " + m_config.getGroupId());
            builder.append(" \n         Submitted Counts:" + m_workTrackers);
            builder.append(" \n         Committed Offsets: " + m_lastCommittedOffSets.get());
            LOGGER.debug(builder.toString());
        }
    }

    //Move offsets to correct positions for next poll
    private void seek(List<TopicPartition> seekList) {
        for (TopicPartition tp : seekList) {
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
                    LOGGER.debug("Moves offset for group " +  m_config.getGroupId() + " -" + tp + " to " + lastCommittedOffset);
                }
            }
        }
    }

    private CommitTracker getCommitTracker(TopicPartition partition) {
        return m_trackerMap.get().get(partition);
    }

    private void commitPauseOffsets() {
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();
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
            if (skipCommit) {
                continue;
            }
            CommitTracker commitTracker = getCommitTracker(entry.getKey());
            if (commitTracker == null) {
                continue;
            }
            long safe = commitTracker.getSafe();
            //nothing safe to commit
            if (safe < 0) {
                continue;
            }
            AtomicLong committedOffSet = m_lastCommittedOffSets.get().get(entry.getKey());
            if (committedOffSet != null && committedOffSet.get() != pausedOffSet && (safe > committedOffSet.get() || pausedOffSet != -1)) {
                if (pausedOffSet != -1) {
                    safe = Math.min(pausedOffSet, safe);
                }
                partitionToMetadataMap.put(entry.getKey(), new OffsetAndMetadata(safe));
                committedOffSet.set(safe);
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Consumer group " + m_config.getGroupId() + " committs offsets upon pausing.");
        }
        commit(partitionToMetadataMap);
    }

    private void commitOffsets(List<TopicPartition> topicPartitions) {
        m_offsetLock.lock();
        try {
            Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();
            for (TopicPartition partition : topicPartitions) {
                CommitTracker commitTracker = getCommitTracker(partition);
                if (commitTracker != null) {
                    long safe = commitTracker.getSafe();
                    //nothing safe to commit;
                    if (safe < 0) {
                        continue;
                    }
                    AtomicLong committedOffSet = m_lastCommittedOffSets.get().get(partition);
                    if (committedOffSet != null) {
                        long lastCommittedOffset = committedOffSet.longValue();
                        if (safe > lastCommittedOffset) {
                            partitionToMetadataMap.put(partition, new OffsetAndMetadata(safe));
                            committedOffSet.set(safe);
                        }
                    }
                } else {
                    LOGGER.debug("Consumner group " + m_config.getGroupId() + " does't have" + partition + " assigned any more.");
                }
            }
            commit(partitionToMetadataMap);
        } finally {
            m_offsetLock.unlock();
        }
    }

    private void commit(Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap) {

        if (partitionToMetadataMap.isEmpty()) {
            return;
        }

        if (LOGGER.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder("Consumer group " + m_config.getGroupId() + " committs offsets:");
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : partitionToMetadataMap.entrySet()) {
                builder.append(entry.getKey() + ":" + entry.getValue().offset());
            }
            LOGGER.debug(builder.toString());
        }

        try {
            m_consumer.commitSync(partitionToMetadataMap);
            m_lastCommitTime = EstTime.currentTimeMillis();
        } catch (WakeupException e) {
            //committing while being shut down. retry...
            try {
                m_consumer.commitSync(partitionToMetadataMap);
                m_lastCommitTime = EstTime.currentTimeMillis();
            } catch(KafkaException ke) {
                LOGGER.warn("Consumer group " + m_config.getGroupId() + " commit offsets:" + ke.getMessage());
            }
        } catch (CommitFailedException ce) {
            LOGGER.warn("Consumer group " + m_config.getGroupId() + " commit offsets:" + ce.getMessage());
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

    public int getKafkaTopicPartitionCount() {
        int totalPartitions = 0;
        String[] topics = m_config.getTopics().split(",");
        for (String topic : topics) {
            int partitions = 0;
            while (partitions == 0) {
                try {
                    List<PartitionInfo> info = m_consumer.partitionsFor(topic);
                    if (info != null) {
                        partitions = info.size();
                    }
                } catch (Exception e) {
                    LOGGER.rateLimitedLog(KafkaConstants.LOG_SUPPRESSION_INTERVAL_SECONDS, Level.WARN, e, "Failed to get Kafka partition info");
                }
                //paused or shutting down
                if (m_done.get()) {
                    return -1;
                }
            }
            totalPartitions += partitions;
        }
        return totalPartitions;
    }
}
