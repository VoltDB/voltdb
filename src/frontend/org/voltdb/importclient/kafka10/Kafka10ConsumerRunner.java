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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
import org.voltdb.importclient.kafka.util.PendingWorkTracker;
import org.voltdb.importer.CommitTracker;
import org.voltdb.importer.ImporterLifecycle;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;
import org.voltdb.importer.formatter.FormatterBuilder;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public abstract class Kafka10ConsumerRunner implements Runnable {

    protected Consumer<byte[], byte[]> m_consumer;
    protected Kafka10StreamImporterConfig m_config;
    protected ImporterLifecycle m_lifecycle;

    private final Map<TopicPartition, AtomicLong> m_currentOffSets = new HashMap<TopicPartition, AtomicLong>();
    private final Map<TopicPartition, AtomicLong> m_lastCommittedOffSets = new HashMap<TopicPartition, AtomicLong>();
    private final Map<TopicPartition, CommitTracker> m_trackerMap = new HashMap<TopicPartition, CommitTracker>();

    private static final VoltLogger LOGGER = new VoltLogger("KAFKAIMPORTER");

    private final int m_waitSleepMs = 1;
    private final AtomicBoolean m_done = new AtomicBoolean(false);

    private final Map<String, Formatter>  m_formatters = new HashMap<String, Formatter>();

    //for commit policies.
    private long m_lastCommitTime = 0;

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
                List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
                topicPartitions.addAll(partitions);
                commitOffsets(topicPartitions);

                //remove Tracker
                for (TopicPartition partition : partitions) {
                    m_trackerMap.remove(partition);
                    m_lastCommittedOffSets.remove(partition);
                    m_currentOffSets.remove(partition);
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                LOGGER.info("Partitions assigned: " + partitions);
                for (TopicPartition partition : partitions) {
                    m_trackerMap.put(partition,
                            new DurableTracker(Integer.getInteger("KAFKA_IMPORT_GAP_LEAD", 32_768), partition.topic(), partition.partition()));

                    OffsetAndMetadata offsetAndMetaData = m_consumer.committed(partition);
                    long startOffset = offsetAndMetaData != null ? offsetAndMetaData.offset() : -1L;
                    m_lastCommittedOffSets.put(partition, new AtomicLong(startOffset));
                    if(startOffset >= 0) {
                        m_consumer.seek(partition, startOffset);
                    }
                    m_currentOffSets.put(partition, new AtomicLong(startOffset));
                }
            }
        });
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
        PendingWorkTracker pendingWorkTracker = new PendingWorkTracker();
        long submitCount = 0;
        try {
            subscribe();
            int sleepCounter = 1;
            while (m_lifecycle.shouldRun()) {
                try {

                    //move to correct offsets
                    for(Map.Entry<TopicPartition, AtomicLong> entry : m_lastCommittedOffSets.entrySet()) {
                        long lastCommittedOffset = entry.getValue().longValue();
                        if (lastCommittedOffset > -1L) {
                            m_consumer.seek(entry.getKey(), lastCommittedOffset);
                        }
                        AtomicLong currentOffset = m_currentOffSets.get(entry.getKey());
                        currentOffset.set(lastCommittedOffset);
                    }

                    ConsumerRecords<byte[], byte[]> records = m_consumer.poll(m_config.getConsumerTimeoutMillis());

                    //wait if nothing fetched last time.
                    if (records.isEmpty()) {
                        try { Thread.sleep(m_waitSleepMs);}
                        catch (InterruptedException ie) {}
                        continue;
                    }
                    List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
                    for (TopicPartition partition : records.partitions()) {
                        Formatter formatter = getFormatter(partition.topic());
                        List<ConsumerRecord<byte[], byte[]>> messages = records.records(partition);
                        int count = messages.size();
                        int partitionSubmittedCount = 0;
                        AtomicLong currentOffset = m_currentOffSets.get(partition);
                        for (int i = 0; i < count; i++) {
                            ConsumerRecord<byte[], byte[]> record = messages.get(i);
                            byte[] msg  = record.value();
                            long offset = record.offset();

                            //if currentOffset is less means we have already pushed it and also check pending queue.
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

                            String smsg = new String(msg, "UTF-8");
                            Object params[] = null;
                            try {
                                if (formatter != null) {
                                    params = formatter.transform(ByteBuffer.wrap(msg));
                                } else {
                                    params = csvParser.parseLine(smsg);
                                }
                                getDurabaleTracker(partition).submit(nextOffSet);

                                submitCount++;
                                if (m_lifecycle.hasTransaction()) {
                                    ProcedureCallback cb = new InvocationCallback(offset,
                                            nextOffSet, pendingWorkTracker, getDurabaleTracker(partition), m_done);
                                    partitionSubmittedCount++;
                                    if (invoke(smsg, offset,partition.topic(), params, cb)) {
                                        pendingWorkTracker.produceWork();
                                    }
                                } else {
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("Failed to process Invocation possibly bad data: " + Arrays.toString(params));
                                    }
                                    getDurabaleTracker(partition).commit(nextOffSet);
                                }
                            } catch (FormatException | IOException e) {
                                LOGGER.warn("Failed to transform message " + smsg + " at offset " + offset + ", error message: " + e.getMessage());
                                getDurabaleTracker(partition).commit(nextOffSet);
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
        builder.append("Callback Rcvd: " + pendingWorkTracker.getCallbackCount());
        builder.append("Submitted: " + submitCount);
        m_lastCommittedOffSets.entrySet().stream().forEach(e-> builder.append("\npartition:" + e.getKey() + " last commit:" + e.getValue().get()));
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

    private CommitTracker getDurabaleTracker(TopicPartition partition) {
        return m_trackerMap.get(partition);
    }

    private void commitOffsets(List<TopicPartition> topicPartitions) {
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (TopicPartition partition : topicPartitions) {
            long safe = getDurabaleTracker(partition).commit(-1L);
            long lastCommittedOffset = m_lastCommittedOffSets.get(partition).longValue();
            if (safe > lastCommittedOffset) {
                partitionToMetadataMap.put(partition, new OffsetAndMetadata(safe + 1));
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("committing offsets:" +  partitionToMetadataMap);
            }
        }

        if (!partitionToMetadataMap.isEmpty()) {
            m_consumer.commitSync(partitionToMetadataMap);
            m_lastCommitTime = EstTime.currentTimeMillis();
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
