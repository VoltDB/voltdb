/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
package org.voltdb.importclient.kafka;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import kafka.api.ConsumerMetadataRequest;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.javaapi.ConsumerMetadataResponse;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import kafka.network.BlockingChannel;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importclient.ImportBaseException;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.CSVInvocation;
import org.voltdb.importer.Invocation;

/**
 * Implementation that imports from a Kafka topic. This is for a single partition of a Kafka topic.
 */
public class KafkaTopicPartitionImporter extends AbstractImporter
{
    private final static PartitionOffsetRequestInfo LATEST_OFFSET =
            new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1);
    private final static PartitionOffsetRequestInfo EARLIEST_OFFSET =
            new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1);

    private final int m_waitSleepMs = 1;
    private final AtomicBoolean m_dead = new AtomicBoolean(false);
    //Start with invalid so consumer will fetch it.
    private final AtomicLong m_currentOffset = new AtomicLong(-1);
    private long m_lastCommittedOffset = -1;
    private final AtomicReference<SimpleConsumer> m_offsetManager = new AtomicReference<SimpleConsumer>();
    private SimpleConsumer m_consumer = null;
    private final TopicAndPartition m_topicAndPartition;
    private final Gap m_gapTracker = new Gap(Integer.getInteger("KAFKA_IMPORT_GAP_LEAD", 32_768));
    private final KafkaStreamImporterConfig m_config;
    private KafkaStreamImporterConfig.HostAndPort m_coordinator;

    public KafkaTopicPartitionImporter(KafkaStreamImporterConfig config)
    {
        m_config = config;
        m_coordinator = m_config.getPartitionLeader();
        m_topicAndPartition = new TopicAndPartition(config.getTopic(), config.getPartition());
    }

    @Override
    public URI getResourceID()
    {
        return m_config.getResourceID();
    }

    //Find leader for the topic+partition.
    private PartitionMetadata findLeader() {
        PartitionMetadata returnMetaData = null;
        loop:
            for (KafkaStreamImporterConfig.HostAndPort broker : m_config.getBrokers()) {
                SimpleConsumer consumer = null;
                try {
                    consumer = new SimpleConsumer(broker.getHost(), broker.getPort(), m_config.getSocketTimeout(), m_config.getFetchSize(), "findLeader");

                    List<String> topics = singletonList(m_topicAndPartition.topic());
                    TopicMetadataRequest req = new TopicMetadataRequest(topics);
                    kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                    List<TopicMetadata> metaData = resp.topicsMetadata();
                    for (TopicMetadata item : metaData) {
                        for (PartitionMetadata part : item.partitionsMetadata()) {
                            if (part.partitionId() == m_topicAndPartition.partition()) {
                                returnMetaData = part;
                                break loop;
                            }
                        }
                    }
                } catch (Exception e) {
                    error(e, "Error in finding leader for %s", m_topicAndPartition);
                } finally {
                    KafkaStreamImporterConfig.closeConsumer(consumer);
                }
            }
        if (returnMetaData == null) {
            error(null, "Failed to find Leader for %s", m_topicAndPartition);
        }
        return returnMetaData;
    }

    //Find leader for this topic partition.
    private KafkaStreamImporterConfig.HostAndPort findNewLeader() {
        for (int i = 0; i < 3; i++) {
            boolean shouldSleep = false;
            PartitionMetadata metadata = findLeader();
            if (metadata == null) {
                shouldSleep = true;
            } else if (metadata.leader() == null) {
                shouldSleep = true;
            } else if (m_config.getPartitionLeader().getHost().equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                shouldSleep = true;
            } else {
                return new KafkaStreamImporterConfig.HostAndPort(metadata.leader().host(), metadata.leader().port());
            }
            if (shouldSleep) {
                backoffSleep(i+1);
            }
        }
        //Unable to find return null for recheck.
        error(null, "Failed to find new leader for %s", m_topicAndPartition);
        return null;
    }

    public void getOffsetCoordinator() {
        KafkaStreamImporterException probeException = null;
        int correlationId = 0;

        OUTER: for (int attempts = 0; attempts < 3; ++attempts) {
            for (KafkaStreamImporterConfig.HostAndPort hp: m_config.getBrokers()) {
                BlockingChannel channel = null;
                try {
                    channel = new BlockingChannel(hp.getHost(), hp.getPort(),
                            BlockingChannel.UseDefaultBufferSize(), BlockingChannel.UseDefaultBufferSize(), m_config.getSocketTimeout());
                    channel.connect();
                    channel.send(new ConsumerMetadataRequest(m_config.getGroupId(), ConsumerMetadataRequest.CurrentVersion(), correlationId++, KafkaStreamImporterConfig.CLIENT_ID));
                    ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());
                    if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
                        Broker offsetManager = metadataResponse.coordinator();
                        m_coordinator = new KafkaStreamImporterConfig.HostAndPort(offsetManager.host(), offsetManager.port());
                        SimpleConsumer consumer = m_offsetManager.getAndSet(
                                new SimpleConsumer(
                                        m_coordinator.getHost(),
                                        m_coordinator.getPort(),
                                        m_config.getSocketTimeout(),
                                        m_config.getFetchSize(), KafkaStreamImporterConfig.CLIENT_ID
                                        ));
                        info(null, "Offset Coordinator for " + m_topicAndPartition + " is " + offsetManager);
                        KafkaStreamImporterConfig.closeConsumer(consumer);
                        probeException = null;
                        consumer = null;
                        break OUTER;
                    }
                    probeException = new KafkaStreamImporterException("Failed to get Offset Coordinator for %s",
                            ErrorMapping.exceptionFor(metadataResponse.errorCode()), m_topicAndPartition
                            );
                } catch (Exception e) {
                    probeException = new KafkaStreamImporterException(
                            "Failed to get Offset Coordinator for %s", e, m_topicAndPartition
                            );
                } finally {
                    if (channel != null) {
                        channel.disconnect();
                    }
                }
            }
            if (probeException != null) {
                warn(probeException, "Failed to query all brokers for the offset coordinator for " + m_topicAndPartition);
            }
            backoffSleep(attempts+1);
        }
    }

    private OffsetResponse getTopicOffset(PartitionOffsetRequestInfo pori) {
        final int partition = m_topicAndPartition.partition();
        final String topic = m_topicAndPartition.topic();

        kafka.javaapi.OffsetRequest earlyRq = new kafka.javaapi.OffsetRequest(
                singletonMap(m_topicAndPartition, pori),
                kafka.api.OffsetRequest.CurrentVersion(), KafkaStreamImporterConfig.CLIENT_ID
                );
        OffsetResponse response = null;
        Throwable fault = null;

        for (int attempts = 0; attempts < 3; ++attempts) try {
            response = m_consumer.getOffsetsBefore(earlyRq);
            if (response.hasError()) {
                short code = response.errorCode(topic, partition);
                fault = ErrorMapping.exceptionFor(code);
                resetLeader();
            } else {
                return response;
            }
        } catch (Exception e) {
            if (e instanceof IOException) {
                resetLeader();
            }
            fault = e;
        }
        if (fault != null) {
            error(fault, "unable to fetch earliest offset for " + m_topicAndPartition);
            response = null;
        }
        return response;
    }

    private OffsetFetchResponse getClientTopicOffset() {
        final short version = 1;
        final OffsetFetchRequest rq = new OffsetFetchRequest(
                m_config.getGroupId(), singletonList(m_topicAndPartition), version, 1, KafkaStreamImporterConfig.CLIENT_ID
                );
        OffsetFetchResponse rsp = null;
        Throwable fault = null;

        for (int attempts = 0; attempts < 3; ++attempts) try {
            rsp = m_offsetManager.get().fetchOffsets(rq);
            short code = rsp.offsets().get(m_topicAndPartition).error();
            if (code != ErrorMapping.NoError()) {
                fault = ErrorMapping.exceptionFor(code);
                if (code == ErrorMapping.NotCoordinatorForConsumerCode()) {
                    getOffsetCoordinator();
                } else if (code == ErrorMapping.UnknownTopicOrPartitionCode()) {
                    fault = null;
                    break;
                }
            } else {
                fault = null;
                break;
            }
        } catch (Exception e) {
            if (e instanceof IOException) {
                getOffsetCoordinator();
            }
            fault = e;
        }
        if (fault != null) {
            error(fault, "unable to fetch earliest offset for " + m_topicAndPartition);
                    rsp = null;
        }
        return rsp;
    }

    public long getLastOffset() {

        final int partition = m_topicAndPartition.partition();
        final String topic = m_topicAndPartition.topic();

        OffsetResponse response = getTopicOffset(EARLIEST_OFFSET);
        if (response == null) return -1L;

        long earliest = response.offsets(topic, partition)[0];

        response = getTopicOffset(LATEST_OFFSET);
        if (response == null) return -1L;

        long latest = response.offsets(topic, partition)[0];
        if (latest == earliest) return latest;

        OffsetFetchResponse ofr = getClientTopicOffset();
        if (ofr == null) return earliest;

        long current = ofr.offsets().get(m_topicAndPartition).offset();
        if (current < earliest) return earliest;

        if (current < latest) return current;

        return latest;
    }

    //Sleep with backoff.
    private int backoffSleep(int fetchFailedCount) {
        try {
            Thread.sleep(1000 * fetchFailedCount++);
            if (fetchFailedCount > 10) fetchFailedCount = 1;
        } catch (InterruptedException ie) {
        }
        return fetchFailedCount;
    }

    private void resetLeader() {
        KafkaStreamImporterConfig.closeConsumer(m_consumer);
        m_consumer = null;
        KafkaStreamImporterConfig.HostAndPort leaderBroker = findNewLeader();
        if (leaderBroker == null) {
            //point to original leader which will fail and we fall back again here.
            error(null, "Fetch Failed to find leader continue with old leader: %s", m_config.getPartitionLeader().toString());
            leaderBroker = m_config.getPartitionLeader();
        } else {
            if (!leaderBroker.equals(m_config.getPartitionLeader())) {
                info(null, "Fetch Found new leader for " + m_topicAndPartition + " New Leader: " + leaderBroker);
                m_config.setPartitionLeader(leaderBroker);
            }
        }
        m_consumer = new SimpleConsumer(
                leaderBroker.getHost(), leaderBroker.getPort(),
                m_config.getSocketTimeout(), m_config.getFetchSize(), KafkaStreamImporterConfig.CLIENT_ID
                );
    }

    @Override
    protected void accept() {
        info(null, "Starting partition fetcher for " + m_topicAndPartition);
        long submitCount = 0;
        AtomicLong cbcnt = new AtomicLong(0);
        try {
            //Start with the starting leader.
            resetLeader();

            int sleepCounter = 1;
            while (shouldRun()) {
                if (m_currentOffset.get() < 0) {
                    getOffsetCoordinator();
                    if (m_offsetManager.get() == null) {
                        sleepCounter = backoffSleep(sleepCounter);
                        continue;
                    }
                    long lastOffset = getLastOffset();

                    m_gapTracker.resetTo(lastOffset);
                    m_lastCommittedOffset = lastOffset;

                    m_currentOffset.set(lastOffset);
                    if (m_currentOffset.get() < 0) {
                        //If we dont know the offset get it backoff if we fail.
                        sleepCounter = backoffSleep(sleepCounter);
                        info(null, "No valid offset found for " + m_topicAndPartition);
                        continue;
                    }
                    info(null, "Starting offset for " + m_topicAndPartition + " is " + m_currentOffset.get());
                }
                long currentFetchCount = 0;
                //Build fetch request of we have a valid offset and not too many are pending.
                FetchRequest req = new FetchRequestBuilder().clientId(KafkaStreamImporterConfig.CLIENT_ID)
                        .addFetch(m_topicAndPartition.topic(),
                                m_topicAndPartition.partition(), m_currentOffset.get(), m_config.getFetchSize())
                                .build();
                FetchResponse fetchResponse = null;
                try {
                    fetchResponse = m_consumer.fetch(req);
                    if (fetchResponse == null) {
                        sleepCounter = backoffSleep(sleepCounter);
                        continue;
                    }
                } catch (Exception ex) {
                    error(ex, "Failed to fetch from %s", m_topicAndPartition);
                    //See if its network error and find new leader for this partition.
                    if (ex instanceof IOException) {
                        resetLeader();
                        //find leader in resetLeader would sleep and backoff
                        continue;
                    }
                    sleepCounter = backoffSleep(sleepCounter);
                    continue;
                }

                if (fetchResponse.hasError()) {
                    // Something went wrong!
                    short code = fetchResponse.errorCode(m_topicAndPartition.topic(), m_topicAndPartition.partition());
                    warn(ErrorMapping.exceptionFor(code), "Failed to fetch messages for %s", m_topicAndPartition);
                    sleepCounter = backoffSleep(sleepCounter);
                    if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                        // We asked for an invalid offset. For simple case ask for the last element to reset
                        info(null, "Invalid offset requested for " + m_topicAndPartition);
                        getOffsetCoordinator();
                        m_currentOffset.set(-1L);
                        continue;
                    }
                    resetLeader();
                    continue;
                }
                sleepCounter = 1;
                for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(m_topicAndPartition.topic(), m_topicAndPartition.partition())) {
                    //You may be catchin up so dont sleep.
                    currentFetchCount++;
                    long currentOffset = messageAndOffset.offset();
                    //if currentOffset is less means we have already pushed it and also check pending queue.
                    if (currentOffset < m_currentOffset.get()) {
                        continue;
                    }
                    ByteBuffer payload = messageAndOffset.message().payload();

                    String line = new String(payload.array(),payload.arrayOffset(),payload.limit(),StandardCharsets.UTF_8);
                    CSVInvocation invocation = new CSVInvocation(m_config.getProcedure(), line, m_config.getSeparator());
                    TopicPartitionInvocationCallback cb = new TopicPartitionInvocationCallback(
                            messageAndOffset.nextOffset(), cbcnt, m_gapTracker, m_dead,
                            invocation);
                    if (!callProcedure(invocation, cb)) {
                        if (isDebugEnabled()) {
                            debug(null, "Failed to process Invocation possibly bad data: " + line);
                        }
                        m_gapTracker.commit(currentOffset);
                    }
                    submitCount++;
                    m_currentOffset.set(messageAndOffset.nextOffset());
                    if (!shouldRun()) {
                        break;
                    }
                }
                if (!shouldRun()) {
                    break;
                }

                //wait to fetch more if we read nothing last time.
                if (currentFetchCount == 0) {
                        try {
                            Thread.sleep(m_waitSleepMs);
                        } catch (InterruptedException ie) {
                        }
                }
                commitOffset();
            }
        } catch (Exception ex) {
            error(ex, "Failed to start topic partition fetcher for " + m_topicAndPartition);
        } finally {
            commitOffset();
            KafkaStreamImporterConfig.closeConsumer(m_consumer);
            m_consumer = null;
            KafkaStreamImporterConfig.closeConsumer(m_offsetManager.getAndSet(null));
        }
        m_dead.compareAndSet(false, true);
        info(null, "Partition fetcher stopped for " + m_topicAndPartition
                + " Last commit point is: " + m_lastCommittedOffset
                + " Callback Rcvd: " + cbcnt.get()
                + " Submitted: " + submitCount);
    }

    public boolean commitOffset() {
        final int correlationId = m_topicAndPartition.partition();
        final short version = 1;

        final long safe = m_gapTracker.commit(-1L);
        if (safe > m_lastCommittedOffset) {

            OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest(
                    m_config.getGroupId(),
                    singletonMap(m_topicAndPartition, new OffsetAndMetadata(safe, "commit", ErrorMapping.NoError())),
                    correlationId,
                    KafkaStreamImporterConfig.CLIENT_ID,
                    version
                    );
            OffsetCommitResponse offsetCommitResponse = null;
            try {
                SimpleConsumer consumer = m_offsetManager.get();
                if (consumer == null) {
                    getOffsetCoordinator();
                    consumer = m_offsetManager.get();
                }
                if (consumer != null) {
                    offsetCommitResponse = consumer.commitOffsets(offsetCommitRequest);
                    final short code = ((Short) offsetCommitResponse.errors().get(m_topicAndPartition));
                    if (code == ErrorMapping.NotCoordinatorForConsumerCode()) {
                        info(null, "Not coordinator for committing offset for " + m_topicAndPartition + " Updating coordinator.");
                        getOffsetCoordinator();
                        consumer = m_offsetManager.get();
                        if (consumer != null) {
                            offsetCommitResponse = consumer.commitOffsets(offsetCommitRequest);
                        }
                    }
                } else {
                    error(null, "Commit Offset Failed to get offset coordinator for " + m_topicAndPartition);
                    return false;
                }
            } catch (Exception e) {
                error(e, "Failed to commit Offset for " + m_topicAndPartition);
                return false;
            }
            final short code = ((Short) offsetCommitResponse.errors().get(m_topicAndPartition));
            if (code != ErrorMapping.NoError()) {
                final String msg = "Commit Offset Failed to commit for " + m_topicAndPartition;
                error(ErrorMapping.exceptionFor(code), msg);
                return false;
            }
            m_lastCommittedOffset = safe;
        }

        return true;
    }

    final class Gap {
        long c = 0;
        long s = -1L;
        final long [] lag;

        Gap(int leeway) {
            if (leeway <= 0) {
                throw new IllegalArgumentException("leeways is zero or negative");
            }
            lag = new long[leeway];
        }

        synchronized void submit(long offset) {
            if (s == -1L && offset >= 0) {
                lag[idx(offset)] = c = s = offset;
            }
            if (offset > s) {
                s = offset;
            }
        }

        private final int idx(long offset) {
            return (int)offset % lag.length;
        }

        synchronized void resetTo(long offset) {
            if (offset < 0) {
                throw new IllegalArgumentException("offset is negative");
            }
            lag[idx(offset)] = s = c = offset;
        }

        synchronized long commit(long offset) {
            if (offset <= s && offset > c) {
                int ggap = (int)Math.min(lag.length, offset-c);
                if (ggap == lag.length) {
                    warn(
                              null, "Gap tracker moving topic commit point from %d to %d for "
                              + m_topicAndPartition, c, (offset - lag.length + 1)
                            );
                    c = offset - lag.length + 1;
                    lag[idx(c)] = c;
                }
                lag[idx(offset)] = offset;
                while (ggap > 0 && lag[idx(c)]+1 == lag[idx(c+1)]) {
                    ++c;
                }
            }
            return c;
        }
    }

    public class KafkaStreamImporterException extends ImportBaseException {
        private static final long serialVersionUID = 7668280657393399984L;

        public KafkaStreamImporterException() {
        }

        public KafkaStreamImporterException(String format, Object... args) {
            super(format, args);
        }

        public KafkaStreamImporterException(Throwable cause) {
            super(cause);
        }

        public KafkaStreamImporterException(String format, Throwable cause,
                Object... args) {
            super(format, cause, args);
        }
    }

    @Override
    public String getName()
    {
        return "KafkaImporter";
    }

    @Override
    protected void stop()
    {
        // Nothing to stop. shouldRun() should take care of exiting the work loop.
    }

    //Per topic per partition that we are responsible for.
    //Callback for each invocation we have submitted.
    private final static class TopicPartitionInvocationCallback implements ProcedureCallback
    {
        private final long m_offset;
        private final AtomicLong m_cbcnt;
        private final Gap m_tracker;
        private final AtomicBoolean m_dontCommit;
        private final Invocation m_invocation;

        public TopicPartitionInvocationCallback(
                final long offset,
                final AtomicLong cbcnt,
                final Gap tracker,
                final AtomicBoolean dontCommit,
                final Invocation invocation) {
            m_offset = offset;
            m_cbcnt = cbcnt;
            m_tracker = tracker;
            m_dontCommit = dontCommit;
            m_tracker.submit(m_offset);
            m_invocation = invocation;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {

            m_cbcnt.incrementAndGet();
            if (!m_dontCommit.get()) {
                m_tracker.commit(m_offset);
            }
        }

        @SuppressWarnings("unused")
        public Invocation getInvocation() {
            return m_invocation;
        }
    }
}
