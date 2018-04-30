/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.voltcore.logging.Level;
import org.voltcore.utils.EstTime;
import org.voltdb.importclient.kafka.util.DurableTracker;
import org.voltdb.importclient.kafka.util.HostAndPort;
import org.voltdb.importclient.kafka.util.KafkaConstants;
import org.voltdb.importclient.kafka.util.KafkaCommitPolicy;
import org.voltdb.importclient.kafka.util.KafkaUtils;
import org.voltdb.importclient.kafka.util.PendingWorkTracker;
import org.voltdb.importclient.kafka.util.ProcedureInvocationCallback;
import org.voltdb.importclient.kafka.util.SimpleTracker;
import org.voltdb.importer.CommitTracker;
import org.voltdb.importer.ImporterLifecycle;
import org.voltdb.importer.ImporterLogger;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;

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

/*
 * This base class provides the implementation for manual offset management of a single Kafka partition of a given topic. An instance of this
 * class must provide an implementation of the executeVolt() method, which executes a procedure or database function in the
 * appropriate manner.
 */
public abstract class BaseKafkaTopicPartitionImporter {

    private final static PartitionOffsetRequestInfo LATEST_OFFSET =
            new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1);
    private final static PartitionOffsetRequestInfo EARLIEST_OFFSET =
            new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1);


    private final int m_waitSleepMs = 1;
    protected final AtomicBoolean m_dead = new AtomicBoolean(false);
    //Start with invalid so consumer will fetch it.
    private final AtomicLong m_currentOffset = new AtomicLong(-1);
    // CAUTION: m_pauseOffset is not reliable until all callbacks have completed.
    protected final AtomicLong m_pauseOffset = new AtomicLong(-1);
    private long m_lastCommittedOffset = -1;
    protected final AtomicReference<BlockingChannel> m_offsetManager = new AtomicReference<>();
    protected SimpleConsumer m_consumer = null;
    public final TopicAndPartition m_topicAndPartition;
    protected final CommitTracker m_gapTracker;
    protected final KafkaStreamImporterConfig m_config;
    private HostAndPort m_coordinator;
    private final FetchRequestBuilder m_fetchRequestBuilder;

    //Counters for commit policies.
    private long m_lastCommitTime = 0;

    protected ImporterLifecycle m_lifecycle;
    protected ImporterLogger m_logger;

    /*
     * Submit the supplied data to the database. Subclasses override this method with the appropriate operations.
     */
    public abstract boolean invoke(Object[] params, ProcedureInvocationCallback cb);

    public BaseKafkaTopicPartitionImporter(KafkaStreamImporterConfig config, ImporterLifecycle lifecycle, ImporterLogger logger)
    {
        m_lifecycle = lifecycle;
        m_logger = logger;
        m_config = config;
        m_coordinator = m_config.getPartitionLeader();
        m_topicAndPartition = new TopicAndPartition(config.getTopic(), config.getPartition());
        m_fetchRequestBuilder = new FetchRequestBuilder().clientId(KafkaConstants.CLIENT_ID);
        if (m_config.getCommitPolicy() == KafkaCommitPolicy.TIME && m_config.getTriggerValue() > 0) {
            m_gapTracker = new SimpleTracker();
        }
        else {
            m_gapTracker = new DurableTracker(KafkaConstants.IMPORT_GAP_LEAD, config.getTopic(), config.getPartition());
        }
    }

    public URI getResourceID()  {
        return m_config.getResourceID();
    }


    //Find leader for the topic+partition.
    private PartitionMetadata findLeader() {
        PartitionMetadata returnMetaData = null;
        loop:
            for (HostAndPort broker : m_config.getBrokers()) {
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
                    m_logger.rateLimitedLog(Level.WARN, e, "Error in finding leader for " + m_topicAndPartition);
                } finally {
                    KafkaStreamImporterConfig.closeConsumer(consumer);
                }
            }
        if (returnMetaData == null) {
            m_logger.rateLimitedLog(Level.WARN, null, "Failed to find Leader for " + m_topicAndPartition);
        }
        return returnMetaData;
    }

    private int nextCorrelationId() {
        FetchRequest fr = m_fetchRequestBuilder.addFetch(m_topicAndPartition.topic(),
                m_topicAndPartition.partition(), 1L, m_config.getFetchSize())
                .build();
        return fr.correlationId();
    }

    //Find leader for this topic partition.
    private HostAndPort findNewLeader() {
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
                return new HostAndPort(metadata.leader().host(), metadata.leader().port());
            }
            if (shouldSleep) {
                KafkaUtils.backoffSleep(i+1);
            }
        }
        //Unable to find return null for recheck.
        m_logger.rateLimitedLog(Level.WARN, null, "Failed to find new leader for " + m_topicAndPartition);
        return null;
    }

    public void getOffsetCoordinator() {
        KafkaStreamImporterException probeException = null;

        OUTER: for (int attempts = 0; attempts < 3; ++attempts) {
            for (HostAndPort hp: m_config.getBrokers()) {
                BlockingChannel channel = null;
                try {
                    channel = new BlockingChannel(
                            hp.getHost(), hp.getPort(),
                            BlockingChannel.UseDefaultBufferSize(),
                            BlockingChannel.UseDefaultBufferSize(),
                            m_config.getSocketTimeout()
                            );
                    channel.connect();
                    channel.send(new ConsumerMetadataRequest(
                            m_config.getGroupId(),
                            ConsumerMetadataRequest.CurrentVersion(),
                            nextCorrelationId(),
                            KafkaConstants.CLIENT_ID
                            ));
                    ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());
                    if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
                        Broker offsetManager = metadataResponse.coordinator();
                        m_coordinator = new HostAndPort(offsetManager.host(), offsetManager.port());
                        BlockingChannel consumer = m_offsetManager.getAndSet(
                                new BlockingChannel(
                                        m_coordinator.getHost(), m_coordinator.getPort(),
                                        BlockingChannel.UseDefaultBufferSize(),
                                        BlockingChannel.UseDefaultBufferSize(),
                                        m_config.getSocketTimeout()
                                        )
                                );
                        m_offsetManager.get().connect();
                        m_logger.info(null, "Offset Coordinator for " + m_topicAndPartition + " is " + offsetManager);
                        if (consumer != null) try {
                            consumer.disconnect();
                        } catch (Exception ignoreIt) {
                        }
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
                m_logger.warn(probeException, "Failed to query all brokers for the offset coordinator for " + m_topicAndPartition);
            }
            KafkaUtils.backoffSleep(attempts+1);
        }
    }

    private OffsetResponse getTopicOffset(PartitionOffsetRequestInfo offsetPartitionInfo) {
        final int partition = m_topicAndPartition.partition();
        final String topic = m_topicAndPartition.topic();

        kafka.javaapi.OffsetRequest earlyRq = new kafka.javaapi.OffsetRequest(
                singletonMap(m_topicAndPartition, offsetPartitionInfo),
                kafka.api.OffsetRequest.CurrentVersion(), KafkaConstants.CLIENT_ID
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
            m_logger.rateLimitedLog(Level.WARN, fault, "unable to fetch earliest offset for " + m_topicAndPartition);
            response = null;
        }
        return response;
    }

    private OffsetFetchResponse getClientTopicOffset() {
        final short version = 1;
        OffsetFetchResponse rsp = null;
        Throwable fault = null;

        for (int attempts = 0; attempts < 3; ++attempts) try {
            final OffsetFetchRequest rq = new OffsetFetchRequest(
                    m_config.getGroupId(),
                    singletonList(m_topicAndPartition),
                    version, nextCorrelationId(),
                    KafkaConstants.CLIENT_ID
                    );
            BlockingChannel channel = m_offsetManager.get();
            channel.send(rq.underlying());
            rsp = OffsetFetchResponse.readFrom(channel.receive().buffer());
            short code = rsp.offsets().get(m_topicAndPartition).error();
            if (code != ErrorMapping.NoError()) {
                fault = ErrorMapping.exceptionFor(code);
                KafkaUtils.backoffSleep(attempts+1);
                if (code == ErrorMapping.NotCoordinatorForConsumerCode()) {
                    getOffsetCoordinator();
                } else if (code == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
                    getOffsetCoordinator();
                } else if (code == ErrorMapping.UnknownTopicOrPartitionCode()) {
                    getOffsetCoordinator();
                    fault = null;
                    continue;
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
            m_logger.rateLimitedLog(Level.WARN, fault, "unable to fetch earliest offset for " + m_topicAndPartition);
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

    protected void resetLeader() {
        KafkaStreamImporterConfig.closeConsumer(m_consumer);
        m_consumer = null;
        HostAndPort leaderBroker = findNewLeader();
        if (leaderBroker == null) {
            //point to original leader which will fail and we fall back again here.
            m_logger.rateLimitedLog(Level.WARN, null, "Fetch Failed to find leader continue with old leader: " + m_config.getPartitionLeader());
            leaderBroker = m_config.getPartitionLeader();
        } else {
            if (!leaderBroker.equals(m_config.getPartitionLeader())) {
                m_logger.info(null, "Fetch Found new leader for " + m_topicAndPartition + " New Leader: " + leaderBroker);
                m_config.setPartitionLeader(leaderBroker);
            }
        }
        m_consumer = new SimpleConsumer(
                leaderBroker.getHost(), leaderBroker.getPort(),
                m_config.getSocketTimeout(), m_config.getFetchSize(), KafkaConstants.CLIENT_ID
                );
    }

    protected void accept() {
        m_logger.info(null, "Starting partition fetcher for " + m_topicAndPartition);
        long submitCount = 0;
        PendingWorkTracker callbackTracker = new PendingWorkTracker();
        Formatter formatter = m_config.getFormatterBuilder().create();

        try {
            //Start with the starting leader.
            resetLeader();

            int sleepCounter = 1;
            while (m_lifecycle.shouldRun()) {
                if (m_currentOffset.get() < 0) {
                    getOffsetCoordinator();
                    if (m_offsetManager.get() == null) {
                        sleepCounter = KafkaUtils.backoffSleep(sleepCounter);
                        continue;
                    }

                    long lastOffset = getLastOffset();
                    if (lastOffset == -1) {
                        sleepCounter = KafkaUtils.backoffSleep(sleepCounter);
                        continue;
                    }

                    m_gapTracker.resetTo(lastOffset);
                    m_lastCommittedOffset = lastOffset;

                    m_currentOffset.set(lastOffset);
                    if (m_currentOffset.get() < 0) {
                        //If we dont know the offset get it backoff if we fail.
                        sleepCounter = KafkaUtils.backoffSleep(sleepCounter);
                        m_logger.info(null, "No valid offset found for " + m_topicAndPartition);
                        continue;
                    }
                    m_logger.info(null, "Starting offset for " + m_topicAndPartition + " is " + m_currentOffset.get());
                }
                long currentFetchCount = 0;
                //Build fetch request of we have a valid offset and not too many are pending.
                FetchRequest req = m_fetchRequestBuilder.addFetch(m_topicAndPartition.topic(),
                                m_topicAndPartition.partition(), m_currentOffset.get(), m_config.getFetchSize())
                                .build();
                FetchResponse fetchResponse = null;
                try {
                    fetchResponse = m_consumer.fetch(req);
                    if (fetchResponse == null) {
                        sleepCounter = KafkaUtils.backoffSleep(sleepCounter);
                        continue;
                    }
                } catch (Exception ex) {
                    m_logger.rateLimitedLog(Level.WARN, ex, "Failed to fetch from " +  m_topicAndPartition);
                    //See if its network error and find new leader for this partition.
                    if (ex instanceof IOException) {
                        resetLeader();
                        //find leader in resetLeader would sleep and backoff
                        continue;
                    }
                    sleepCounter = KafkaUtils.backoffSleep(sleepCounter);
                    continue;
                }

                if (fetchResponse.hasError()) {
                    // Something went wrong!
                    short code = fetchResponse.errorCode(m_topicAndPartition.topic(), m_topicAndPartition.partition());
                    m_logger.warn(ErrorMapping.exceptionFor(code), "Failed to fetch messages for %s", m_topicAndPartition);
                    sleepCounter = KafkaUtils.backoffSleep(sleepCounter);
                    if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                        // We asked for an invalid offset. For simple case ask for the last element to reset
                        m_logger.info(null, "Invalid offset requested for " + m_topicAndPartition);
                        getOffsetCoordinator();
                        m_currentOffset.set(-1L);
                        continue;
                    }
                    resetLeader();
                    continue;
                }
                sleepCounter = 1;
                String topicIdentifer = m_topicAndPartition.topic() + "-" + m_topicAndPartition.partition();
                for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(m_topicAndPartition.topic(), m_topicAndPartition.partition())) {
                    //You may be catchin up so dont sleep.
                    currentFetchCount++;
                    long currentOffset = messageAndOffset.offset();

                    //if currentOffset is less means we have already pushed it and also check pending queue.
                    if (currentOffset < m_currentOffset.get()) {
                        continue;
                    }
                    ByteBuffer payload = messageAndOffset.message().payload();
                    Object params[] = null;
                    try {
                        m_gapTracker.submit(messageAndOffset.nextOffset());
                        params = formatter.transform(payload);

                        ProcedureInvocationCallback cb = new ProcedureInvocationCallback(messageAndOffset.offset(),
                                messageAndOffset.nextOffset(), callbackTracker, m_gapTracker, m_dead, m_pauseOffset, topicIdentifer);

                        if (m_lifecycle.hasTransaction()) {
                            if (invoke(params, cb)) {
                                callbackTracker.produceWork();
                            }
                            else {
                                if (m_logger.isDebugEnabled()) {
                                    m_logger.debug(null, "Failed to process Invocation possibly bad data: " + Arrays.toString(params));
                                }
                                m_gapTracker.commit(messageAndOffset.nextOffset());
                            }
                        }

                    } catch (FormatException e) {
                        m_logger.rateLimitedLog(Level.WARN, e, "Failed to tranform data: %s" , Arrays.toString(params));
                        m_gapTracker.commit(messageAndOffset.nextOffset());
                    }
                    submitCount++;
                    m_currentOffset.set(messageAndOffset.nextOffset());
                    if (!m_lifecycle.shouldRun()) {
                        break;
                    }
                }
                if (!m_lifecycle.shouldRun()) {
                    break;
                }

                //wait to fetch more if we read nothing last time.
                if (currentFetchCount == 0) {
                    try {
                        Thread.sleep(m_waitSleepMs);
                    }
                    catch (InterruptedException ie) {
                    }
                }
                if (KafkaCommitPolicy.shouldCommit(m_config.getCommitPolicy(), m_config.getTriggerValue(), m_lastCommitTime)) {
                    commitOffset(false);
                }
            }
        } catch (Exception ex) {
            m_logger.error(ex, "Failed to start topic partition fetcher for " + m_topicAndPartition);
        } finally {
            final boolean usePausedOffset = m_pauseOffset.get() != -1;
            boolean skipCommit = false;
            if (usePausedOffset) {
                // Paused offset is not guaranteed reliable until all callbacks have been called.
                if (callbackTracker.waitForWorkToFinish() == false) {
                    if (m_pauseOffset.get() < m_lastCommittedOffset) {
                        m_logger.warn(null, "Committing paused offset even though a timeout occurred waiting for pending stored procedures to finish.");
                    } else {
                        m_logger.warn(null, "Refusing to commit paused offset because a timeout occurred waiting for pending stored procedures to finish.");
                        skipCommit = true;
                    }
                }
            }
            if (skipCommit == false) {
                // Force a commit. Paused offset will be re-acquired if needed.
                commitOffset(usePausedOffset);
            }
            KafkaStreamImporterConfig.closeConsumer(m_consumer);
            m_consumer = null;
            BlockingChannel channel = m_offsetManager.getAndSet(null);
            if (channel != null) {
                try { channel.disconnect(); } catch (Exception ignoreIt) {}
            }
        }
        m_dead.compareAndSet(false, true);
        m_logger.info(null, "Partition fetcher stopped for " + m_topicAndPartition
                + " Last commit point is: " + m_lastCommittedOffset
                + " Callback Rcvd: " + callbackTracker.getCallbackCount()
                + " Submitted: " + submitCount);

    }

    public void resetCounters() {
        switch(m_config.getCommitPolicy()) {
            case TIME:
                m_lastCommitTime = EstTime.currentTimeMillis();
        default:
            break;
        }
    }

    public boolean commitOffset(boolean usePausedOffset) {
        final short version = 1;
        long safe = m_gapTracker.getSafe();

        //nothing to commit;
        if (safe < 0) {
            return true;
        }
        final long pausedOffset = usePausedOffset ? m_pauseOffset.get() : -1;

        if (m_lastCommittedOffset != pausedOffset && (safe > m_lastCommittedOffset || pausedOffset != -1)) {
            long now = System.currentTimeMillis();
            OffsetCommitResponse offsetCommitResponse = null;
            try {
                BlockingChannel channel = null;
                int retries = 3;
                if (pausedOffset != -1) {
                    m_logger.rateLimitedLog(Level.INFO, null, m_topicAndPartition + " is using paused offset to commit: " + pausedOffset);
                }
                while (channel == null && --retries >= 0) {
                    if ((channel = m_offsetManager.get()) == null) {
                        getOffsetCoordinator();
                        m_logger.rateLimitedLog(Level.WARN, null, "Commit Offset Failed to get offset coordinator for " + m_topicAndPartition);
                        continue;
                    }
                    if (pausedOffset != -1) {
                        safe = Math.min(pausedOffset, safe);
                    }
                    OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest(
                            m_config.getGroupId(),
                            singletonMap(m_topicAndPartition, new OffsetAndMetadata(safe, "commit", now)),
                            nextCorrelationId(),
                            KafkaConstants.CLIENT_ID,
                            version
                            );
                    channel.send(offsetCommitRequest.underlying());
                    offsetCommitResponse = OffsetCommitResponse.readFrom(channel.receive().buffer());
                    final short code = ((Short)offsetCommitResponse.errors().get(m_topicAndPartition)).shortValue();
                    if (code == ErrorMapping.NotCoordinatorForConsumerCode() || code == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
                        m_logger.info(null, "Not coordinator for committing offset for " + m_topicAndPartition + " Updating coordinator.");
                        getOffsetCoordinator();
                        channel = null;
                        continue;
                    }
                }
                if (retries < 0 || offsetCommitResponse == null) {
                    return false;
                }
            } catch (Exception e) {
                m_logger.rateLimitedLog(Level.WARN, e, "Failed to commit Offset for " + m_topicAndPartition);
                if (e instanceof IOException) {
                    getOffsetCoordinator();
                }
                return false;
            }
            final short code = ((Short) offsetCommitResponse.errors().get(m_topicAndPartition)).shortValue();
            if (code != ErrorMapping.NoError()) {
                final String msg = "Commit Offset Failed to commit for " + m_topicAndPartition;
                m_logger.rateLimitedLog(Level.WARN, ErrorMapping.exceptionFor(code), msg);
                return false;
            }
            m_lastCommittedOffset = safe;
            resetCounters();
            return true;
        }

        return false;
    }

    protected void stop() {
    }
}
