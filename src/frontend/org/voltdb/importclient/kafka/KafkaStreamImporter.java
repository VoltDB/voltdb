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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import kafka.api.FetchRequestBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import kafka.javaapi.consumer.SimpleConsumer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import kafka.api.ConsumerMetadataRequest;
import kafka.api.FetchRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.javaapi.ConsumerMetadataResponse;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.message.MessageAndOffset;
import kafka.network.BlockingChannel;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importer.CSVInvocation;
import org.voltdb.importer.ImportHandlerProxy;

/**
 * Based on SimpleConsumer Implement a BundleActivator interface and extend ImportHandlerProxy.
 * For each partition for a topic a thread is launched to pull data and submit transactions.
 *
 * @author akhanzode
 */
public class KafkaStreamImporter extends ImportHandlerProxy implements BundleActivator {

    //Properties for the importer
    private Properties m_properties;
    //Group id
    private String m_groupId;
    //Procedure to be invoked with params.
    private String m_procedure;
    //List of topics form comma seperated list.
    private List<String> m_topicList;
    //List of brokers.
    private final List<HostAndPort> m_brokerList = new ArrayList<HostAndPort>();
    //kafka properties which has defaults
    private int m_fetchSize = 65536;
    private int m_consumerSocketTimeout = 30000; //In milliseconds

    private static final String GROUP_ID = "voltdb";
    private static final String CLIENT_ID = "voltdb-importer";
    private static final int KAFKA_DEFAULT_BROKER_PORT = 9092;
    //readyForData is waiting for this released by shutdown
    private final Semaphore m_done = new Semaphore(0);
    private boolean m_stopping = false;

    //topic partition metadata
    private final Map<String, List<TopicMetadata>> m_topicPartitionMetaData = new HashMap<String, List<TopicMetadata>>();
    //Topic partitions
    private final Map<String, List<Integer>> m_topicPartitions = new HashMap<String, List<Integer>>();
    //topic partition leader
    private final Map<String, HostAndPort> m_topicPartitionLeader = new HashMap<String, HostAndPort>();
    private final Map<String, TopicPartitionFetcher> m_fetchers = new HashMap<String, TopicPartitionFetcher>();

    private ExecutorService m_es = null;

    //Simple Host and Port abstraction....dont want to use our big stuff here orgi bundle import nastiness.
    public static class HostAndPort {

        private String m_host;
        private int m_port;

        public HostAndPort(String h, int p) {
            m_host = h;
            m_port = p;
        }

        public static HostAndPort fromString(String hap) {
            String s[] = hap.split(":");
            int p = KAFKA_DEFAULT_BROKER_PORT;
            if (s.length > 1 && s[1] != null && s[1].length() > 0) {
                p = Integer.parseInt(s[1].trim());
            }
            return new HostAndPort(s[0].trim(), p);
        }

        public String getHost() {
            return m_host;
        }

        public void setHost(String host) {
            m_host = host;
        }

        public int getPort() {
            return m_port;
        }

        public void setPort(int port) {
            m_port = port;
        }

        @Override
        public String toString() {
            return m_host + ":" + m_port;
        }
    }

    // Register ImportHandlerProxy service.
    @Override
    public void start(BundleContext context) throws Exception {
        context.registerService(this.getClass().getName(), this, null);
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        //Do any bundle related cleanup.
    }

    @Override
    public boolean isRunEveryWhere() {
        //This is not a run everywhere importer only allocated resources are polled and consumed.
        return false;
    }

    //This is called to get all available resources.
    private Set<URI> buildTopicLeaderMetadata(SimpleConsumer simpleConsumer) {

        //For all topics connect and get metadata.
        Set<URI> availableResources = new TreeSet<URI>();
        for (String topic : m_topicList) {
            List<String> topics = Collections.singletonList(topic);
            TopicMetadataRequest req = new TopicMetadataRequest(topics);
            kafka.javaapi.TopicMetadataResponse resp = simpleConsumer.send(req);

            List<TopicMetadata> metaData = resp.topicsMetadata();
            if (metaData == null) {
                return availableResources;
            }
            m_topicPartitionMetaData.put(topic, metaData);
            List<Integer> partitions = m_topicPartitions.get(topic);
            if (partitions == null) {
                partitions = new ArrayList<Integer>();
                m_topicPartitions.put(topic, partitions);
            }
            for (TopicMetadata item : metaData) {
                for (PartitionMetadata part : item.partitionsMetadata()) {
                    partitions.add(part.partitionId());
                    for (kafka.cluster.Broker replica : part.replicas()) {
                        String leaderKey = topic + "-" + part.partitionId();
                        m_topicPartitionLeader.put(leaderKey, new HostAndPort(replica.host(), replica.port()));
                        URI uri = URI.create("kafka:/" + topic + "/partition/" + part.partitionId());
                        availableResources.add(uri);
                    }
                }
            }
        }

        info("Available Channels are: " + availableResources);
        //Create an executor serice with Queue.
        m_es = Executors.newFixedThreadPool(availableResources.size() + 1);
        return availableResources;
    }

    @Override
    public Set<URI> getAllResponsibleResources() {
        SimpleConsumer simpleConsumer = null;
        Set<URI> availableResources = new TreeSet<URI>();
        try {
            simpleConsumer = new SimpleConsumer(m_brokerList.get(0).getHost(), m_brokerList.get(0).getPort(), m_consumerSocketTimeout, m_fetchSize, CLIENT_ID);
            //Build all available topic URIs
            availableResources = buildTopicLeaderMetadata(simpleConsumer);
        } catch (Exception ex) {
            //Handle
            error("Failed to get available resources for kafka importer", ex);
        } finally {
            if (simpleConsumer != null) {
                simpleConsumer.close();
            }
        }
        return availableResources;
    }

    @Override
    public void stop() {
        m_stopping = true;
        //Stop all the fetchers.
        for (TopicPartitionFetcher fetcher : m_fetchers.values()) {
            fetcher.shutdown();
        }
        m_done.release();
        if (m_es != null) {
            //Now wait for fetchers to break out.
            m_es.shutdown();
            try {
                m_es.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        m_fetchers.clear();
    }

    /**
     * Return a name for VoltDB to log with friendly name.
     *
     * @return name of the importer.
     */
    @Override
    public String getName() {
        return "KafkaImporter82";
    }

    /**
     * This is called with the properties that are supplied in the deployment.xml Do any initialization here.
     *
     * @param p
     */
    @Override
    public void configure(Properties p) {
        m_properties = (Properties) p.clone();
        m_procedure = m_properties.getProperty("procedure", "").trim();
        if (m_procedure.isEmpty()) {
            throw new RuntimeException("Missing procedure.");
        }
        //pipe seperated list of topics.
        String topics = m_properties.getProperty("topics", "").trim();
        if (topics.isEmpty()) {
            throw new RuntimeException("Missing topic(s).");
        }
        m_topicList = Arrays.asList(topics.split("\\s*,\\s*"));
        if (m_topicList == null || m_topicList.isEmpty()) {
            throw new RuntimeException("Missing topic(s).");
        }
       String brokers = m_properties.getProperty("brokers", "").trim();
        if (brokers.isEmpty()) {
            throw new RuntimeException("Missing kafka broker");
        }
        List<String> brokerList = Arrays.asList(brokers.split("\\s*,\\s*"));
        if (brokerList == null || brokerList.isEmpty()) {
            throw new RuntimeException("Missing kafka broker");
        }
        for (String broker : brokerList) {
            HostAndPort hap = HostAndPort.fromString(broker);
            m_brokerList.add(hap);
        }
        if (m_brokerList.isEmpty()) {
            throw new RuntimeException("Missing or misconfigured kafka broker list. See brokers property");
        }
        m_groupId = m_properties.getProperty("groupid", GROUP_ID).trim();
        //These are defaults picked up from kafka we save them so that they are passed around.
        m_fetchSize = Integer.parseInt(m_properties.getProperty("fetch.message.max.bytes", "65536"));
        m_consumerSocketTimeout = Integer.parseInt(m_properties.getProperty("socket.timeout.ms", "30000"));
    }

    //Per topic per partition that we are responsible for.
    private class TopicPartitionFetcher implements Runnable {

        private final URI m_url;
        private final String m_topic;
        private final int m_partition;
        private final HostAndPort m_leader;
        private HostAndPort m_coordinator;
        private boolean m_shutdown = false;
        private final int m_fetchSize;
        private final List<HostAndPort> m_brokers;
        private final int m_consumerSocketTimeout;
        //Start with invalid so consumer will fetch it.
        private final AtomicLong m_currentOffset = new AtomicLong(-1);
        private final Map<Long, TopicPartitionInvocationCallback> m_pendingCallbacksByOffset = new LinkedHashMap<Long, TopicPartitionInvocationCallback>();
        private final int m_perTopicPendingLimit = Integer.getInteger("voltdb.kafka.pertopicPendingLimit", 500);
        private final AtomicReference<SimpleConsumer> m_offsetManager = new AtomicReference<SimpleConsumer>();
        private final TopicAndPartition m_topicAndPartition;

        public TopicPartitionFetcher(List<HostAndPort> brokers, URI uri, String topic, int partition, HostAndPort leader, int fetchSize, int consumerSocketTimeout) {
            m_url = uri;
            m_brokers = brokers;
            m_topic = topic;
            m_partition = partition;
            m_leader = leader;
            m_coordinator = leader;
            m_fetchSize = fetchSize;
            m_consumerSocketTimeout = consumerSocketTimeout;
            m_topicAndPartition = new TopicAndPartition(m_topic, m_partition);
        }

        public final URI getUrl() {
            return m_url;
        }

        //Find leader for the topic+partition.
        private PartitionMetadata findLeader() {
            PartitionMetadata returnMetaData = null;
            loop:
            for (HostAndPort broker : m_brokers) {
                SimpleConsumer consumer = null;
                try {
                    consumer = new SimpleConsumer(broker.getHost(), broker.getPort(), m_consumerSocketTimeout, m_fetchSize, "findLeader");

                    List<String> topics = Collections.singletonList(m_topic);
                    TopicMetadataRequest req = new TopicMetadataRequest(topics);
                    kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                    List<TopicMetadata> metaData = resp.topicsMetadata();
                    for (TopicMetadata item : metaData) {
                        for (PartitionMetadata part : item.partitionsMetadata()) {
                            if (part.partitionId() == m_partition) {
                                returnMetaData = part;
                                break loop;
                            }
                        }
                    }
                } catch (Exception e) {
                    error("Error in finding leader for " + m_topicAndPartition, e);
                } finally {
                    if (consumer != null) {
                        consumer.close();
                    }
                }
            }
            if (returnMetaData == null) {
                error("Failed to find Leader for " + m_topicAndPartition);
            }
            return returnMetaData;
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
                } else if (m_leader.getHost().equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                    // first time through if the leader hasn't changed give ZooKeeper a second to recover
                    // second time, assume the broker did recover before failover, or it was a non-Broker issue
                    shouldSleep = true;
                } else {
                    return new HostAndPort(metadata.leader().host(), metadata.leader().port());
                }
                if (shouldSleep) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                    }
                }
            }
            //Unable to find return null for recheck.
            info("Failed to find new leader for " + m_topicAndPartition);
            return null;
        }

        //Just set shutdown flag fetcher timeout will then exit the thread.
        public void shutdown() {
            m_shutdown = true;
        }

        public void getOffsetCoordinator() {
            BlockingChannel channel = null;
            try {
                //This can go to any broker
                channel = new BlockingChannel(m_leader.getHost(), m_leader.getPort(),
                        BlockingChannel.UseDefaultBufferSize(),
                        BlockingChannel.UseDefaultBufferSize(),
                        5000 /* read timeout in millis */);
                channel.connect();
                int correlationId = 0;
                channel.send(new ConsumerMetadataRequest(m_groupId, ConsumerMetadataRequest.CurrentVersion(), correlationId++, CLIENT_ID));
                ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());

                if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
                    Broker offsetManager = metadataResponse.coordinator();
                    m_coordinator.setHost(offsetManager.host());
                    m_coordinator.setPort(offsetManager.port());
                    SimpleConsumer consumer = m_offsetManager.getAndSet(new SimpleConsumer(m_coordinator.getHost(), m_coordinator.getPort(), m_consumerSocketTimeout, m_fetchSize, CLIENT_ID) );
                    if (consumer != null) {
                        consumer.close();
                    }
                }
            } catch (Exception e) {
                // retry the query (after backoff)??
                error("Failed to get Offset Coordinator for " + m_topicAndPartition, e);
            } finally {
                if (channel != null) {
                    channel.disconnect();
                }
            }
            info("Coordinator for consumer is: " + m_coordinator);
        }

        public long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
            if (consumer == null) {
                return -1;
            }
            try {
                Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
                requestInfo.put(m_topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
                kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),clientName);
                OffsetResponse response = consumer.getOffsetsBefore(request);

                if (response.hasError()) {
                    if (response.errorCode(topic, partition) == ErrorMapping.NotLeaderForPartitionCode()) {
                        HostAndPort leaderBroker = findNewLeader();
                        if (leaderBroker != null) {
                            info("Found new leader for " + m_topicAndPartition + " Coordinator will be updated.");
                            SimpleConsumer oconsumer = m_offsetManager.getAndSet(new SimpleConsumer(leaderBroker.getHost(), leaderBroker.getPort(), m_consumerSocketTimeout, m_fetchSize, CLIENT_ID) );
                            if (oconsumer != null) {
                                oconsumer.close();
                            }
                            m_coordinator = leaderBroker;
                        }
                    }
                    info("Error fetching Offset Data from Broker " + m_topicAndPartition.toString() + " Reason: " + response.errorCode(topic, partition) );
                    return -1;
                }
                long[] offsets = response.offsets(topic, partition);
                return offsets[0];
            } catch (Exception ex) {
                error("Failed to get last Offset for " + m_topicAndPartition, ex);
            }
            return -1;
        }

        //Callback for each invocation we have submitted.
        private class TopicPartitionInvocationCallback implements ProcedureCallback {

            private final long m_offset;
            private final long m_nextOffset;
            private final TopicAndPartition m_topicAndPartition;

            public TopicPartitionInvocationCallback(long offset, long noffset, TopicAndPartition tAndP) {
                m_offset = offset;
                m_nextOffset = noffset;
                m_topicAndPartition = tAndP;
            }

            public boolean commitOffset(long offset) {

                final int correlationId = m_partition;
                final short version = 1;

                OffsetAndMetadata offsetMetdata = new OffsetAndMetadata(offset, "commitRequest", ErrorMapping.NoError());
                Map<TopicAndPartition, OffsetAndMetadata> reqMap = new HashMap<TopicAndPartition, OffsetAndMetadata>();
                reqMap.put(m_topicAndPartition, offsetMetdata);
                OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest(m_groupId, reqMap, correlationId, CLIENT_ID, version);
                OffsetCommitResponse offsetCommitResponse = null;
                try {
                    SimpleConsumer consumer = m_offsetManager.get();
                    if (consumer == null) {
                        getOffsetCoordinator();
                        consumer = m_offsetManager.get();
                    }
                    if (consumer != null) {
                        offsetCommitResponse = consumer.commitOffsets(offsetCommitRequest);
                    } else {
                        error("Failed to get offset coordinator for " + m_topicAndPartition);
                        return false;
                    }
                } catch (Exception e) {
                    error("Failed to commit Offset for " + m_topicAndPartition, e);
                    return false;
                }
                final short code = ((Short) offsetCommitResponse.errors().get(m_topicAndPartition));

                return code == ErrorMapping.NoError();
            }

            @Override
            public void clientCallback(ClientResponse response) throws Exception {
                m_pendingCallbacksByOffset.remove(m_offset);
                //This message offset is already committed we just remove from pending and move on.
                if (m_nextOffset < m_currentOffset.get()) {
                    return;
                }
                if (commitOffset(m_nextOffset)) {
                    //Update the offset to read to next offset.
                    m_currentOffset.set(m_nextOffset);
                }
            }

        }

        @Override
        public void run() {
            SimpleConsumer consumer = null;
            info("Starting partition fetcher for topic " + m_topic + " And partition " + m_partition);
            try {
                //Startwith the starting leader.
                HostAndPort leaderBroker = m_leader;
                while (!m_shutdown) {
                    if (consumer == null) {
                        consumer = new SimpleConsumer(leaderBroker.getHost(), leaderBroker.getPort(), m_consumerSocketTimeout, m_fetchSize, CLIENT_ID);
                        getOffsetCoordinator();
                        m_currentOffset.set(getLastOffset(m_offsetManager.get(), m_topic, m_partition, kafka.api.OffsetRequest.LatestTime(), CLIENT_ID));
                    }
                    long currentFetchCount = 0;
                    //Build fetch request of we have a valid offset and not too many are pending.
                    if (m_currentOffset.get() >= 0 && m_pendingCallbacksByOffset.size() < m_perTopicPendingLimit) {
                        FetchRequest req = new FetchRequestBuilder().clientId(CLIENT_ID)
                                .addFetch(m_topic, m_partition, m_currentOffset.get(), m_fetchSize)
                                .build();
                        FetchResponse fetchResponse = consumer.fetch(req);

                        if (fetchResponse.hasError()) {
                            // Something went wrong!
                            short code = fetchResponse.errorCode(m_topic, m_partition);
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException ie) {
                            }
                            error("Failed to fetch messages for " + m_topicAndPartition + " Code " + code);
                            if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                                // We asked for an invalid offset. For simple case ask for the last element to reset
                                error("Invalid offset requested for " + m_topicAndPartition);
                                getOffsetCoordinator();
                                m_currentOffset.set(getLastOffset(m_offsetManager.get(), m_topic, m_partition, kafka.api.OffsetRequest.LatestTime(), CLIENT_ID));
                                continue;
                            }
                            consumer.close();
                            consumer = null;
                            leaderBroker = findNewLeader();
                            if (leaderBroker == null) {
                                //point to original leader which will fail and we fall back again here.
                                leaderBroker = m_leader;
                            }
                            info("Found new leader for " + m_topicAndPartition + " New Leader: " + leaderBroker);
                            continue;
                        }
                        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(m_topic, m_partition)) {
                            long currentOffset = messageAndOffset.offset();
                            if (currentOffset < m_currentOffset.get() || m_pendingCallbacksByOffset.containsKey(currentOffset)) {
                                //info("Found an old offset: " + currentOffset + " Expecting: " + m_currentOffset.get());
                                continue;
                            }
                            ByteBuffer payload = messageAndOffset.message().payload();

                            byte[] bytes = new byte[payload.limit()];
                            payload.get(bytes);
                            String line = new String(bytes);
                            CSVInvocation invocation = new CSVInvocation(m_procedure, line);
                            TopicPartitionInvocationCallback cb = new TopicPartitionInvocationCallback(currentOffset, messageAndOffset.nextOffset(), m_topicAndPartition);
                            m_pendingCallbacksByOffset.put(currentOffset, cb);
                            if (!callProcedure(cb, invocation)) {
                                m_currentOffset.set(messageAndOffset.nextOffset());
                                m_pendingCallbacksByOffset.remove(currentOffset);
                            }
                            currentFetchCount++;
                        }
                    }

                    if (currentFetchCount == 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                        }
                    }
                }
                info("Partition fecher stopped for topic " + this.m_topic + " And partition " + m_partition);
            } catch (Exception ex) {
                error("Failed to start topic partition fetcher for " + m_topicAndPartition, ex);
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }

        }

    }

    //On getting this event kick off ready
    @Override
    public void onChange(Set<URI> added, Set<URI> removed, Set<URI> assigned, int version) {
        if (m_stopping) {
            info("Importer is stopping ignoring the change notification.");
        }
        if (m_es == null) {
            //Create executor with sufficient threads.
            throw new RuntimeException("Failed to get configured executor service.");
        }

        //For addeed create fetchers...make sure existing fetchers are not there.
        for (URI nuri : added) {
            Map<String, List<Integer>> topicMap = new HashMap<String, List<Integer>>();
            for (String topic : m_topicList) {
                topicMap.put(topic, Collections.singletonList(0));
            }
            for (String topic : m_topicList) {
                List<Integer> topicPartitions = m_topicPartitions.get(topic);
                for (int partition : topicPartitions) {
                    String leaderKey = topic + "-" + partition;
                    URI assignedKey = URI.create("kafka:/" + topic + "/partition/" + partition);
                    //The fetcher must not have existed.
                    if (!m_fetchers.containsKey(nuri) && nuri.equals(assignedKey)) {
                        info("Channel " + assignedKey + " mastership is assigned to this node.");
                        HostAndPort hap = m_topicPartitionLeader.get(leaderKey);
                        TopicPartitionFetcher fetcher = new TopicPartitionFetcher(m_brokerList, assignedKey, topic, partition,
                                hap, m_fetchSize, m_consumerSocketTimeout);
                        m_fetchers.put(assignedKey.toString(), fetcher);
                        m_es.submit(fetcher);
                        info("KafkaImporter is fetching for resource: " + nuri);
                    }
                }
            }
        }

        //For removed shutdown the fetchers if all are removed the importer will be closed/shutdown?
        for (URI r : removed) {
            TopicPartitionFetcher fetcher = m_fetchers.get(r.toString());
            if (fetcher != null) {
                fetcher.shutdown();
                info("KafkaImporter is NOT fetching for resource: " + r);
                m_fetchers.remove(r.toString());
            }
        }
    }


    /**
     * This is called when server is ready to accept any transactions.
     */
    @Override
    public void readyForData() {
        try {
            info("Configured and ready with properties: " + m_properties);
            //We wait for shutdown task to release.
            m_done.acquire();
        } catch (Exception ex) {
            error("Kafka Importer finished with exeception ", ex);
        }
    }

}
