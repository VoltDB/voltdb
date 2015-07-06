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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import kafka.api.FetchRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.message.MessageAndOffset;

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
    private final Map<String, String> m_topicPartitionLeader = new HashMap<String, String>();
    private final Map<String, Integer> m_topicPartitionLeaderPort = new HashMap<String, Integer>();
    private final Map<String, TopicPartitionFetcher> m_fetchers = new HashMap<String, TopicPartitionFetcher>();

    private ExecutorService m_es = null;

    //Simple Host and Port abstraction....dont want to use our big stuff here orgi bundle import nastiness.
    public static class HostAndPort {

        public final String host;
        public final int port;

        public HostAndPort(String h, int p) {
            host = h;
            port = p;
        }

        public static HostAndPort fromString(String hap) {
            String s[] = hap.split(":");
            int p = KAFKA_DEFAULT_BROKER_PORT;
            if (s.length > 1 && s[1] != null && s[1].length() > 0) {
                p = Integer.parseInt(s[1]);
            }
            return new HostAndPort(s[0], p);
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
    private Set<URI> buildTopicLeaderMetadata(SimpleConsumer simpleConsumer) throws URISyntaxException {

        //For all topics connect and get metadata.
        Set<URI> availableResources = new TreeSet<URI>();
        for (String topic : m_topicList) {
            List<String> topics = Collections.singletonList(topic);
            TopicMetadataRequest req = new TopicMetadataRequest(topics);
            kafka.javaapi.TopicMetadataResponse resp = simpleConsumer.send(req);

            List<TopicMetadata> metaData = resp.topicsMetadata();
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
                        m_topicPartitionLeader.put(leaderKey, replica.host());
                        m_topicPartitionLeaderPort.put(leaderKey, replica.port());
                        URI uri = new URI("kafka:/" + topic + "/partition/" + part.partitionId());
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
            simpleConsumer = new SimpleConsumer(m_brokerList.get(0).host, m_brokerList.get(0).port, m_consumerSocketTimeout, m_fetchSize, CLIENT_ID);
            //Build all available topic URIs
            availableResources = buildTopicLeaderMetadata(simpleConsumer);
        } catch (Exception ex) {
            //Handle
            error("Failed to get available resources for kafka importer" + ex.toString());
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
        if (m_es != null) {
            m_es.submit(new Runnable() {
                @Override
                public void run() {
                    m_done.release();
                }
            });
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
        return "KafkaImporter";
    }

    /**
     * This is called with the properties that are supplied in the deployment.xml Do any initialization here.
     *
     * @param p
     */
    @Override
    public void configure(Properties p) {
        m_properties = (Properties) p.clone();
        m_procedure = (String) m_properties.get("procedure");
        if (m_procedure == null || m_procedure.trim().length() == 0) {
            throw new RuntimeException("Missing procedure.");
        }
        //pipe seperated list of topics.
        String topics = (String) m_properties.getProperty("topics");
        if (topics == null || topics.trim().length() == 0) {
            throw new RuntimeException("Missing topic(s).");
        }
        m_topicList = Arrays.asList(topics.split("\\s*,\\s*"));
        if (m_topicList == null || m_topicList.isEmpty()) {
            throw new RuntimeException("Missing topic(s).");
        }
       String brokers = (String) m_properties.getProperty("brokers");
        if (brokers == null || brokers.trim().length() == 0) {
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
        //These are defaults picked up from kafka we save them so that they are passed around.
        m_fetchSize = Integer.parseInt(m_properties.getProperty("fetch.message.max.bytes", "65536"));
        m_consumerSocketTimeout = Integer.parseInt(m_properties.getProperty("socket.timeout.ms", "30000"));
    }

    //Per topic per partition that we are responsible for.
    private class TopicPartitionFetcher implements Runnable {

        private final URI m_url;
        private final String m_topic;
        private final int m_partition;
        private final String m_leader;
        private final int m_port;
        private boolean m_shutdown = false;
        private final int m_fetchSize;
        private final List<HostAndPort> m_brokers;
        private final int m_consumerSocketTimeout;
        private final AtomicLong m_currentOffset = new AtomicLong(0);
        private final Map<Long, TopicPartitionInvocationCallback> m_pendingCallbacksByOffset = new HashMap<Long, TopicPartitionInvocationCallback>();
        private final int m_perTopicPendingLimit = Integer.getInteger("voltdb.kafka.pertopicPendingLimit", 500);

        public TopicPartitionFetcher(List<HostAndPort> brokers, URI url, String topic, int partition, String leader, int port, int fetchSize, int consumerSocketTimeout) {
            m_url = url;
            m_brokers = brokers;
            m_topic = topic;
            m_partition = partition;
            m_leader = leader;
            m_port = port;
            m_fetchSize = fetchSize;
            m_consumerSocketTimeout = consumerSocketTimeout;
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
                    consumer = new SimpleConsumer(broker.host, broker.port, m_consumerSocketTimeout, m_fetchSize, "findLeader");

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
                    e.printStackTrace();
                } finally {
                    if (consumer != null) {
                        consumer.close();
                    }
                }
            }
            if (returnMetaData == null) {
                error("Failed to find Leader for topic " + m_topic + " and partition " + m_partition);
            }
            return returnMetaData;
        }

        //Find leader for this topic partition.
        private String findNewLeader() {
            for (int i = 0; i < 3; i++) {
                boolean shouldSleep = false;
                PartitionMetadata metadata = findLeader();
                if (metadata == null) {
                    shouldSleep = true;
                } else if (metadata.leader() == null) {
                    shouldSleep = true;
                } else if (m_leader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                    // first time through if the leader hasn't changed give ZooKeeper a second to recover
                    // second time, assume the broker did recover before failover, or it was a non-Broker issue
                    shouldSleep = true;
                } else {
                    return metadata.leader().host();
                }
                if (shouldSleep) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                    }
                }
            }
            //Unable to find return null for recheck.
            return null;
        }

        //Just set shutdown flag fetcher timeout will then exit the thread.
        public void shutdown() {
            m_shutdown = true;
        }

        //Return offset last committed if -1 means error.
        private long getLastOffset(SimpleConsumer consumer) {

            final int correlationId = m_partition;
            final short version = 1;
            TopicAndPartition topicAndPartition = new TopicAndPartition(m_topic, m_partition);

            OffsetFetchRequest offsetFetchRequest
                    = new OffsetFetchRequest(GROUP_ID, Arrays.asList(topicAndPartition), version, correlationId, CLIENT_ID);
            OffsetFetchResponse offsetFetchResponse = consumer.fetchOffsets(offsetFetchRequest);
            OffsetMetadataAndError ofmd = offsetFetchResponse.offsets().get(topicAndPartition);
            short code = ofmd.error();

            // this means that an offset has yet to be committed
            if (code == ErrorMapping.UnknownTopicOrPartitionCode()) {
                PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1);
                Map<TopicAndPartition, PartitionOffsetRequestInfo> reqMap = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
                reqMap.put(topicAndPartition, partitionOffsetRequestInfo);
                OffsetRequest offsetRequest = new OffsetRequest(reqMap, kafka.api.OffsetRequest.CurrentVersion(), null);
                OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);
                if (offsetResponse.hasError()) {
                    return -1;
                }
                return offsetResponse.offsets(m_topic, m_partition)[0];
            } else if (code == ErrorMapping.NoError()) {
                return ofmd.offset() + 1;
            } else {
                return -1;
            }
        }

        //Callback for each invocation we have submitted.
        private class TopicPartitionInvocationCallback implements ProcedureCallback {

            private final long m_offset;
            private final long m_nextOffset;
            private final SimpleConsumer m_consumer;
            private final TopicAndPartition m_topicAndPartition;

            public TopicPartitionInvocationCallback(SimpleConsumer consumer, long offset, long noffset) {
                m_consumer = consumer;
                m_offset = offset;
                m_nextOffset = noffset;
                m_topicAndPartition = new TopicAndPartition(m_topic, m_partition);
            }

            public boolean commitOffset(long offset) {

                final int correlationId = m_partition;
                final short version = 1;

                OffsetAndMetadata offsetMetdata = new OffsetAndMetadata(offset, "commitRequest", ErrorMapping.NoError());
                Map<TopicAndPartition, OffsetAndMetadata> reqMap = new HashMap<TopicAndPartition, OffsetAndMetadata>();
                reqMap.put(m_topicAndPartition, offsetMetdata);
                OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest(GROUP_ID, reqMap, correlationId, CLIENT_ID, version);
                OffsetCommitResponse offsetCommitResponse = null;
                try {
                    offsetCommitResponse = m_consumer.commitOffsets(offsetCommitRequest);
                } catch (Exception e) {
                    e.printStackTrace();
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
            try {
                //Startwith the starting leader.
                String leaderBroker = m_leader;
                int failedCount = 0;
                while (!m_shutdown) {
                    if (consumer == null) {
                        consumer = new SimpleConsumer(leaderBroker, m_port, m_consumerSocketTimeout, m_fetchSize, CLIENT_ID);
                        m_currentOffset.set(getLastOffset(consumer));
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
                            if (++failedCount > 5) {
                                error("Failed to fetch messages for topic " + m_topic + " and partition " + m_partition);
                                return;
                            }
                            if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                                // We asked for an invalid offset. For simple case ask for the last element to reset
                                m_currentOffset.set(getLastOffset(consumer));
                                continue;
                            }
                            error("Failed to fetch messages for topic " + m_topic + " and partition " + m_partition);
                            consumer.close();
                            consumer = null;
                            leaderBroker = findNewLeader();
                            if (leaderBroker == null) {
                                //point to original leader which will fail and we fall back again here.
                                error("Failed to find leader for " + m_topic + " and partition " + m_partition);
                                leaderBroker = m_leader;
                            }
                            info("Found new leader for topic " + m_topic + " and partition " + m_partition + " New Leader: " + leaderBroker);
                            continue;
                        }
                        failedCount = 0;
                        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(m_topic, m_partition)) {
                            long currentOffset = messageAndOffset.offset();
                            if (currentOffset < m_currentOffset.get() || m_pendingCallbacksByOffset.containsKey(currentOffset)) {
                                info("Found an old offset: " + currentOffset + " Expecting: " + m_currentOffset.get());
                                continue;
                            }
                            ByteBuffer payload = messageAndOffset.message().payload();

                            byte[] bytes = new byte[payload.limit()];
                            payload.get(bytes);
                            String line = new String(bytes);
                            CSVInvocation invocation = new CSVInvocation(m_procedure, line);
                            TopicPartitionInvocationCallback cb = new TopicPartitionInvocationCallback(consumer, currentOffset, messageAndOffset.nextOffset());
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
                ex.printStackTrace();
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
        try {
            for (URI nuri : added) {
                Map<String, List<Integer>> topicMap = new HashMap<String, List<Integer>>();
                for (String topic : m_topicList) {
                    topicMap.put(topic, Collections.singletonList(0));
                }
                for (String topic : m_topicList) {
                    List<Integer> topicPartitions = m_topicPartitions.get(topic);
                    for (int partition : topicPartitions) {
                        String leaderKey = topic + "-" + partition;
                        URI assignedKey = new URI("kafka:/" + topic + "/partition/" + partition);
                        //The fetcher must not have existed.
                        if (!m_fetchers.containsKey(nuri) && nuri.equals(assignedKey)) {
                            info("Channel " + assignedKey + " mastership is assigned to this node.");
                            TopicPartitionFetcher fetcher = new TopicPartitionFetcher(m_brokerList, assignedKey, topic, partition,
                                    m_topicPartitionLeader.get(leaderKey),
                                    m_topicPartitionLeaderPort.get(leaderKey), m_fetchSize, m_consumerSocketTimeout);
                            m_fetchers.put(assignedKey.toString(), fetcher);
                            m_es.submit(fetcher);
                            info("KafkaImporter is fetching for resource: " + nuri);
                        }
                    }
                }
            }
        } catch (URISyntaxException ex) {
            //This should never happen.
            ex.printStackTrace();
        }
        //For removed shutdown the fetchers if all are removed the importer will be closed/shutdown?
        for (URI r : removed) {
            TopicPartitionFetcher fetcher = m_fetchers.get(r.toString());
            if (fetcher != null) {
                fetcher.shutdown();
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
            ex.printStackTrace();
        }
    }

}
