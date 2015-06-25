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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import kafka.javaapi.consumer.SimpleConsumer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltdb.importer.ImportHandlerProxy;

/**
 * Based on SimpleConsumer
 * Implement a BundleActivator interface and extend ImportHandlerProxy.
 * @author akhanzode
 */
public class KafkaStreamImporter extends ImportHandlerProxy implements BundleActivator {

    private Properties m_properties;
    private String m_procedure;
    private List<String> m_topicList;
    private String m_topics;
    private String m_brokers;
    private List<HostAndPort> m_brokerList = new ArrayList<HostAndPort>();

    public static class HostAndPort {
        public final String host;
        public final int port;
        public HostAndPort(String h, int p) {
            host = h;
            port = p;
        }
        public static HostAndPort fromString(String hap) {
            String s[] = hap.split(":");
            int p = Integer.parseInt(s[1]);
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
    public synchronized void stop() {
    }

    /**
     * Return a name for VoltDB to log with friendly name.
     * @return name of the importer.
     */
    @Override
    public String getName() {
        return "KafkaImporter";
    }

    /**
     * This is called with the properties that are supplied in the deployment.xml
     * Do any initialization here.
     * @param p
     */
    @Override
    public void configure(Properties p) {
        m_properties = (Properties) p.clone();
        m_procedure = (String )m_properties.get("procedure");
        if (m_procedure == null || m_procedure.trim().length() == 0) {
            throw new RuntimeException("Missing procedure.");
        }
        //pipe seperated list of topics.
        m_topics = (String )m_properties.getProperty("topics");
        if (m_topics == null || m_topics.trim().length() == 0) {
            throw new RuntimeException("Missing topic(s).");
        }
        m_topicList = Arrays.asList(m_topics.split("\\s*,\\s*"));
        if (m_topicList == null || m_topicList.isEmpty()) {
            throw new RuntimeException("Missing topic(s).");
        }
        m_brokers = (String )m_properties.getProperty("brokers");
        if (m_brokers == null || m_brokers.trim().length() == 0) {
            throw new RuntimeException("Missing kafka broker");
        }
        List<String> brokerList = Arrays.asList(m_brokers.split("\\s*,\\s*"));
        if (brokerList == null || brokerList.isEmpty()) {
            throw new RuntimeException("Missing kafka broker");
        }
        for (String broker : brokerList) {
            HostAndPort hap = HostAndPort.fromString(broker);
            m_brokerList.add(hap);
        }
    }

    /**
     * This is called when server is ready to accept any transactions.
     */
    @Override
    public void readyForData() {
        try {
            info("Configured and ready with properties: " + m_properties);
            SimpleConsumer simpleConsumer = new SimpleConsumer(m_brokerList.get(0).host,
                    m_brokerList.get(0).port, 0, 4096, "voltdb-importer");

            //For all topics connect and get metadata.
            Map<String, List<TopicMetadata>> topicPartitionMetaData = new HashMap<String, List<TopicMetadata>>();
            Map<String, List<Integer>> topicPartitions = new HashMap<String, List<Integer>>();
            for (String topic : m_topicList) {
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = simpleConsumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                topicPartitionMetaData.put(topic, metaData);
                List<Integer> partitions = topicPartitions.get(topic);
                if (partitions == null) {
                    partitions = new ArrayList<Integer>();
                    topicPartitions.put(topic, partitions);
                }
                for (TopicMetadata item : metaData) {

                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        partitions.add(part.partitionId());
                        //We found partitions.
                        System.out.println("Topic partition metadata: " + part);
                        for (kafka.cluster.Broker replica : part.replicas()) {
//                            topicPartitionLeader.add(replica.host());
                        }
                    }
                }
            }

            Map<String, List<Integer>> topicMap = new HashMap<String, List<Integer>>();
            for (String topic : m_topicList) {
                topicMap.put(topic, Collections.singletonList(0));
            }
            FetchRequest req = new FetchRequestBuilder()
                    .clientId("voltdb-importer")
                    .addFetch("foo", 0, 0L, 100)
                    .build();
            FetchResponse fetchResponse = simpleConsumer.fetch(req);
            for ( Map.Entry<String, List<Integer>> entry : topicMap.entrySet() ) {
              String topic = entry.getKey();
              for ( Integer offset : entry.getValue()) {
                ByteBufferMessageSet bbmsg = fetchResponse.messageSet(topic, offset);
                for(MessageAndOffset messageAndOffset: bbmsg) {
                    ByteBuffer payload = messageAndOffset.message().payload();
                    byte[] bytes = new byte[payload.limit()];
                    payload.get(bytes);
                    System.out.println(new String(bytes, "UTF-8"));
                }
              }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
