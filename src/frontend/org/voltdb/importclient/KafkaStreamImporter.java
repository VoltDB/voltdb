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

package org.voltdb.importclient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltdb.importer.CSVInvocation;
import org.voltdb.importer.ImportHandlerProxy;

/**
 * Implement a BundleActivator interface and extend ImportHandlerProxy.
 * @author akhanzode
 */
public class KafkaStreamImporter extends ImportHandlerProxy implements BundleActivator {

    private Properties m_properties;
    private String m_procedure;
    private String[] m_topic;
    private String m_zookeeper;

    private KafkaStreamConsumerConnector m_connector;
    private ExecutorService m_es;

    // Register ImportHandlerProxy service.
    @Override
    public void start(BundleContext context) throws Exception {
        context.registerService(KafkaStreamImporter.class.getName(), this, null);
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        //Do any bundle related cleanup.
    }

    @Override
    public synchronized void stop() {
        synchronized (this) {
            try {
                if (m_connector != null) {
                    info("Stopping Kafka connector.");
                    m_connector.stop();
                    info("Stopped Kafka connector.");
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                m_connector = null;
            }
            try {
                if (m_es != null) {
                    info("Stopping Kafka consumer executor.");
                    m_es.shutdown();
                    m_es.awaitTermination(1, TimeUnit.DAYS);
                    info("Stopped Kafka consumer executor.");
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                m_es = null;
            }
        }
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
        String topics = (String )m_properties.getProperty("topic");
        if (topics == null || topics.trim().length() == 0) {
            throw new RuntimeException("Missing topic(s).");
        }
        m_topic = topics.split(",");
        if (m_topic == null || m_topic.length == 0) {
            throw new RuntimeException("Missing topic(s).");
        }
        m_zookeeper = (String )m_properties.getProperty("zookeeper");
        if (m_zookeeper == null || m_zookeeper.trim().length() == 0) {
            throw new RuntimeException("Missing kafka zookeeper");
        }
    }

    private class KafkaStreamConsumerConnector {

        private ConsumerConnector m_consumer;

        public KafkaStreamConsumerConnector(String zk, String groupName) {
            //Get group id which should be unique for table so as to keep offsets clean for multiple runs.
            String groupId = "voltdbimporter-" + groupName;
            //TODO: Should get this from properties file or something as override?
            Properties props = new Properties();
            props.put("zookeeper.connect", zk);
            props.put("group.id", groupId);
            props.put("zookeeper.session.timeout.ms", "400");
            props.put("zookeeper.sync.time.ms", "200");
            props.put("auto.commit.interval.ms", "1000");
            props.put("auto.commit.enable", "true");
            props.put("auto.offset.reset", "smallest");
            props.put("rebalance.backoff.ms", "10000");

            m_consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        }

        public void stop() {
            try {
                m_consumer.commitOffsets();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                m_consumer.shutdown();
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                m_consumer = null;
            }
        }
    }

    private class KafkaConsumer implements Runnable {

        private final KafkaStream m_stream;
        private final String m_procedure;

        public KafkaConsumer(KafkaStream a_stream, String proc) {
            m_stream = a_stream;
            m_procedure = proc;
        }

        @Override
        public void run() {
            try {
                ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
                while (it.hasNext()) {
                    MessageAndMetadata<byte[], byte[]> md = it.next();
                    byte msg[] = md.message();
                    String line = new String(msg);
                    CSVInvocation invocation = new CSVInvocation(m_procedure, line);
                    callProcedure(invocation);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

    }

    private ExecutorService getConsumerExecutor(KafkaStreamConsumerConnector consumer) throws Exception {

        Map<String, Integer> topicCountMap = new HashMap<>();
        //Get this from config or arg. Use 3 threads default.
        ExecutorService executor = Executors.newFixedThreadPool(3 * m_topic.length);
        for (int i = 0; i < m_topic.length; i++) {
            topicCountMap.put(m_topic[i], 1);
        }
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.m_consumer.createMessageStreams(topicCountMap);

        for (int i = 0; i < m_topic.length; i++) {
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(m_topic[i]);

            // now launch all the threads for partitions.
            for (final KafkaStream stream : streams) {
                KafkaConsumer bconsumer = new KafkaConsumer(stream, m_procedure);
                executor.submit(bconsumer);
            }
        }

        return executor;
    }

    /**
     * This is called when server is ready to accept any transactions.
     */
    @Override
    public void readyForData() {
        try {
            info("Configured and ready with properties: " + m_properties);
            synchronized (this) {
                //TODO: Make group id specific to node.
                m_connector = new KafkaStreamConsumerConnector(m_zookeeper, "voltdb-importer");
                m_es = getConsumerExecutor(m_connector);
            }
            //Now we dont need lock as stop can come along.
            m_es.awaitTermination(365, TimeUnit.DAYS);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
