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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltdb.client.Client;
import org.voltdb.importer.ImportHandlerProxy;
import org.voltdb.utils.CSVDataLoader;

/**
 * Implement a BundleActivator interface and extend ImportHandlerProxy.
 * @author akhanzode
 */
public class KafkaStreamImporter extends ImportHandlerProxy implements BundleActivator {

    private KafkaConsumerConnector m_consumer = null;
    private final static AtomicLong m_failedCount = new AtomicLong(0);
    private ExecutorService m_es = null;
    private CSVDataLoader m_loader = null;
    private Client m_client = null;

    private Properties m_properties;
    private ServerSocket m_serverSocket;
    private String m_procedure;
    private String m_zookeeper;
    private String m_bootstrap;
    private String m_topic;
    private Integer m_maxErrors = 100;

    // Register ImportHandlerProxy service.
    @Override
    public void start(BundleContext context) throws Exception {
        context.registerService(ImportHandlerProxy.class.getName(), this, null);
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        //Do any bundle related cleanup.
    }

    @Override
    public void stop() {
        try {
            m_serverSocket.close();
            m_serverSocket = null;
        } catch (IOException ex) {
            ex.printStackTrace();
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
        m_zookeeper = (String )m_properties.getProperty("zookeeper");
        if (m_zookeeper == null || m_zookeeper.trim().length() == 0) {
            throw new RuntimeException("Missing zookeeper.");
        }
        m_bootstrap = (String )m_properties.getProperty("bootstrap");
        if (m_bootstrap == null || m_bootstrap.trim().length() == 0) {
            throw new RuntimeException("Missing bootstrap.");
        }
        m_topic = (String )m_properties.getProperty("topic");
        if (m_topic == null || m_topic.trim().length() == 0) {
            throw new RuntimeException("Missing topic.");
        }
        String maxErrors = (String )m_properties.getProperty("maxErrors");
        if (maxErrors != null && maxErrors.trim().length() != 0) {
            m_maxErrors = Integer.parseInt(maxErrors);
        }
    }

    /**
     * This is called when server is ready to accept any transactions.
     */
    @Override
    public void readyForData() {
        try {
            info("Configured and ready with properties: " + m_properties);
            String procedure = m_properties.getProperty("procedure");
            m_consumer = new KafkaConsumerConnector(m_zookeeper, m_topic, m_bootstrap);
            close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static class KafkaConsumerConnector {

        final KafkaConsumer m_consumer;

        public KafkaConsumerConnector(String zk, String groupName, String bootstrap) {
            //Get group id which should be unique for table so as to keep offsets clean for multiple runs.
            String groupId = "voltdb-" + groupName;
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
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("partition.assignment.strategy", "roundrobin");
            props.put("bootstrap.servers", bootstrap);
            m_consumer = new KafkaConsumer(props);
        }

        public void stop() {
            try {
                //Let offset get pushed to zk....so sleep for auto.commit.interval.ms
                Thread.sleep(1100);
            } catch (InterruptedException ex) { }
            finally {
                m_consumer.commit(true);
                m_consumer.close();
            }
        }
    }

  //Close the consumer after this app will exit.
    public void closeConsumer() throws InterruptedException {
        if (m_consumer != null) {
            m_consumer.stop();
            m_consumer = null;
        }
        if (m_es != null) {
            m_es.shutdownNow();
            m_es.awaitTermination(365, TimeUnit.DAYS);
            m_es = null;
        }
    }
    /**
     * Close all connections and cleanup on both the sides.
     */
    public void close() {
        try {
            closeConsumer();
            m_loader.close();
            if (m_client != null) {
                m_client.close();
                m_client = null;
            }
        } catch (Exception ex) {
        }
    }

}
