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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltdb.client.Client;
import org.voltdb.importer.CSVInvocation;
import org.voltdb.importer.ImportHandlerProxy;
import org.voltdb.utils.CSVDataLoader;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

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
    private String m_topic;
    private Integer m_maxErrors = 100;
    private final ArrayList<ClientConnectionHandler> m_clients = new ArrayList<ClientConnectionHandler>();

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
            for (ClientConnectionHandler s : m_clients) {
                s.stopClient();
            }
            m_clients.clear();
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
        m_topic = (String )m_properties.getProperty("topic");
        if (m_topic == null || m_topic.trim().length() == 0) {
            throw new RuntimeException("Missing topic.");
        }
        String maxErrors = (String )m_properties.getProperty("maxErrors");
        if (maxErrors != null && maxErrors.trim().length() != 0) {
            m_maxErrors = Integer.parseInt(maxErrors);
        }
    }

    //This is ClientConnection handler to read and dispatch data to stored procedure.
    private class ClientConnectionHandler extends Thread {
        private final Socket m_clientSocket;
        private final String m_procedure;
        private final ImportHandlerProxy m_importHandlerProxy;

        public ClientConnectionHandler(ImportHandlerProxy ic, Socket clientSocket, String procedure) {
            m_importHandlerProxy = ic;
            m_clientSocket = clientSocket;
            m_procedure = procedure;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(m_clientSocket.getInputStream()));
                    while (true) {
                        String line = in.readLine();
                        //You should convert your data to params here.
                        if (line == null) break;
                        CSVInvocation invocation = new CSVInvocation(m_procedure, line);
                        if (!callProcedure(invocation)) {
                            System.out.println("Inserted failed: " + line);
                        }
                    }
                    m_clientSocket.close();
                    System.out.println("Client Closed.");
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }

        public void stopClient() {
            try {
                m_clientSocket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
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
            String procedure = m_properties.getProperty("procedure");
            m_consumer = new KafkaConsumerConnector(m_zookeeper, m_topic);
            m_es = getConsumerExecutor(m_consumer, m_loader);
            close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static class KafkaConsumerConnector {

        final ConsumerConfig m_consumerConfig;
        final ConsumerConnector m_consumer;

        public KafkaConsumerConnector(String zk, String groupName) {
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

            m_consumerConfig = new ConsumerConfig(props);

            m_consumer = kafka.consumer.Consumer.createJavaConsumerConnector(m_consumerConfig);
        }

        public void stop() {
            try {
                //Let offset get pushed to zk....so sleep for auto.commit.interval.ms
                Thread.sleep(1100);
            } catch (InterruptedException ex) { }
            finally {
                m_consumer.commitOffsets();
                m_consumer.shutdown();
            }
        }
    }

    public static class KafkaConsumer implements Runnable {

        private final KafkaStream m_stream;
        private final CSVParser m_csvParser;

        public KafkaConsumer(KafkaStream a_stream) {
            m_stream = a_stream;
            m_csvParser = new CSVParser();
        }

        @Override
        public void run() {
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            while (it.hasNext()) {
                MessageAndMetadata<byte[], byte[]> md = it.next();
                byte msg[] = md.message();
                long offset = md.offset();
                String smsg = new String(msg);
            }
        }

    }

    private ExecutorService getConsumerExecutor(KafkaConsumerConnector consumer,
            CSVDataLoader loader) throws Exception {

        Map<String, Integer> topicCountMap = new HashMap<>();
        //Get this from config or arg. Use 3 threads default.
        ExecutorService executor = Executors.newFixedThreadPool(3);
        topicCountMap.put(m_topic, 3);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.m_consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(m_topic);

        // now launch all the threads for partitions.
        for (final KafkaStream stream : streams) {
            KafkaConsumer bconsumer = new KafkaConsumer(stream);
            executor.submit(bconsumer);
        }

        return executor;
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
