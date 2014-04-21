/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
package org.voltdb.utils;

import au.com.bytecode.opencsv_voltpatches.CSVParser;
import java.io.IOException;
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

import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

/**
 * KafkaConsumer loads data from kafka into voltdb
 *
 */
public class KafkaLoader {

    /**
     * log topic name
     */
    private static final VoltLogger m_log = new VoltLogger("KAFKALOADER");
    private static KafkaConfig config = null;

    /**
     * Configuration options.
     */
    public static class KafkaConfig extends CLIConfig {

        @Option(shortOpt = "t", desc = "Kafka Topic to subscribe to")
        String topic = "";

        @Option(shortOpt = "m", desc = "maximum errors allowed")
        int maxerrors = -1;

        @Option(desc = "different ways to handle blank items: {error|null|empty} (default: error)")
        String blank = "error";

        @Option(shortOpt = "s", desc = "list of servers to connect to (default: localhost:21212)")
        String servers = "localhost:21212";

        @Option(desc = "username when connecting to the servers")
        String user = "";

        @Option(desc = "password to use when connecting to servers")
        String password = "";

        @Option(shortOpt = "b", desc = "list of brokers to connect to (default: localhost:9092)")
        String brokers = "localhost:9092";

        @Option(shortOpt = "z", desc = "kafka zookeeper to connect to.")
        String zookeeper = ""; //No default here as default will clash with local voltdb cluster

        /**
         * Batch size for processing batched operations.
         */
        @Option(desc = "Batch Size for processing.")
        public int batch = 200;

        /**
         * Table name to insert CSV data into.
         */
        @AdditionalArgs(desc = "insert the data into database by TABLENAME.insert procedure by default")
        public String table = "";

        /**
         * Validate command line options.
         */
        @Override
        public void validate() {
            if (batch < 0) {
                exitWithMessageAndUsage("batch size number must be >= 0");
            }
            if ((blank.equalsIgnoreCase("error")
                    || blank.equalsIgnoreCase("null")
                    || blank.equalsIgnoreCase("empty")) == false) {
                exitWithMessageAndUsage("blank configuration specified must be one of {error|null|empty}");
            }
            if (topic.length() <= 0) {
                exitWithMessageAndUsage("Topic must be specified.");
            }
            if (zookeeper.length() <= 0) {
                exitWithMessageAndUsage("Kafka Zookeeper must be specified.");
            }
        }

        /**
         * Usage
         */
        @Override
        public void printUsage() {
            System.out
                    .println("Usage: kafkaloader [args] -t topic tablename");
            super.printUsage();
        }
    }

    private final static AtomicLong failedCount = new AtomicLong(0);

    static class KafkaBulkLoaderCallback implements BulkLoaderFailureCallBack {

        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse response) {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                long fc = failedCount.incrementAndGet();
                if (config.maxerrors > 0 && fc > config.maxerrors) {
                    System.exit(1);
                }
            }
        }

    }

    private static class KafkaConsumerConnector {

        final String m_host;
        final String m_port;
        ConsumerConfig consumerConfig;
        ConsumerConnector consumer;

        public KafkaConsumerConnector(String host, String port) {
            m_host = host;
            m_port = port;
        }

        public void buildConfig(String a_zookeeper) {
            //Should get this from properties file or something.
            Properties props = new Properties();
            props.put("zookeeper.connect", a_zookeeper);
            props.put("group.id", "exportverifier");
            props.put("zookeeper.session.timeout.ms", "400");
            props.put("zookeeper.sync.time.ms", "200");
            props.put("auto.commit.interval.ms", "1000");
            props.put("auto.commit.enable", "true");
            props.put("auto.offset.reset", "smallest");

            consumerConfig = new ConsumerConfig(props);

            consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
        }

        public void stop() {
            consumer.commitOffsets();
            consumer.shutdown();
        }
    }


    /**
     * kafkaloader main
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     *
     */
    public static void main(String[] args) {

        final KafkaConfig cfg = new KafkaConfig();
        cfg.parse(KafkaLoader.class.getName(), args);

        config = cfg;

        // Split server list
        final String[] serverlist = config.servers.split(",");

        // Create connection
        final ClientConfig c_config = new ClientConfig(config.user, config.password);
        c_config.setProcedureCallTimeout(0); // Set procedure all to infinite
        Client csvClient = null;
        VoltBulkLoader loader = null;
        try {
            csvClient = KafkaLoader.getClient(c_config, serverlist);
            loader = csvClient.getNewBulkLoader(config.table, config.batch, new KafkaBulkLoaderCallback());
        } catch (Exception e) {
            m_log.error("Error connecting to the servers: "
                    + config.servers);
            System.exit(-1);
        }


        ExecutorService es = null;
        assert (csvClient != null);
        assert (loader != null);
        try {
            es = getConsumerExecutor(loader);
            es.awaitTermination(365, TimeUnit.DAYS);
        } catch (Exception ex) {
            m_log.error("Error in getting Kafka Consumer", ex);
        }

        Runtime.getRuntime().addShutdownHook(
                new Thread() {
                    @Override
                    public void run() {
                        for (KafkaConsumerConnector consumerConnector : m_consumers) {
                            consumerConnector.stop();
                        }
                    }
                });

        try {
            loader.drain();
            loader.close();
        } catch (InterruptedException ex) {
        }
        //TODO: process reports
        System.exit(0);
    }

    public static class KafkaConsumer implements Runnable {

        private final KafkaStream m_stream;
        private final VoltBulkLoader m_loader;
        private final CSVParser m_csvParser;

        public KafkaConsumer(KafkaStream a_stream, VoltBulkLoader loader) {
            m_stream = a_stream;
            m_loader = loader;
            m_csvParser = new CSVParser();
        }

        @Override
        public void run() {
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            while (it.hasNext()) {
                byte msg[] = it.next().message();
                String smsg = new String(msg);
                try {
                    m_loader.insertRow(this, (Object[]) m_csvParser.parseLine(smsg));
                } catch (InterruptedException ex) {
                    m_log.error("Consumer stopped", ex);
                } catch (IOException ex) {
                    m_log.error("Consumer stopped", ex);
                }
            }
        }

    }
    static ArrayList<KafkaConsumerConnector> m_consumers = new ArrayList<KafkaConsumerConnector>();

    public static ExecutorService getConsumerExecutor(VoltBulkLoader loader) throws Exception {
        String blist[] = config.brokers.split(",");

        for (String hostString : blist) {
            String split[] = hostString.split(":");
            String host = split[0];
            String port = split[1];
            KafkaConsumerConnector conn = new KafkaConsumerConnector(host, port);

            m_consumers.add(conn);
        }
        for (KafkaConsumerConnector rh : m_consumers) {
            rh.buildConfig(config.zookeeper);
        }

        ExecutorService executor = Executors.newFixedThreadPool(m_consumers.size() * 2);
        KafkaConsumer bconsumer;
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        //Get this from config or arg.
        topicCountMap.put(config.topic, new Integer(3));
        for (KafkaConsumerConnector rh : m_consumers) {
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = rh.consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(config.topic);

            // now launch all the threads for partitions.
            for (final KafkaStream stream : streams) {
                bconsumer = new KafkaConsumer(stream, loader);
                executor.submit(bconsumer);
            }
        }

        return executor;
    }

    /**
     * Get connection to servers in cluster.
     *
     * @param config
     * @param servers
     * @param port
     * @return client
     * @throws Exception
     */
    public static Client getClient(ClientConfig config, String[] servers) throws Exception {
        final Client client = ClientFactory.createClient(config);
        for (String server : servers) {
            int port = Client.VOLTDB_SERVER_PORT;
            String serverandport[] = server.split(":");
            String s = serverandport[0];
            if (serverandport.length > 1) {
                port = Integer.parseInt(serverandport[1]);
            }
            client.createConnection(s.trim(), port);
        }
        return client;
    }
}
