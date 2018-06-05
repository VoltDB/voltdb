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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.client.AutoReconnectListener;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;
import org.voltdb.utils.BulkLoaderErrorHandler;
import org.voltdb.utils.CSVBulkDataLoader;
import org.voltdb.utils.CSVDataLoader;
import org.voltdb.utils.CSVTupleDataLoader;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.RowWithMetaData;

import au.com.bytecode.opencsv_voltpatches.CSVParser;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * KafkaConsumer loads data from kafka into voltdb
 * Only csv formatted data is supported at this time.
 * VARBINARY columns are not supported
 */
public class KafkaLoader {

    private static final VoltLogger m_log = new VoltLogger("KAFKALOADER");
    private final KafkaConfig m_config;
    private final static AtomicLong m_failedCount = new AtomicLong(0);
    private CSVDataLoader m_loader = null;
    private Client m_client = null;
    private KafkaConsumerConnector m_consumer = null;
    private ExecutorService m_executorService = null;

    public KafkaLoader(KafkaConfig config) {
        m_config = config;
    }

    //Close the consumer after this app will exit.
    public void closeConsumer() throws InterruptedException {
        if (m_consumer != null) {
            m_consumer.stop();
            m_consumer = null;
        }
        if (m_executorService != null) {
            m_executorService.shutdownNow();
            m_executorService.awaitTermination(365, TimeUnit.DAYS);
            m_executorService = null;
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

    public void processKafkaMessages() throws Exception {
        FileReader fr = null;

        // read username and password from txt file
        if (m_config.credentials != null && !m_config.credentials.trim().isEmpty()) {
            Properties props = MiscUtils.readPropertiesFromCredentials(m_config.credentials);
            m_config.user = props.getProperty("username");
            m_config.password = props.getProperty("password");
        }

        // Split server list
        final String[] serverlist = m_config.servers.split(",");

        // If we need to prompt the user for a VoltDB password, do so.
        m_config.password = CLIConfig.readPasswordIfNeeded(m_config.user, m_config.password, "Enter password: ");

        // Create connection
        final ClientConfig c_config;
        AutoReconnectListener listener = new AutoReconnectListener();
        if (m_config.stopondisconnect) {
            c_config = new ClientConfig(m_config.user, m_config.password, null);
            c_config.setReconnectOnConnectionLoss(false);
        } else {
            c_config = new ClientConfig(m_config.user, m_config.password, listener);
            c_config.setReconnectOnConnectionLoss(true);
        }

        if (m_config.ssl != null && !m_config.ssl.trim().isEmpty()) {
            c_config.setTrustStoreConfigFromPropertyFile(m_config.ssl);
            c_config.enableSSL();
        }
        c_config.setProcedureCallTimeout(0); // Set procedure all to infinite

        m_client = getClient(c_config, serverlist, m_config.port);

        if (m_config.useSuppliedProcedure) {
            m_loader = new CSVTupleDataLoader((ClientImpl) m_client, m_config.procedure, new KafkaBulkLoaderCallback());
        } else {
            m_loader = new CSVBulkDataLoader((ClientImpl) m_client, m_config.table, m_config.batch, m_config.update, new KafkaBulkLoaderCallback());
        }
        if (!m_config.stopondisconnect) {
            listener.setLoader(m_loader);
        }
        m_loader.setFlushInterval(m_config.flush, m_config.flush);
        m_consumer = new KafkaConsumerConnector(m_config);
        try {
            m_executorService = getConsumerExecutor(m_consumer, m_loader);
            if (m_config.useSuppliedProcedure) {
                m_log.info("Kafka Consumer from topic: " + m_config.topic + " Started using procedure: " + m_config.procedure);
            } else {
                m_log.info("Kafka Consumer from topic: " + m_config.topic + " Started for table: " + m_config.table);
            }
            m_executorService.awaitTermination(365, TimeUnit.DAYS);
        } catch (Throwable terminate) {
            m_log.error("Error in Kafka Consumer", terminate);
            System.exit(-1);
        }
        close();
    }

    /**
     * Configuration options.
     */
    public static class KafkaConfig extends CLIConfig {

        @Option(shortOpt = "p", desc = "procedure name to insert the data into the database")
        String procedure = "";

        // This is set to true when -p option us used.
        boolean useSuppliedProcedure = false;

        @Option(shortOpt = "t", desc = "Kafka Topic to subscribe to")
        String topic = "";

        @Option(shortOpt = "m", desc = "maximum errors allowed")
        int maxerrors = 100;

        @Option(shortOpt = "s", desc = "list of servers to connect to (default: localhost)")
        String servers = "localhost";

        @Option(desc = "port to use when connecting to database (default: 21212)")
        int port = Client.VOLTDB_SERVER_PORT;

        @Option(desc = "username when connecting to the servers")
        String user = "";

        @Option(desc = "password to use when connecting to servers")
        String password = "";

        @Option(desc = "credentials that contains username and password information")
        String credentials = "";

        @Option(shortOpt = "z", desc = "kafka zookeeper to connect to. (format: zkserver:port)")
        String zookeeper = ""; //No default here as default will clash with local voltdb cluster

        @Option(shortOpt = "f", desc = "Periodic Flush Interval in seconds. (default: 10)")
        int flush = 10;

        @Option(shortOpt = "k", desc = "Kafka Topic Partitions. (default: 10)")
        int kpartitions = 10;

        @Option(shortOpt = "c", desc = "Kafka Consumer Configuration File")
        String config = "";

        @Option(desc = "Formatter configuration file. (Optional) .")
        String formatter = "";

        /**
         * Batch size for processing batched operations.
         */
        @Option(desc = "Batch Size for processing.")
        public int batch = 200;

        /**
         * Table name to insert CSV data into.
         */
        @AdditionalArgs(desc = "insert the data into this table.")
        public String table = "";

        @Option(desc = "Use upsert instead of insert", hasArg = false)
        boolean update = false;

        @Option(desc = "Enable SSL, Optionally provide configuration file.")
        String ssl = "";

        @Option(desc = "Stop when all connections are lost", hasArg = false)
        boolean stopondisconnect = false;

        //Read properties from formatter option and do basic validation.
        Properties m_formatterProperties = new Properties();
        /**
         * Validate command line options.
         */
        @Override
        public void validate() {
            if (batch < 0) {
                exitWithMessageAndUsage("batch size number must be >= 0");
            }
            if (flush <= 0) {
                exitWithMessageAndUsage("Periodic Flush Interval must be > 0");
            }
            if (topic.trim().isEmpty()) {
                exitWithMessageAndUsage("Topic must be specified.");
            }
            if (zookeeper.trim().isEmpty()) {
                exitWithMessageAndUsage("Kafka Zookeeper must be specified.");
            }
            if (port < 0) {
                exitWithMessageAndUsage("port number must be >= 0");
            }
            if (procedure.trim().isEmpty() && table.trim().isEmpty()) {
                exitWithMessageAndUsage("procedure name or a table name required");
            }
            if (!procedure.trim().isEmpty() && !table.trim().isEmpty()) {
                exitWithMessageAndUsage("Only a procedure name or a table name required, pass only one please");
            }
            if (!procedure.trim().isEmpty()) {
                useSuppliedProcedure = true;
            }
            if ((useSuppliedProcedure) && (update)){
                update = false;
                exitWithMessageAndUsage("update is not applicable when stored procedure specified");
            }
            //Try and load classes we need and not packaged.
            try {
                KafkaConfig.class.getClassLoader().loadClass("org.I0Itec.zkclient.IZkStateListener");
                KafkaConfig.class.getClassLoader().loadClass("org.apache.zookeeper.Watcher");
            } catch (ClassNotFoundException cnfex) {
                System.out.println("Cannot find the Zookeeper libraries, zkclient-0.3.jar and zookeeper-3.3.4.jar.");
                System.out.println("Use the ZKLIB environment variable to specify the path to the Zookeeper jars files.");
                System.exit(1);
            }
        }

        /**
         * Usage
         */
        @Override
        public void printUsage() {
            System.out.println("Usage: kafkaloader [args] -z kafka-zookeeper -t topic tablename");
            super.printUsage();
        }
    }

    class KafkaBulkLoaderCallback implements BulkLoaderErrorHandler {

        @Override
        public boolean handleError(RowWithMetaData metaData, ClientResponse response, String error) {
            if (m_config.maxerrors <= 0) return false;
            if (response != null) {
                byte status = response.getStatus();
                if (status != ClientResponse.SUCCESS) {
                    m_log.error("Failed to Insert Row: " + metaData.rawLine);
                    long fc = m_failedCount.incrementAndGet();
                    if ((m_config.maxerrors > 0 && fc > m_config.maxerrors)
                            || (status != ClientResponse.USER_ABORT && status != ClientResponse.GRACEFUL_FAILURE)) {
                        try {
                            m_log.error("Kafkaloader will exit.");
                            closeConsumer();
                            return true;
                        } catch (InterruptedException ex) {
                        }
                    }
                }
            }
            return false;
        }

        @Override
        public boolean hasReachedErrorLimit() {
            long fc = m_failedCount.get();
            return (m_config.maxerrors > 0 && fc > m_config.maxerrors);
        }
    }

    private static class KafkaConsumerConnector {

        final ConsumerConfig m_consumerConfig;
        final ConsumerConnector m_consumer;
        final KafkaConfig m_config;

        public KafkaConsumerConnector(KafkaConfig config) throws IOException {
            m_config = config;
            //Get group id which should be unique for table so as to keep offsets clean for multiple runs.
            String groupId = "voltdb-" + (m_config.useSuppliedProcedure ? m_config.procedure : m_config.table);
            Properties props = new Properties();
            // If configuration is provided for consumer pick up
            if (!m_config.config.trim().isEmpty()) {
                props.load(new FileInputStream(new File(m_config.config)));
                //Get GroupId from property if present and use it.
                groupId = props.getProperty("group.id", groupId);
                //Get zk connection from props file if present.
                m_config.zookeeper = props.getProperty("zookeeper.connect", m_config.zookeeper);
                if (props.getProperty("zookeeper.session.timeout.ms") == null)
                    props.put("zookeeper.session.timeout.ms", "400");
                if (props.getProperty("zookeeper.sync.time.ms") == null)
                    props.put("zookeeper.sync.time.ms", "200");
                if (props.getProperty("auto.commit.interval.ms") == null)
                    props.put("auto.commit.interval.ms", "1000");
                if (props.getProperty("auto.commit.enable") == null)
                    props.put("auto.commit.enable", "true");
                if (props.getProperty("auto.offset.reset") == null)
                    props.put("auto.offset.reset", "smallest");
                if (props.getProperty("rebalance.backoff.ms") == null)
                    props.put("rebalance.backoff.ms", "10000");
            } else {
                props.put("zookeeper.session.timeout.ms", "400");
                props.put("zookeeper.sync.time.ms", "200");
                props.put("auto.commit.interval.ms", "1000");
                props.put("auto.commit.enable", "true");
                props.put("auto.offset.reset", "smallest");
                props.put("rebalance.backoff.ms", "10000");
            }
            props.put("group.id", groupId);
            props.put("zookeeper.connect", m_config.zookeeper);
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
        private final CSVDataLoader m_loader;
        private final CSVParser m_csvParser;
        private final Formatter m_formatter;
        private final KafkaConfig m_config;

        public KafkaConsumer(KafkaStream a_stream, CSVDataLoader loader, KafkaConfig config)
                throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
            m_stream = a_stream;
            m_loader = loader;
            m_csvParser = new CSVParser();
            m_config = config;
            if (m_config.m_formatterProperties.size() > 0) {
                String formatter = m_config.m_formatterProperties.getProperty("formatter");
                String format = m_config.m_formatterProperties.getProperty("format", "csv");
                Class classz = Class.forName(formatter);
                Class[] ctorParmTypes = new Class[]{ String.class, Properties.class };
                Constructor ctor = classz.getDeclaredConstructor(ctorParmTypes);
                Object[] ctorParms = new Object[]{ format, m_config.m_formatterProperties };
                m_formatter = (Formatter )ctor.newInstance(ctorParms);
            } else {
                m_formatter = null;
            }
        }

        @Override
        public void run() {
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            while (it.hasNext()) {
                MessageAndMetadata<byte[], byte[]> md = it.next();
                byte msg[] = md.message();
                long offset = md.offset();
                String smsg = new String(msg);
                try {
                    Object params[];
                    if (m_formatter != null) {
                        try {
                            params = m_formatter.transform(ByteBuffer.wrap(smsg.getBytes()));
                        } catch (FormatException fe) {
                            m_log.warn("Failed to transform message: " + smsg);
                            continue;
                        }
                    } else {
                        params = m_csvParser.parseLine(smsg);
                    }
                    if (params == null) continue;
                    m_loader.insertRow(new RowWithMetaData(smsg, offset), params);
                } catch (Throwable terminate) {
                    m_log.error("Consumer stopped", terminate);
                    System.exit(1);
                }
            }
        }

    }

    private ExecutorService getConsumerExecutor(KafkaConsumerConnector consumer, CSVDataLoader loader) throws Exception {
        Map<String, Integer> topicCountMap = new HashMap<>();
        // generate as many threads as there are partitions defined in kafka config
        ExecutorService executor = Executors.newFixedThreadPool(m_config.kpartitions);
        topicCountMap.put(m_config.topic, m_config.kpartitions);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.m_consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(m_config.topic);

        // now launch all the threads for partitions.
        for (final KafkaStream stream : streams) {
            KafkaConsumer bconsumer = new KafkaConsumer(stream, loader, m_config);
            executor.submit(bconsumer);
        }

        return executor;
    }

    /**
     * Get connection to servers in cluster.
     *
     * @param config
     * @param servers
     * @return client
     * @throws Exception
     */
    public static Client getClient(ClientConfig config, String[] servers, int port) throws Exception {
        config.setTopologyChangeAware(true);
        final Client client = ClientFactory.createClient(config);
        for (String server : servers) {
            try {
                client.createConnection(server.trim(), port);
            } catch (IOException e) {
                // Only swallow exceptions caused by Java network or connection problem
                // Unresolved hostname exceptions will be thrown
            }
        }
        if (client.getConnectedHostList().isEmpty()) {
            try {
                client.close();
            } catch (Exception ignore) {}
            throw new Exception("Unable to connect to any servers");
        }
        return client;
    }

    /**
     * kafkaloader main
     *
     * @param args
     *
     */
    public static void main(String[] args) {

        final KafkaConfig cfg = new KafkaConfig();
        cfg.parse(KafkaLoader.class.getName(), args);
        try {
            if (!cfg.formatter.trim().isEmpty()) {
                InputStream pfile = new FileInputStream(cfg.formatter);
                cfg.m_formatterProperties.load(pfile);
                String formatter = cfg.m_formatterProperties.getProperty("formatter");
                if (formatter == null || formatter.trim().isEmpty()) {
                    m_log.error("formatter class must be specified in formatter file as formatter=<class>: " + cfg.formatter);
                    System.exit(-1);
                }
            }
            KafkaLoader kloader = new KafkaLoader(cfg);
            kloader.processKafkaMessages();
        } catch (Exception e) {
            m_log.error("Failure in kafkaloader", e);
            System.exit(-1);
        }

        System.exit(0);
    }


}
