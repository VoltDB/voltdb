/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import au.com.bytecode.opencsv_voltpatches.CSVParser;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
//import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
/**
 * KafkaConsumer loads data from kafka into voltdb
 * Only csv formatted data is supported at this time.
 * VARBINARY columns are not supported
 */
public class KafkaLoader {

    private static final int KAFKA_0_8_2_API = 8;
    private static final int KAFKA_0_10_2_API = 10;
    private static final int DEFAULT_KAFKA_VERSION = KAFKA_0_8_2_API;

    private static final VoltLogger m_log = new VoltLogger("KAFKALOADER");
    private final KafkaCLIOptions m_config;
    private final static AtomicLong m_failedCount = new AtomicLong(0);
    private CSVDataLoader m_loader = null;
    private Client m_client = null;
    private KafkaConsumerConnector m_consumer = null;
    private ExecutorService m_es = null;

    public KafkaLoader(KafkaCLIOptions config) {
        m_config = config;
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

    public void processKafkaMessages() throws Exception {
        // Split server list
        final String[] serverlist = m_config.servers.split(",");

        // If we need to prompt the user for a VoltDB password, do so.
        m_config.password = CLIConfig.readPasswordIfNeeded(m_config.user, m_config.password, "Enter password: ");

        // Create connection
        final ClientConfig c_config = new ClientConfig(m_config.user, m_config.password, null);
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
        m_loader.setFlushInterval(m_config.flush, m_config.flush);
        if (m_config.kversion == KAFKA_0_8_2_API) {
            m_consumer = new KafkaConsumerConnector(m_config);
            try {
                m_es = getConsumerExecutorKafka8(m_consumer, m_loader);
                if (m_config.useSuppliedProcedure) {
                    m_log.info("Kafka Consumer from topic: " + m_config.topic + " Started using procedure: " + m_config.procedure);
                } else {
                    m_log.info("Kafka Consumer from topic: " + m_config.topic + " Started for table: " + m_config.table);
                }
                m_es.awaitTermination(365, TimeUnit.DAYS);
            } catch (Throwable terminate) {
                m_log.error("Error in Kafka Consumer", terminate);
                System.exit(-1);
            }
            close();
        } else if (m_config.kversion == KAFKA_0_10_2_API){
            try {
//                Properties props = getKafka10ConfigProperties();
//                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//                
//                try {
//                    consumer.subscribe(Arrays.asList(m_config.topic));
//                    while (true) {
//                        ConsumerRecords<String, String> records = consumer.poll(100);
//                        for (ConsumerRecord<String, String> record : records) {
//                            System.out.println("topic : " + record.topic() + ", offset: " + record.offset() + " key " 
//                                    + record.key() +", value " + new String (record.value()));
//                        }
//                    }
//                } catch (Throwable terminate) {
//                    System.out.println("error " + terminate.getMessage());
//                    
//                } finally {
//                    consumer.close();
//                }
                m_es = getConsumerExecutor();

                if (m_config.useSuppliedProcedure) {
                    m_log.info("Kafka Consumer from topic: " + m_config.topic + " Started using procedure: " + m_config.procedure);
                } else {
                    m_log.info("Kafka Consumer from topic: " + m_config.topic + " Started for table: " + m_config.table);
                }
                m_es.awaitTermination(365, TimeUnit.DAYS);
            } catch (Throwable terminate) {
                m_log.error("Error encountered Kafka 10 Consumer", terminate);
                System.exit(-1);
            }
        }
    }

    /**
     * Configuration options.
     */
    public static class KafkaCLIOptions extends CLIConfig {

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

        @Option(shortOpt = "z", desc = "kafka zookeeper to connect to. (format: zkserver:port)")
        String zookeeper = ""; //No default here as default will clash with local voltdb cluster

        @Option(desc = "kafka bootstrap server to connect to. (format: kafka-server:port)")
        String kserver = ""; //No default here as default will clash with local voltdb cluster

        @Option(shortOpt = "f", desc = "Periodic Flush Interval in seconds. (default: 10)")
        int flush = 10;

        @Option(shortOpt = "k", desc = "Kafka Topic Partitions. (default: 10)")
        int kpartitions = 10;

        @Option(shortOpt = "c", desc = "Kafka Consumer Configuration File")
        String config = "";

        @Option(desc = "Formatter configuration file. (Optional) .")
        String formatter = "";



        @Option(desc = "Kafka version. " + KAFKA_0_8_2_API + " for Kafka 0.8.2, "
                + KAFKA_0_10_2_API + " for Kafka 0.10.2. "
                + "(Default: " + DEFAULT_KAFKA_VERSION + ")")
        int kversion = DEFAULT_KAFKA_VERSION;

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
            if (kversion == KAFKA_0_10_2_API) {
                if (kserver.trim().isEmpty()) {
                    exitWithMessageAndUsage("Kafka bootstrap server must be specified.");
                }
                if (!zookeeper.trim().isEmpty()) {
                    m_log.warn("Zookeeper server list is ignored for Kafka API version - " + kversion);
                }
            } else if (kversion == KAFKA_0_8_2_API) {
                if (zookeeper.trim().isEmpty()) {
                    exitWithMessageAndUsage("Kafka Zookeeper must be specified.");
                }
                if (!kserver.trim().isEmpty()) {
                    m_log.warn("Kafka bootstrap server list is ignored for Kafka API version - " + kversion);
                }
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

            // TODO: is the zookeeper library needed for 10.0?
            if (kversion == KAFKA_0_8_2_API) {
                //Try and load classes we need and not packaged.
                try {
                    KafkaCLIOptions.class.getClassLoader().loadClass("org.I0Itec.zkclient.IZkStateListener");
                    KafkaCLIOptions.class.getClassLoader().loadClass("org.apache.zookeeper.Watcher");
                } catch (ClassNotFoundException cnfex) {
                    System.out.println("Cannot find the Zookeeper libraries, zkclient-0.3.jar and zookeeper-3.3.4.jar.");
                    System.out.println("Use the ZKLIB environment variable to specify the path to the Zookeeper jars files.");
                    System.exit(1);
                }
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

        final kafka.consumer.ConsumerConfig m_consumerConfig;
        final ConsumerConnector m_consumer;
        final KafkaCLIOptions m_config;

        Properties getKafkaConfigProperties() throws IOException {
            //Get group id which should be unique for table so as to keep offsets clean for multiple runs.
            String groupId = "voltdb-" + (m_config.useSuppliedProcedure ? m_config.procedure : m_config.table);
            Properties props = new Properties();
            // If configuration is provided for consumer pick up
            if (m_config.config.trim().isEmpty()) {
                props.put("zookeeper.session.timeout.ms", "400");
                props.put("zookeeper.sync.time.ms", "200");
                props.put("auto.commit.interval.ms", "1000");
                props.put("auto.commit.enable", "true");
                props.put("auto.offset.reset", "smallest");
                props.put("rebalance.backoff.ms", "10000");
            } else {
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
            }
            props.put("group.id", groupId);
            props.put("zookeeper.connect", m_config.zookeeper);
            return props;
        }

        public KafkaConsumerConnector(KafkaCLIOptions config) throws IOException {
            m_config = config;
            m_consumerConfig = new kafka.consumer.ConsumerConfig(getKafkaConfigProperties());
            m_consumer = kafka.consumer.Consumer.createJavaConsumerConnector(m_consumerConfig);
        }

        public void stop() {
            try {
                // Let offset get pushed to zk....so sleep for auto.commit.interval.ms+
                Thread.sleep(1100);
            } catch (InterruptedException ignore) { }
            finally {
                m_consumer.commitOffsets();
                m_consumer.shutdown();
            }
        }
    }

    public static class Kafka8Consumer implements Runnable {

        private final KafkaStream m_stream;
        private final CSVDataLoader m_loader;
        private final CSVParser m_csvParser;
        private final Formatter m_formatter;
        private final KafkaCLIOptions m_config;

        public Kafka8Consumer(KafkaStream a_stream, CSVDataLoader loader, KafkaCLIOptions config)
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

    private ExecutorService getConsumerExecutorKafka8(KafkaConsumerConnector consumer, CSVDataLoader loader) throws Exception {
        Map<String, Integer> topicCountMap = new HashMap<>();
        // generate as many threads as there are partitions defined in config
        ExecutorService executor = Executors.newFixedThreadPool(m_config.kpartitions);
        topicCountMap.put(m_config.topic, m_config.kpartitions);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.m_consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(m_config.topic);

        // now launch all the threads for partitions.
        for (final KafkaStream stream : streams) {
            Kafka8Consumer bconsumer = new Kafka8Consumer(stream, loader, m_config);
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
        final Client client = ClientFactory.createClient(config);
        for (String server : servers) {
            client.createConnection(server.trim(), port);
        }
        return client;
    }

    private Properties getKafka10ConfigProperties() throws IOException {            //Get group id which should be unique for table so as to keep offsets clean for multiple runs.
        String groupId = "voltdb-" + (m_config.useSuppliedProcedure ? m_config.procedure : m_config.table);

        Properties props = new Properties();
        if (m_config.config.trim().isEmpty()) {
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // kafka's config value is 5000
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // 5 minutes before the consumer will dropped from the group
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
            // max number of records return per poll
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(m_config.batch));
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    //"org.apache.kafka.common.serialization.StringDeserializer");
                    "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    //"org.apache.kafka.common.serialization.StringDeserializer");
                    "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        } else {
            props.load(new FileInputStream(new File(m_config.config)));
            //Get GroupId from property if present and use it.
            groupId = props.getProperty("group.id", groupId);
            //Get zk connection from props file if present.
            m_config.kserver = props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_config.kserver);

            if (props.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) == null) {
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
                if (props.getProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG) == null)
                    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            }

            if (props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) == null)
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            if (props.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) == null)
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");

            if (props.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) == null)
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
        }

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_config.kserver);

        return props;
    }

    private final AtomicBoolean m_shutdown = new AtomicBoolean(false);
    private List<KafkaConsumer<byte[], byte[]>> m_consumers;

    private ExecutorService getConsumerExecutor() throws Exception {
        Properties props = getKafka10ConfigProperties();
        // generate as many threads as there are partitions defined in config
        ExecutorService executor = Executors.newFixedThreadPool(m_config.kpartitions);
        m_consumers = new ArrayList<>();
        try {
            KafkaConsumer<byte[], byte[]> consumer = null;
            for (int i = 0; i < m_config.kpartitions; i++) {
                consumer = new KafkaConsumer<>(props);
                m_consumers.add(consumer);
            }
        } catch (Throwable terminate) {
            m_log.error("Failed creating Kafka consumer ", terminate);
            System.exit(-1);
        }

        for (KafkaConsumer<byte[], byte[]> consumer : m_consumers) {
            executor.submit(new Kafka10ConsumerRunner(m_config, m_loader, consumer));
        }

        return executor;
    }

    class Kafka10ConsumerRunner implements Runnable {
        private KafkaConsumer<byte[], byte[]> m_consumer;
        private KafkaCLIOptions m_config;
        private final CSVDataLoader m_loader;
        private final CSVParser m_csvParser;
        private final Formatter m_formatter;


        Kafka10ConsumerRunner(KafkaCLIOptions config, CSVDataLoader loader, KafkaConsumer<byte[], byte[]> consumer)
                throws FileNotFoundException, IOException, ClassNotFoundException, NoSuchMethodException,
                SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException,
                InvocationTargetException {
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
            m_consumer = consumer;
        }


        // stub for shutdown hook
        void shutdown() {
            if(m_shutdown.compareAndSet(false, true)) {
            }

        }

        @Override
        public void run() {
            try {
                m_consumer.subscribe(Arrays.asList(m_config.topic));
            } catch (IllegalArgumentException topicNotValid) {
                m_log.error("Failed subscribing to the topic", topicNotValid);
                System.exit(-1);
            }

            Set<TopicPartition> partitions = m_consumer.assignment();
            StringBuilder topicPartitions = new StringBuilder();
            for (TopicPartition partition : partitions) {
                topicPartitions.append("Topic: " + partition.topic() + " partition: " + partition.partition() + "\n");
            }
            System.out.println(topicPartitions.toString());

            while (!m_shutdown.get()) {
                ConsumerRecords<byte[], byte[]> records = null;
                try {
                    records = m_consumer.poll(1000); // milliseconds
                } catch (WakeupException wakeup) {
                    System.out.println("Wakeup exception seen " + wakeup);
                    m_log.warn("Received wakeup exception",  wakeup);
                    m_consumer.close();
                    break;
                } catch (Exception terminate) {
                    m_log.error("Consumer got an error when polling for record", terminate);
                    System.exit(-1);
                }

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (record.serializedValueSize() < 0) {
                        m_log.debug("got zero size record at offset " + record.offset());
                    }
                    try {
                        byte[] msg = record.value();
                        long offset = record.offset();
                        String smsg = new String(msg);
                        Object params[];
                        if (m_formatter != null) {
                            try {
                                params = m_formatter.transform(ByteBuffer.wrap(msg));
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
                        System.exit(-1);
                    }
                }
            }
        }

    }

    /**
     * kafkaloader main
     *
     * @param args
     *
     */
    public static void main(String[] args) {

        final KafkaCLIOptions cfg = new KafkaCLIOptions();
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
