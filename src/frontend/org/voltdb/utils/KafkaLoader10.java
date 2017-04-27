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
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

/**
 * KafkaConsumer loads data from kafka into voltdb
 * Only csv formatted data is supported at this time.
 * VARBINARY columns are not supported
 */

public class KafkaLoader10 {
    private static final VoltLogger m_log = new VoltLogger("KAFKALOADER8");
    private CLIOptions m_cliOptions;
    private final static AtomicLong m_failedCount = new AtomicLong(0);
    private CSVDataLoader m_loader = null;
    private Client m_client = null;

    private ExecutorService m_es = null;

    public KafkaLoader10(CLIOptions options) {
        m_cliOptions = options;
    }
    public static class CLIOptions extends CLIConfig {

        @Option(shortOpt = "p", desc = "Procedure name to insert the data into the database")
        String procedure = "";

        // This is set to true when -p option us used.
        boolean useSuppliedProcedure = false;

        @Option(shortOpt = "t", desc = "Kafka topic to subscribe to")
        String topic = "";

        @Option(shortOpt = "m", desc = "Maximum errors allowed (default: 100)")
        int maxerrors = 100;

        //TODO: migrate to server:port option
        @Option(shortOpt = "s", desc = "List of servers to connect to (default: localhost)")
        String servers = "localhost";

        //TODO: remove port option
        @Option(desc = "Port to use when connecting to database (default: 21212)")
        int port = Client.VOLTDB_SERVER_PORT;

        @Option(desc = "username when connecting to the database")
        String user = "";

        @Option(desc = "Password to use when connecting to database")
        String password = "";

        @Option(shortOpt = "b", desc = "Kafka bootstrap server / broker to connect to. (format: broker:port)")
        String brokers = ""; //No default here as default will clash with local voltdb cluster

        @Option(shortOpt = "f", desc = "Periodic Flush Interval in seconds (default: 10)")
        int flush = 10;

        @Option(shortOpt = "k", desc = "Kafka topic partitions (default: 10)")
        int kpartitions = 10;

        @Option(shortOpt = "c", desc = "Kafka consumer configuration file")
        String config = "";

        @Option(desc = "Formatter configuration file (optional)")
        String formatter = "";

        /**
         * Batch size for processing batched operations.
         */
        @Option(desc = "Batch Size for processing.")
        public int batch = 200;

        /**
         * Table name to insert CSV data into.
         */
        @AdditionalArgs(desc = "Insert the data into this table.")
        public String table = "";

        @Option(desc = "Use upsert instead of insert", hasArg = false)
        boolean update = false;

        @Option(desc = "Enable SSL, optionally provide configuration file")
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
            if (brokers.trim().isEmpty()) {
                exitWithMessageAndUsage("Kafka bootstrap server must be specified.");
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
        }

        /**
         * Usage
         */
        @Override
        public void printUsage() {
            System.out.println("Usage: kafkaloader [args] -b kafka-brokers -t topic tablename");
            super.printUsage();
        }
    }

    class KafkaBulkLoaderCallback implements BulkLoaderErrorHandler {
        @Override
        public boolean handleError(RowWithMetaData metaData, ClientResponse response, String error) {
            if (m_cliOptions.maxerrors <= 0) return false;
            if (response != null) {
                byte status = response.getStatus();
                if (status != ClientResponse.SUCCESS) {
                    m_log.error("Failed to Insert Row: " + metaData.rawLine);
                    long fc = m_failedCount.incrementAndGet();
                    if ((m_cliOptions.maxerrors > 0 && fc > m_cliOptions.maxerrors)
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

      //Close the consumer after this app will exit.
        public void closeConsumer() throws InterruptedException {
            // TODO: close kafka consumer
            if (m_es != null) {
                m_es.shutdownNow();
                m_es.awaitTermination(365, TimeUnit.DAYS);
                m_es = null;
            }
        }

        @Override
        public boolean hasReachedErrorLimit() {
            long fc = m_failedCount.get();
            return (m_cliOptions.maxerrors > 0 && fc > m_cliOptions.maxerrors);
        }
    }

    private final AtomicBoolean m_shutdown = new AtomicBoolean(false);
    private List<KafkaConsumer<byte[], byte[]>> m_consumers;


    private Properties kafkaConfigProperties() throws IOException {
        //Get group id which should be unique for table so as to keep offsets clean for multiple runs.
        String groupId = "voltdb-" + (m_cliOptions.useSuppliedProcedure ? m_cliOptions.procedure : m_cliOptions.table);

        Properties props = new Properties();
        if (m_cliOptions.config.trim().isEmpty()) {
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // kafka's config value is 5000
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // 5 minutes before the consumer will dropped from the group
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
            // max number of records return per poll
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(m_cliOptions.batch));
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
//                    StringDeserializer.class.getName());    // org.apache.kafka.common.serialization.StringDeserializer
                    ByteArrayDeserializer.class.getName()); // org.apache.kafka.common.serialization.ByteArrayDeserializer
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
//                    StringDeserializer.class.getName());    // org.apache.kafka.common.serialization.StringDeserializer
                    ByteArrayDeserializer.class.getName()); // org.apache.kafka.common.serialization.ByteArrayDeserializer
        } else {
            props.load(new FileInputStream(new File(m_cliOptions.config)));
            //Get GroupId from property if present and use it.
            groupId = props.getProperty("group.id", groupId);
            //Get zk connection from props file if present.
            m_cliOptions.brokers = props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_cliOptions.brokers);

            if (props.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) == null) {
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
                if (props.getProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG) == null)
                    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            }

            if (props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) == null)
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            if (props.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) == null)
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class.getName()); // org.apache.kafka.common.serialization.StringDeserializer

            if (props.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) == null)
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class.getName()); // org.apache.kafka.common.serialization.StringDeserializer
        }

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_cliOptions.brokers);

        return props;
    }

    private ExecutorService getConsumerExecutor() throws Exception {
        Properties props = kafkaConfigProperties();
        // create as many threads equal as number of partitions specified in config
        ExecutorService executor = Executors.newFixedThreadPool(m_cliOptions.kpartitions);
        m_consumers = new ArrayList<>();
        try {
            KafkaConsumer<byte[], byte[]> consumer = null;
            for (int i = 0; i < m_cliOptions.kpartitions; i++) {
                consumer = new KafkaConsumer<>(props);
                m_consumers.add(consumer);
            }
        } catch (Throwable terminate) {
            m_log.error("Failed creating Kafka consumer ", terminate);
            System.exit(-1);
        }

        for (KafkaConsumer<byte[], byte[]> consumer : m_consumers) {
            executor.submit(new Kafka10ConsumerRunner(m_cliOptions, m_loader, consumer));
        }

        return executor;
    }


    class Kafka10ConsumerRunner implements Runnable {
        private KafkaConsumer<byte[], byte[]> m_consumer;
        private CLIOptions m_config;
        private final CSVDataLoader m_loader;
        private final CSVParser m_csvParser;
        private final Formatter m_formatter;


        Kafka10ConsumerRunner(CLIOptions config, CSVDataLoader loader, KafkaConsumer<byte[], byte[]> consumer)
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

//            Set<TopicPartition> partitions = m_consumer.assignment();
//            StringBuilder topicPartitions = new StringBuilder();
//            for (TopicPartition partition : partitions) {
//                topicPartitions.append("Topic: " + partition.topic() + " partition: " + partition.partition() + "\n");
//            }
//            System.out.println(topicPartitions.toString());

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

    public void processKafkaMessages() throws Exception {
        // Split server list
        final String[] serverlist = m_cliOptions.servers.split(",");

        // If we need to prompt the user for a VoltDB password, do so.
        m_cliOptions.password = CLIConfig.readPasswordIfNeeded(m_cliOptions.user, m_cliOptions.password, "Enter password: ");

        // Create connection
        final ClientConfig c_config = new ClientConfig(m_cliOptions.user, m_cliOptions.password, null);
        if (m_cliOptions.ssl != null && !m_cliOptions.ssl.trim().isEmpty()) {
            c_config.setTrustStoreConfigFromPropertyFile(m_cliOptions.ssl);
            c_config.enableSSL();
        }
        c_config.setProcedureCallTimeout(0); // Set procedure all to infinite

        m_client = getClient(c_config, serverlist, m_cliOptions.port);

        if (m_cliOptions.useSuppliedProcedure) {
            m_loader = new CSVTupleDataLoader((ClientImpl) m_client, m_cliOptions.procedure, new KafkaBulkLoaderCallback());
        } else {
            m_loader = new CSVBulkDataLoader((ClientImpl) m_client, m_cliOptions.table, m_cliOptions.batch, m_cliOptions.update, new KafkaBulkLoaderCallback());
        }
        m_loader.setFlushInterval(m_cliOptions.flush, m_cliOptions.flush);
        try {
//            Properties props = kafkaConfigProperties();
//            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
//            CSVParser parser = new CSVParser();
//
//            try {
//                consumer.subscribe(Arrays.asList(m_cliOptions.topic));
//                while (true) {
//                    ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
//                    for (ConsumerRecord<byte[], byte[]> record : records) {
//                        int serializedValueSize = record.serializedValueSize();
//                        long offset = record.offset();
//
//                        byte[] rawKey = record.key();
//                        byte[] rawValue = record.value();
//
//                        String smsg = new String(rawValue);
//                        System.out.println("topic : " + record.topic() + ", offset: " + offset
//                                + " key " + new String(rawKey) +", value " + smsg
//                                + " serialized size: " + serializedValueSize);
//                        Object params[] = parser.parseLine(smsg);
//                        if (params == null) continue;
//                        m_loader.insertRow(new RowWithMetaData(smsg, offset), params);
//                    }
//                }
//            } catch (Throwable terminate) {
//                terminate.printStackTrace();
//                System.out.println("error " + terminate);
//                m_log.error("Consumer dieeddd !!!!! ", terminate);
//            } finally {
//                consumer.close();
//            }

            m_es = getConsumerExecutor();

            if (m_cliOptions.useSuppliedProcedure) {
                m_log.info("Kafka Consumer from topic: " + m_cliOptions.topic + " Started using procedure: " + m_cliOptions.procedure);
            } else {
                m_log.info("Kafka Consumer from topic: " + m_cliOptions.topic + " Started for table: " + m_cliOptions.table);
            }
            m_es.awaitTermination(365, TimeUnit.DAYS);
        } catch (Throwable terminate) {
            m_log.error("Error encountered Kafka 10 Consumer", terminate);
            System.exit(-1);
        }
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

    /**
     * kafkaloader main
     *
     * @param args
     *
     */
    public static void main(String[] args) {
        final CLIOptions options = new CLIOptions();
        options.parse(KafkaLoader10.class.getName(), args);

        if (!options.formatter.trim().isEmpty()) {
            try {
                InputStream pfile = new FileInputStream(options.formatter);
                options.m_formatterProperties.load(pfile);
            } catch (IOException terminate) {
                m_log.error("Failed to load formatter properties", terminate);
                System.exit(-1);
            }
            String formatter = options.m_formatterProperties.getProperty("formatter");
            if (formatter == null || formatter.trim().isEmpty()) {
                m_log.error("formatter class must be specified in formatter file as formatter=<class>: " + options.formatter);
                System.exit(-1);
            }
        }
        KafkaLoader10 kloader = new KafkaLoader10(options);
        try {
            kloader.processKafkaMessages();
        } catch (Exception e) {
            m_log.error("Failure in KafkaLoader10 ", e);
            System.exit(-1);
        }

        System.exit(0);
    }

}
