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
import au.com.bytecode.opencsv_voltpatches.CSVParser;

/**
 * KafkaConsumer loads data from kafka into voltdb
 * Only csv formatted data is supported at this time.
 * VARBINARY columns are not supported
 */

public class KafkaLoader10 {
    private static final VoltLogger m_log = new VoltLogger("KAFKALOADER10");
    private static final String KEY_DESERIALIZER = ByteArrayDeserializer.class.getName();
    private static final String VALUE_DESERIALIZER = ByteArrayDeserializer.class.getName();

    private CLIOptions m_cliOptions;
    private final static AtomicLong m_failedCount = new AtomicLong(0);
    private CSVDataLoader m_loader = null;
    private Client m_client = null;
    private ExecutorService m_executorService = null;
    private final AtomicBoolean m_shutdown = new AtomicBoolean(false);
    private List<Kafka10ConsumerRunner> m_consumers;
    private final long pollTimedWaitInMilliSec = Integer.getInteger("KAFKALOADER_POLLED_WAIT_MILLI_SECONDS", 1000); // 1 second

    public KafkaLoader10(CLIOptions options) {
        m_cliOptions = options;
    }

    private void shutdownExecutorNow() {
        if (m_executorService == null) return;
        try {
            m_executorService.shutdownNow();
            m_executorService.awaitTermination(365, TimeUnit.DAYS);
        } catch (Throwable ignore) {
        } finally {
            m_executorService = null;
        }
    }

    private void closeLoader() {
        if (m_loader == null) return;
        try {
            m_loader.close();
            m_loader = null;
        } catch (Throwable ignore) {
        } finally {
            m_loader = null;
        }
    }

    private void closeClient() {
        if (m_client == null) return;
        try {
            m_client.close();
        } catch (Throwable ignore) {
        } finally {
            m_client = null;
        }
    }

    void close() {
        shutdownExecutorNow();
        closeLoader();
        closeClient();
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

        @Option(shortOpt = "s", desc = "Comma separated list of the form server[:port] to connect to (default: localhost:"
                + Client.VOLTDB_SERVER_PORT + ")")
        String servers = "localhost:" + Client.VOLTDB_SERVER_PORT;

        @Option(desc = "Username when connecting to the database")
        String user = "";

        @Option(desc = "Password to use when connecting to database")
        String password = "";

        @Option(shortOpt = "b", desc = "Comma separated list of kafka broker(s) to connect to. (format: broker:port)")
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
        @Option(desc = "Batch Size for processing (default: 200)")
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
            if (servers.trim().isEmpty()) {
                exitWithMessageAndUsage("Provided server list can't be empty");
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
                exitWithMessageAndUsage("Update is not applicable when stored procedure specified");
            }
        }

        /**
         * Usage
         */
        @Override
        public void printUsage() {
            System.out.println("Usage: kafkaloader10 [args] -b kafka-brokers -t topic tablename");
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
                        notifyShutdown();
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public boolean hasReachedErrorLimit() {
            long fc = m_failedCount.get();
            return (m_cliOptions.maxerrors > 0 && fc > m_cliOptions.maxerrors);
        }
    }

    private Properties kafkaConfigProperties() throws IOException {
        //Get group id which should be unique for table so as to keep offsets clean for multiple runs.
        String groupId = "voltdb-" + (m_cliOptions.useSuppliedProcedure ? m_cliOptions.procedure : m_cliOptions.table);

        Properties props = new Properties();
        if (m_cliOptions.config.trim().isEmpty()) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // default liveness check of consumer is 5 minutes
        } else {
            props.load(new FileInputStream(new File(m_cliOptions.config)));
            //Get GroupId from property if present and use it.
            groupId = props.getProperty("group.id", groupId);

            // get kafka broker connections from properties file if present - supplied brokers, if any, overrides
            // the supplied command line
            m_cliOptions.brokers = props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_cliOptions.brokers);

            String autoCommit = props.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
            if (autoCommit != null && !autoCommit.trim().isEmpty() &&
                    !("true".equals(autoCommit.trim().toLowerCase())) ) {
                m_log.warn("Auto commit policy for Kafka loader will be set to \'true\' instead of \'" + autoCommit +"\'");
            }

            if (props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) == null)
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // Only byte array are used by Kafka Loader. If there are any deserializer supplied in config file
            // log warning message about it
            String deserializer = props.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
            if (deserializer != null && KEY_DESERIALIZER.equals(deserializer.trim()) ) {
                m_log.warn("Key deserializer \'" + deserializer.trim() + "\' not supported. \'"
                        + KEY_DESERIALIZER + "\' will be used for deserializering keys");
            }
            deserializer = props.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
            if ( deserializer != null && VALUE_DESERIALIZER.equals(deserializer.trim())) {
                m_log.warn("Value deserializer \'" + deserializer.trim() + "\' not supported. \'"
                        + VALUE_DESERIALIZER + "\' will be used for deserializering values");
            }
        }

        // populate/override kafka consumer properties
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_cliOptions.brokers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        return props;
    }

    private ExecutorService getExecutor() throws Exception {
        Properties props = kafkaConfigProperties();
        // create as many threads equal as number of partitions specified in config
        ExecutorService executor = Executors.newFixedThreadPool(m_cliOptions.kpartitions);
        m_consumers = new ArrayList<>();
        try {
            KafkaConsumer<byte[], byte[]> consumer = null;
            for (int i = 0; i < m_cliOptions.kpartitions; i++) {
                consumer = new KafkaConsumer<>(props);
                m_consumers.add(new Kafka10ConsumerRunner(m_cliOptions, m_loader, consumer));

            }
        } catch (Throwable terminate) {
            m_log.error("Failed creating Kafka consumer ", terminate);
            for (Kafka10ConsumerRunner consumer : m_consumers) {
                // close all consumer connections
                consumer.forceClose();
            }
            return null;
        }

        for (Kafka10ConsumerRunner consumer : m_consumers) {
            executor.submit(consumer);
        }
        return executor;
    }

    // shutdown hook to notify kafka consumer threads of shutdown
    private void notifyShutdown() {
        if (m_shutdown.compareAndSet(false, true)) {
            m_log.info("Kafka consumer shutdown signalled ... ");
            for (Kafka10ConsumerRunner consumer : m_consumers) {
                consumer.shutdown();
            }
        }
    }

    class Kafka10ConsumerRunner implements Runnable {
        private KafkaConsumer<byte[], byte[]> m_consumer;
        private CLIOptions m_config;
        private final CSVDataLoader m_loader;
        private final CSVParser m_csvParser;
        private final Formatter m_formatter;
        private AtomicBoolean m_closed = new AtomicBoolean(false);

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

        void forceClose() {
            m_closed.set(true);
            try {
                m_consumer.close();
            } catch (Exception ignore) {}
        }

        void shutdown() {
            if (m_closed.compareAndSet(false,  true)) {
                m_consumer.wakeup();
            }

        }

        @Override
        public void run() {
            String smsg = null;
            try {
                m_consumer.subscribe(Arrays.asList(m_config.topic));
                while (!m_closed.get()) {
                    ConsumerRecords<byte[], byte[]> records = m_consumer.poll(pollTimedWaitInMilliSec);
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        byte[] msg  = record.value();
                        long offset = record.offset();
                        smsg = new String(msg);
                        Object params[];
                        if (m_formatter != null) {
                            try {
                                params = m_formatter.transform(ByteBuffer.wrap(msg));
                            } catch (FormatException badMsg) {
                                m_log.warn("Failed to transform message " + smsg + " at offset " + offset
                                        + ", error message: " + badMsg.getMessage());
                                continue;
                            }
                        } else {
                            params = m_csvParser.parseLine(smsg);
                        }
                        if (params == null) continue;
                        m_loader.insertRow(new RowWithMetaData(smsg, offset), params);
                    }
                }
            } catch (IllegalArgumentException invalidTopic) {
                m_closed.set(true);
                m_log.error("Failed subscribing to the topic " + m_config.topic, invalidTopic);
            } catch (WakeupException wakeup) {
                m_closed.set(true);
                m_log.debug("Consumer signalled to terminate ", wakeup);
            } catch (IOException ioExcp) {
                m_closed.set(true);
                if (m_formatter == null) {
                    m_log.error("Failed to parse message" + smsg);
                } else {
                    m_log.error("Error seen when processing message ", ioExcp);
                }
            } catch (Throwable terminate) {
                m_closed.set(true);
                m_log.error("Error seen during poll", terminate);
            } finally {
                try {
                    m_consumer.close();
                } catch (Exception ignore) {}
                notifyShutdown();
            }
        }
    }

    private void processKafkaMessages() throws Exception {
        // Split server list
        final String[] serverlist = m_cliOptions.servers.split(",");

        // If we need to prompt the user for a VoltDB password, do so.
        m_cliOptions.password = CLIConfig.readPasswordIfNeeded(m_cliOptions.user, m_cliOptions.password, "Enter password: ");

        // Create connection
        final ClientConfig clientConfig = new ClientConfig(m_cliOptions.user, m_cliOptions.password, null);
        if (m_cliOptions.ssl != null && !m_cliOptions.ssl.trim().isEmpty()) {
            clientConfig.setTrustStoreConfigFromPropertyFile(m_cliOptions.ssl);
            clientConfig.enableSSL();
        }
        clientConfig.setProcedureCallTimeout(0);
        m_client = getClient(clientConfig, serverlist);

        if (m_cliOptions.useSuppliedProcedure) {
            m_loader = new CSVTupleDataLoader((ClientImpl) m_client, m_cliOptions.procedure, new KafkaBulkLoaderCallback());
        } else {
            m_loader = new CSVBulkDataLoader((ClientImpl) m_client, m_cliOptions.table, m_cliOptions.batch, m_cliOptions.update, new KafkaBulkLoaderCallback());
        }
        m_loader.setFlushInterval(m_cliOptions.flush, m_cliOptions.flush);

        if ((m_executorService = getExecutor()) != null) {
            if (m_cliOptions.useSuppliedProcedure) {
                m_log.info("Kafka Consumer from topic: " + m_cliOptions.topic + " Started using procedure: " + m_cliOptions.procedure);
            } else {
                m_log.info("Kafka Consumer from topic: " + m_cliOptions.topic + " Started for table: " + m_cliOptions.table);
            }
            m_executorService.shutdown();
            m_executorService.awaitTermination(365, TimeUnit.DAYS);
            m_executorService = null;
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
    public static Client getClient(ClientConfig config, String[] servers) throws Exception {
        config.setTopologyChangeAware(true);
        final Client client = ClientFactory.createClient(config);
        for (String server : servers) {
            try {
                client.createConnection(server.trim());
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
                m_log.error("Formatter class must be specified in formatter file as formatter=<class>: " + options.formatter);
                System.exit(-1);
            }
        }
        KafkaLoader10 kloader = new KafkaLoader10(options);
        try {
            kloader.processKafkaMessages();
        } catch (Exception e) {
            m_log.error("Failure in KafkaLoader10 ", e);
        } finally {
            kloader.close();
        }

        System.exit(0);
    }

}
