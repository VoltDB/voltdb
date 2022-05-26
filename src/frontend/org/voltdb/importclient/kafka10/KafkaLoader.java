/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
package org.voltdb.importclient.kafka10;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.client.AutoReconnectListener;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.importer.ImporterLifecycle;
import org.voltdb.utils.BulkLoaderErrorHandler;
import org.voltdb.utils.CSVBulkDataLoader;
import org.voltdb.utils.CSVDataLoader;
import org.voltdb.utils.CSVTupleDataLoader;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.RowWithMetaData;

/**
 * KafkaConsumer loads data from kafka into voltdb
 * Only csv formatted data is supported at this time.
 */

public class KafkaLoader implements ImporterLifecycle {

    private static final VoltLogger LOGGER = new VoltLogger("KAFKALOADER10");
    private final static AtomicLong FAILED_COUNT = new AtomicLong(0);

    private final KafkaLoaderCLIArguments m_cliOptions;
    private CSVDataLoader m_loader = null;
    private Client m_client = null;
    private ExecutorService m_executorService = null;
    private final AtomicBoolean m_shutdown = new AtomicBoolean(false);
    private List<KafkaExternalConsumerRunner> m_consumers;
    private volatile boolean m_stopping = false;

    public KafkaLoader(KafkaLoaderCLIArguments options) {
        m_cliOptions = options;
    }

    @Override
    public boolean shouldRun() {
        return !m_stopping;
    }

    @Override
    public void stop() {
        m_stopping = true;
    }

    @Override
    public boolean hasTransaction() {
        return false;
    }

    void close() {
        if (m_executorService != null) {
            try {
                m_executorService.shutdownNow();
                m_executorService.awaitTermination(365, TimeUnit.DAYS);
            } catch (Throwable ignore) {
            } finally {
                m_executorService = null;
            }
        }
        if (m_loader != null) {
            try {
                m_loader.close();
                m_loader = null;
            } catch (Throwable ignore) {
            } finally {
                m_loader = null;
            }
        }

        if (m_client != null) {
            try {
                m_client.close();
            } catch (Throwable ignore) {
            } finally {
                m_client = null;
            }
        }
    }

    class KafkaBulkLoaderCallback implements BulkLoaderErrorHandler {

        @Override
        public boolean handleError(RowWithMetaData metaData, ClientResponse response, String error) {
            if (m_cliOptions.maxerrors <= 0 || response == null) {
                return false;
            }

            byte status = response.getStatus();
            if (status != ClientResponse.SUCCESS) {
                LOGGER.error("Failed to insert: " + metaData.rawLine);
                long fc = FAILED_COUNT.incrementAndGet();
                if (fc > m_cliOptions.maxerrors || (status != ClientResponse.USER_ABORT && status != ClientResponse.GRACEFUL_FAILURE)) {
                    notifyShutdown();
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean hasReachedErrorLimit() {
            final long fc = FAILED_COUNT.get();
            return (m_cliOptions.maxerrors > 0 && fc > m_cliOptions.maxerrors);
        }
    }

    private Properties getKafkaConfigFromCLIArguments() throws IOException {

        Properties props = new Properties();
        String groupId = "voltdb-" + (m_cliOptions.useSuppliedProcedure ? m_cliOptions.procedure : m_cliOptions.table);

        if (m_cliOptions.config.trim().isEmpty()) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        } else {
            props.load(new FileInputStream(new File(m_cliOptions.config)));
            groupId = props.getProperty("group.id", groupId);
            m_cliOptions.brokers = props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_cliOptions.brokers);

            String autoCommit = props.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
            if (autoCommit != null && !autoCommit.trim().isEmpty() &&
                    !("true".equals(autoCommit.trim().toLowerCase())) ) {
                LOGGER.warn("Auto commit policy for Kafka loader will be set to \'true\' instead of \'" + autoCommit +"\'");
            }

            if (props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) == null) {
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            }
        }

        // populate/override kafka consumer properties
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_cliOptions.brokers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, m_cliOptions.getMaxPollRecords());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, m_cliOptions.getMaxPollInterval());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, m_cliOptions.getSessionTimeout());
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, m_cliOptions.getRequestTimeout());
        return props;
    }

    private ExecutorService getExecutor() throws Exception {

        Properties consumerProps = getKafkaConfigFromCLIArguments();

        KafkaLoaderConfig cfg = new KafkaLoaderConfig(m_cliOptions);

        ExecutorService executor = Executors.newFixedThreadPool(m_cliOptions.getConsumerCount());
        m_consumers = new ArrayList<>();
        try {
            KafkaConsumer<ByteBuffer, ByteBuffer> consumer = null;
            for (int i = 0; i < m_cliOptions.getConsumerCount(); i++) {
                consumer = new KafkaConsumer<>(consumerProps);
                m_consumers.add(new KafkaExternalConsumerRunner(this, cfg, consumer, m_loader));
            }
        } catch (KafkaException ke) {
            LOGGER.error("Couldn't create Kafka consumer. Please check the configuration paramaters. Error:" + ke.getMessage());
        } catch (Throwable terminate) {
            LOGGER.error("Failed creating Kafka consumer ", terminate);
        }
        //fail to create all consumers
        if (m_consumers.size() != m_cliOptions.getConsumerCount()) {
            for (KafkaExternalConsumerRunner consumer : m_consumers) {
                consumer.shutdown();
            }
            return null;
        }

        for (KafkaExternalConsumerRunner consumer : m_consumers) {
            executor.submit(consumer);
        }
        return executor;
    }

    // shutdown hook to notify kafka consumer threads of shutdown
    public void notifyShutdown() {
        if (m_shutdown.compareAndSet(false, true)) {
            for (KafkaExternalConsumerRunner consumer : m_consumers) {
                consumer.shutdown();
            }
            close();
        }
    }

    private void processKafkaMessages() throws Exception {
        FileReader fr = null;

        // read username and password from txt file
        if (m_cliOptions.credentials != null && !m_cliOptions.credentials.trim().isEmpty()) {
            Properties props = MiscUtils.readPropertiesFromCredentials(m_cliOptions.credentials);
            m_cliOptions.user = props.getProperty("username");
            m_cliOptions.password = props.getProperty("password");
        }

        // If we need to prompt the user for a VoltDB password, do so.
        m_cliOptions.password = CLIConfig.readPasswordIfNeeded(m_cliOptions.user, m_cliOptions.password, "Enter password: ");

        // Create connection
        AutoReconnectListener listener = m_cliOptions.stopondisconnect ? null : new AutoReconnectListener();
        final ClientConfig clientConfig = new ClientConfig(m_cliOptions.user, m_cliOptions.password, listener);
        if (m_cliOptions.ssl != null && !m_cliOptions.ssl.trim().isEmpty()) {
            clientConfig.setTrustStoreConfigFromPropertyFile(m_cliOptions.ssl);
            clientConfig.enableSSL();
        }
        clientConfig.setProcedureCallTimeout(0);
        if (m_cliOptions.priority > 0) {
            clientConfig.setRequestPriority(m_cliOptions.priority);
        }
        m_client = getVoltClient(clientConfig, m_cliOptions.getVoltHosts());

        if (m_cliOptions.useSuppliedProcedure) {
            m_loader = new CSVTupleDataLoader(m_client, m_cliOptions.procedure, new KafkaBulkLoaderCallback());
        } else {
            m_loader = new CSVBulkDataLoader(m_client, m_cliOptions.table, m_cliOptions.batch, m_cliOptions.update, new KafkaBulkLoaderCallback());
        }
        m_loader.setFlushInterval(m_cliOptions.flush, m_cliOptions.flush);

        if (!m_cliOptions.stopondisconnect) {
            listener.setLoader(m_loader);
        }

        if ((m_executorService = getExecutor()) != null) {
            if (m_cliOptions.useSuppliedProcedure) {
                LOGGER.info("Kafka Consumer from topic: " + m_cliOptions.topic + " Started using procedure: " + m_cliOptions.procedure);
            } else {
                LOGGER.info("Kafka Consumer from topic: " + m_cliOptions.topic + " Started for table: " + m_cliOptions.table);
            }
            m_executorService.shutdown();
            m_executorService.awaitTermination(365, TimeUnit.DAYS);
            m_executorService = null;
        }
    }

    /*
     * Create a Volt client from the supplied configuration and list of servers.
     * Connects to the first available server, and uses the topology-awareness
     * features to connect to the rest.
     */
    private static Client getVoltClient(ClientConfig config, List<String> hosts)
            throws IOException, InterruptedException {
        config.setTopologyChangeAware(true);
        Client client = ClientFactory.createClient(config);
        try {
            client.createAnyConnection(String.join(",", hosts));
            return client;
        }
        catch (IOException | RuntimeException ex) {
            client.close();
            throw ex;
        }
    }

    public static void main(String[] args) {

        final KafkaLoaderCLIArguments options = new KafkaLoaderCLIArguments();
        options.parse(KafkaLoader.class.getName(), args);

        KafkaLoader kloader = new KafkaLoader(options);
        try {
            kloader.processKafkaMessages();
        } catch (Exception e) {
            LOGGER.error("Failure in KafkaLoader10 ", e);
        } finally {
            kloader.close();
        }

        System.exit(0);
    }
}
