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

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.voltcore.logging.VoltLogger;
import org.voltdb.importer.AbstractImporter;

public class KafkaStreamImporter extends AbstractImporter {

    private static final VoltLogger LOGGER = new VoltLogger("KAFKAIMPORTER");

    protected KafkaStreamImporterConfig m_config;
    protected KafkaConsumerRunner m_runner;

    private ExecutorService m_executorService = null;
    private List<KafkaInternalConsumerRunner> m_consumers;
    private final AtomicBoolean m_shutdown = new AtomicBoolean(false);
    private final Object m_lock = new Object();
    public KafkaStreamImporter(KafkaStreamImporterConfig config) {
        super();
        m_config = config;
    }

    @Override
    public String getName() {
       return KafkaStreamImporter.class.getName();
    }

    @Override
    public URI getResourceID() {
        return m_config.getURI();
    }

    /**
     * Create a Kafka consumer and runner.
     *
     * @param properties Kafka consumer properties
     * @throws Exception on error
     */
    private KafkaInternalConsumerRunner createConsumerRunner(Properties properties) throws Exception {

        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        try {
            Consumer<ByteBuffer, ByteBuffer> consumer = new KafkaConsumer<>(properties);
            return new KafkaInternalConsumerRunner(this, m_config, consumer);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Override
    public void accept() {

        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, m_config.getGroupId());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_config.getBrokers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, m_config.getMaxMessageFetchSize());
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, m_config.getConsumerRequestTimeout());
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, m_config.getMaxPartitionFetchBytes());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, m_config.getMaxPollRecords());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, m_config.getMaxPollInterval());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, m_config.getSessionTimeOut());
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, m_config.getHeartBeatInterval());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, m_config.getAutoOffsetReset());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        if (m_config.getSaslKerberosServiceName() != null) {
            props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, m_config.getSaslKerberosServiceName());
        }

        if (m_config.getSecurityProtocol() != null) {
            props.put("security.protocol", m_config.getSecurityProtocol());
        }

        // Query the topics and partitions outside the lock in order to properly
        // respond to a shutdown query in case we're querying a non-existent broker
        int kafkaPartitions = 0;
        KafkaInternalConsumerRunner theConsumer = null;
        try {
            theConsumer = createConsumerRunner(props);
            kafkaPartitions = theConsumer.getKafkaTopicPartitionCount();
        } catch (KafkaException ke) {
            LOGGER.error("Couldn't create Kafka consumer. Please check the configuration paramaters. Error:" + ke.getMessage());
        } catch (Throwable terminate) {
            LOGGER.error("Failed creating Kafka consumer ", terminate);
        }

        //paused or shutting down
        if (kafkaPartitions < 1) {
            return;
        }

        // While importers could be restarted upon catalog update, a cluster could be paused, triggering
        // stopping the importer @stop().
        // Thus sync the block to avoid any concurrent update.
        synchronized(m_lock) {
            int totalConsumerCount = kafkaPartitions;
            if (m_config.getConsumerCount() > 0) {
                totalConsumerCount = m_config.getConsumerCount();
            }
            int consumerCount = (int)Math.ceil((double)totalConsumerCount/m_config.getDBHostCount());
            m_executorService = Executors.newFixedThreadPool(consumerCount);
            m_consumers = new ArrayList<>();
            m_consumers.add(theConsumer);
            if (consumerCount > 1) {
                try {
                    for (int i = 1; i < consumerCount; i++) {
                        if (m_shutdown.get()) {
                            return;
                        }
                        m_consumers.add(createConsumerRunner(props));
                    }
                } catch (KafkaException ke) {
                    LOGGER.error("Couldn't create Kafka consumer. Please check the configuration paramaters. Error:" + ke.getMessage());
                } catch (Throwable terminate) {
                    LOGGER.error("Couldn't create Kafka consumer. ", terminate);
                }
            }

            if (m_consumers.size() != consumerCount) {
                for (KafkaInternalConsumerRunner consumer : m_consumers) {
                    consumer.shutdown();
                }
                m_consumers.clear();
            } else {
                for (KafkaInternalConsumerRunner consumer : m_consumers) {
                    if (m_shutdown.get()) {
                        return;
                    }
                    m_executorService.submit(consumer);
                }
            }
            LOGGER.info("Number of Kafka Consumers on this host:" + consumerCount);
        }

        // After the importer is initialized, insert records in @Statistics IMPORTER to make sure VMC can keep track of the import progress
        for (String topicName : m_config.getTopics().split("\\s*,\\s*")) {
            reportInitializedStat(m_config.getProcedure(topicName));
        }
    }

    @Override
    public void stop() {
        m_shutdown.set(true);
        synchronized(m_lock) {
            if (m_consumers != null) {
                for (KafkaInternalConsumerRunner consumer : m_consumers) {
                    if (consumer != null) {
                        consumer.shutdown();
                    }
                }
                m_consumers.clear();
            }

            if (m_executorService == null) {
                return;
            }

            //graceful shutdown to allow importers to properly process post shutdown tasks.
            m_executorService.shutdown();
            try {
                m_executorService.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException ignore) {
            } finally {
                m_executorService = null;
            }
        }
    }
}
