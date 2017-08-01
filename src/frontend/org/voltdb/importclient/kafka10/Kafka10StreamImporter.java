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

package org.voltdb.importclient.kafka10;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.voltcore.logging.VoltLogger;
import org.voltdb.importer.AbstractImporter;

public class Kafka10StreamImporter extends AbstractImporter {

    private static final VoltLogger m_log = new VoltLogger("KAFKAIMPORTER");

    protected Kafka10StreamImporterConfig m_config;
    protected Kafka10ConsumerRunner m_runner;

    public Kafka10StreamImporter(Kafka10StreamImporterConfig config) {
        super();
        m_config = config;
    }

    @Override
    public String getName() {
       return Kafka10StreamImporter.class.getName();
    }

    @Override
    public URI getResourceID() {
        return m_config.getURI();
    }

    /**
     * Create a Kafka consumer and runner. Overrideable for unit tests
     *
     * @param properties Kafka consumer properties
     * @throws Exception on error
     */
    public Kafka10ConsumerRunner createConsumerRunner(Properties properties) throws Exception {

        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        try {
            Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
            return new Kafka10InternalConsumerRunner(this, m_config, consumer);
        }
        finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }


    @Override
    public void accept() {

        // Build up a Kafka Consumer properties from the supplied configuration.  Supported property values
        // are documented here: https://kafka.apache.org/0100/documentation.html#consumerconfigs

        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, m_config.getGroupId());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_config.getBrokers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, m_config.getMaxMessageFetchSize());
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, m_config.getConsumerTimeoutMillis());

        // Someday, we might want to support customers overriding the default assignor strategy. But not now.
        String consumerAssignorStrategy = org.apache.kafka.clients.consumer.RangeAssignor.class.getName();
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, consumerAssignorStrategy);

        try {
            m_runner = createConsumerRunner(props);
        }
        catch (Exception e) {
            // Error in creating the runner, give up.
            m_log.error("Exception creating consumer runner", e.getCause() == null ? e : e.getCause());
            return;
        }

        // Consumer is created, so start it up. Use a custom thread factory so we can put the topic name
        // in the thread name. This helps while debugging.

        ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "Kafka_Consumer_" + m_config.getTopics()); //NEEDSWORK: Replace commas, special characters if any
            }
        });

        try {
            executor.submit(m_runner);
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }
        catch (Throwable t) {
            m_log.warn(t);
        }
        finally {
            stop();
        }

    }

    @Override
    public void stop() {
        m_runner.shutdown();
    }

}