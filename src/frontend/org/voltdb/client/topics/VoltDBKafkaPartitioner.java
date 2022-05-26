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
package org.voltdb.client.topics;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.net.HostAndPort;

/**
 * VoltDBKafkaPartitioner is a partitioner to calculate VoltDB partition id from the key value.
 * Here is a simple example of using the producer to send records with VoltDBKafkaPartitioner
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("bootstrap.servers.voltdb", "localhost:21212");
 * props.put("acks", "all");
 * props.put("retries", 0);
 * props.put("batch.size", 16384);
 * props.put("linger.ms", 1);
 * props.put("buffer.memory", 33554432);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VoltDBKafkaPartitioner.class.getName());
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for (int i = 0; i < 100; i++) {
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 * }
 * producer.close();
 * }
 * </pre>
 * The <code>bootstrap.servers.voltdb</code> is required to calculate partition id from the key.
 * <p>
 * Kafka client properties used by the partitioner
 * <ul>
 * <li>{@link ProducerConfig#BOOTSTRAP_SERVERS_CONFIG} - If {@code bootstrap.servers.voltdb} is not set
 * <li>{@link CommonClientConfigs#SECURITY_PROTOCOL_CONFIG}
 * <li>{@link SaslConfigs#SASL_MECHANISM} - Must be {@code PLAIN}
 * <li>{@link SaslConfigs#SASL_JAAS_CONFIG}
 * <li>{@link SslConfigs#SSL_TRUSTSTORE_LOCATION_CONFIG}
 * <li>{@link SslConfigs#SSL_TRUSTSTORE_PASSWORD_CONFIG}
 * </ul>
 */
public class VoltDBKafkaPartitioner extends DefaultPartitioner {
    /**
     * <p>Configuration for a VoltDB client to connect to VoltDB cluster: comma separated list of the form server[:port]</p>
     */
    public static final String BOOTSTRAP_SERVERS_VOLTDB = "bootstrap.servers.voltdb";

    private static final String PLAIN_SASL_MECHANISM = "PLAIN";

    static final Logger LOG = Logger.getLogger(VoltDBKafkaPartitioner.class.getName());
    protected ClientImpl m_client;
    private ImmutableMap<String, Boolean> m_topics = ImmutableMap.of();
    @Override
    public void configure(Map<String, ?> original) {
        PartitionConfig configs = new PartitionConfig(original);
        m_client = (ClientImpl) ClientFactory.createClient(createClientConfig(configs));

        List<String> urls = configs.getList(BOOTSTRAP_SERVERS_VOLTDB);
        boolean useDefault = false;
        if (urls.isEmpty()) {
            urls = configs.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
            useDefault = true;
        }

        boolean connected = false;
        for (String connection : urls) {
            if (useDefault) {
                HostAndPort url = HostAndPort.fromString(connection);
                connection = HostAndPort.fromParts(url.getHost(), Client.VOLTDB_SERVER_PORT).toString();
            }
            try {
                m_client.createConnection(connection);
                connected = true;
            } catch (IOException e) {
                String fmt = "Failed to open connection to VoltDB host at %s: %s";
                LOG.warning(String.format(fmt, connection, e.getMessage()));
            }
        }
        if (!connected) {
            throw new KafkaException(String.format("Failed to connect to any VoltDB host in %s", urls));
        }
    }

    /**
     *  Use DefaultPartitioner for opaque topics, otherwise use VoltDB hash mechanism for partition calculation.
     *  Under the following corner cases that the partitioner could direct a producer to wrong partitions:
     *  <ul>
     *  <li>When opaque topics are dropped, then recreated as non-opaque topics, or vice versa
     *  <li>When a VoltDB cluster is elastically expanded or reduced, the hash mechanism on VoltDB client is not promptly updated.
     * </ul>
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // Lazy loading topics list
        Boolean entry = m_topics.get(topic);
        if (entry == null) {
            loadTopics();
            entry = m_topics.get(topic);
            // There could be a window within which a topic is created and immediately dropped so that the topic
            // is not found. Or the topic is not created.
            if (entry == null) {
                throw new KafkaException(String.format("Topic %s is not found.", topic));
            }
        }

        // Use DefaultPartitioner for opaque topics
        if (entry.booleanValue()) {
            return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
        }

        int partition = -1;
        VoltType keyType = null;
        if (key != null) {
            try {
                keyType = VoltType.typeFromObject(key);
            } catch (VoltTypeException e) {
                // Types not supported in VoltDB
            }
        }
        if (keyType != null) { // VoltDB supported types
            partition = (int) m_client.getPartitionForParameter(keyType.getValue(), key);
        } else if (keyBytes != null){ // For any other types, use key value hash to get partition
            partition = (int) m_client.getPartitionForParameter(keyBytes);
        }
        return (partition > -1 ? partition : super.partition(topic, key, keyBytes, value, valueBytes, cluster));
    }

    @Override
    public void close() {
        super.close();
        if (m_client != null) {
            try {
                m_client.close();
            } catch (Exception e) {
                LOG.warning("Failed to close connections:" + e.getMessage());
            }
        }
    }

    protected void loadTopics() {
        try {
            VoltTable topics = m_client.callProcedure("@SystemCatalog", "TOPICS").getResults()[0];
            Map<String, Boolean> topicsMap = Maps.newHashMap();
            while (topics.advanceRow()) {
                topicsMap.put(topics.getString("TOPIC_NAME"), Boolean.parseBoolean(topics.getString("IS_OPAQUE")));
            }
            m_topics = ImmutableMap.copyOf(topicsMap);
        } catch (Exception e) {
            throw new KafkaException("Failed to get topics from VoltDB cluster", e);
        }
    }

    /**
     * <p> Create ClientConfig for client connection to VoltDB cluster </p>
     * @param configs Configuration properties from KafkaProducer
     * @return ClientConfig
     */
    protected ClientConfig createClientConfig(PartitionConfig configs) {
        SecurityProtocol protocol = SecurityProtocol
                .forName(configs.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));

        String userName = null;
        String password = null;

        if (protocol == SecurityProtocol.SASL_PLAINTEXT || protocol == SecurityProtocol.SASL_SSL) {
            if (!PLAIN_SASL_MECHANISM.equals(configs.getString(SaslConfigs.SASL_MECHANISM))) {
                throw new IllegalArgumentException(
                        "Only " + PLAIN_SASL_MECHANISM + " is supported for " + SaslConfigs.SASL_MECHANISM);
            }
            Password jaasConfigString = configs.getPassword(SaslConfigs.SASL_JAAS_CONFIG);
            if (jaasConfigString == null) {
                throw new IllegalArgumentException("SASL JAAS configuration not supplied when SASL was specified");
            }

            JaasContext context = JaasContext.loadClientContext(configs.values());
            String moduleName = PlainLoginModule.class.getName();
            for (AppConfigurationEntry ace : context.configurationEntries()) {
                if (moduleName.equals(ace.getLoginModuleName())) {
                    Map<String, ?> options = ace.getOptions();
                    userName = (String) options.get("username");
                    password = (String) options.get("password");
                    break;
                }
            }
        }

        ClientConfig clientConfig = new ClientConfig(userName, password);
        clientConfig.setTopologyChangeAware(true);

        boolean sslEnabled = protocol == SecurityProtocol.SSL || protocol == SecurityProtocol.SASL_SSL;
        if (sslEnabled) {
            clientConfig.enableSSL();
            final String sslStore = configs.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
            if (!StringUtils.isEmpty(sslStore)) {
                Password storePassword = configs.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
                clientConfig.setTrustStore(sslStore, storePassword == null ? null : storePassword.value());
            }
        }

        return clientConfig;
    }

    private static final class PartitionConfig extends AbstractConfig {
        private static final ConfigDef s_config = ProducerConfig.configDef().define(BOOTSTRAP_SERVERS_VOLTDB,
                ConfigDef.Type.LIST, Collections.emptyList(), new ConfigDef.NonNullValidator(), Importance.MEDIUM,
                "List of VoltDB servers to connect to. Defaults to " + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
                        + " using the deafult port  of " + Client.VOLTDB_SERVER_PORT);

        public PartitionConfig(Map<?, ?> originals) {
            super(s_config, originals, false);
        }
    }
}
