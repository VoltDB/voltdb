/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslConfigs;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;

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
 * props.put("partitioner.class", "org.voltdb.e3.topics.VoltDBKafkaPartitioner");
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for (int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }
 * </pre>
 * The <code>bootstrap.servers.voltdb</code> is required to calculate partition id from the key.
 */
public class VoltDBKafkaPartitioner extends DefaultPartitioner {

    /**
     * <p>Configuration for a VoltDB client for SSL enabled authentication, set it true if SSL is enabled.</p>
     */
    public static final String BOOTSTRAP_SERVERS_VOLTDB_SSL = "bootstrap.servers.voltdb.ssl";

   /**
     * <p>Configuration of SSL trust store location for SSL-enabled VoltDB authentication
     */
    public static final String SSL_TRUSTSTORE_LOCATION = SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;

    /**
     * <p>Configuration for a VoltDB client to connect to VoltDB cluster</p>
     */
    public static final String BOOTSTRAP_SERVERS_VOLTDB = "bootstrap.servers.voltdb";

    /**
     * <p>Configuration for a VoltDB client that specifies authentication credentials. The username can be null or the empty string.</p>
     */
    public static final String BOOTSTRAP_SERVERS_VOLTDB_USERNAME = "bootstrap.servers.voltdb.username";

    /**
     * <p>Configuration for a VoltDB client that specifies authentication credentials. The password can be null or the empty string.</p>
     */
    public static final String BOOTSTRAP_SERVERS_VOLTDB_PASSWORD = "bootstrap.servers.voltdb.password";

    static final Logger LOG = Logger.getLogger(VoltDBKafkaPartitioner.class.getName());
    protected ClientImpl m_client;
    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        m_client = (ClientImpl) ClientFactory.createClient(createClientConfig(configs));

        final String connectString = (String)configs.get(BOOTSTRAP_SERVERS_VOLTDB);
        String [] connections = connectString.split(";");
        for (String connection : connections) {
            try {
                m_client.createConnection(connection);
            } catch (IOException e) {
                throw new KafkaException("Failed to create connections to VoltDB cluster", e);
            }
        }
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        VoltType keyType = null;
        if (key != null) {
            try {
                keyType = VoltType.typeFromObject(key);
            } catch (VoltTypeException e) {
                // Types not supported in VoltDB
            }
        }
        int partition = -1;
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

    /**
     * <p> Create ClientConfig for client connection to VoltDB cluster </p>
     * @param configs Configuration properties from KafkaProducer
     * @return ClientConfig
     */
    protected ClientConfig createClientConfig(Map<String, ?> configs) {
        String userName = (String)configs.get(BOOTSTRAP_SERVERS_VOLTDB_USERNAME);
        String password = (String)configs.get(BOOTSTRAP_SERVERS_VOLTDB_PASSWORD);
        ClientConfig clientConfig = new ClientConfig(userName, password);
        clientConfig.setClientAffinity(true);
        clientConfig.setTopologyChangeAware(true);

        boolean sslEnabled = Boolean.parseBoolean((String) configs.get(BOOTSTRAP_SERVERS_VOLTDB_SSL));
        if (sslEnabled) {
            clientConfig.enableSSL();
            final String sslStore = (String)configs.get(SSL_TRUSTSTORE_LOCATION);
            if (!StringUtils.isEmpty(sslStore)) {
                clientConfig.setTrustStoreConfigFromPropertyFile(sslStore);
            }
        }

        return clientConfig;
    }
}
