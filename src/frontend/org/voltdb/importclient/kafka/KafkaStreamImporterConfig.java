/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import static java.util.Collections.singletonList;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.voltdb.importclient.ImportBaseException;
import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.FormatterBuilder;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

/**
 * Holds configuration information required to connect to a single partition for a topic.
 */
public class KafkaStreamImporterConfig implements ImporterConfig
{
    private static final Logger m_logger = Logger.getLogger("IMPORT");

    public static final String CLIENT_ID = "voltdb-importer";
    private static final String GROUP_ID = "voltdb";
    private static final int KAFKA_DEFAULT_BROKER_PORT = 9092;

    // We don't allow period in topic names because we construct URIs using it
    private static final Pattern legalTopicNamesPattern = Pattern.compile("[a-zA-Z0-9\\_-]+");
    private static final int topicMaxNameLength = 255;

    private final URI m_uri;
    private final List<HostAndPort> m_brokers;
    private final String m_topic;
    private final String m_groupId;
    private final int m_fetchSize;
    private final int m_soTimeout;
    private final String m_procedure;
    private final int m_partition;
    private HostAndPort m_partitionLeader;
    private final FormatterBuilder m_formatterBuilder;
    private final KafkaImporterCommitPolicy m_commitPolicy;
    private final long m_triggerValue;

    private KafkaStreamImporterConfig(URI uri, List<HostAndPort> brokers, String topic, int partition, HostAndPort partitionLeader,
            String groupId, int fetchSize, int soTimeout, String procedure, String commitPolicy,
            FormatterBuilder formatterBuilder)
    {
        m_uri = uri;
        m_brokers = brokers;
        m_topic = topic;
        m_partition = partition;
        m_partitionLeader = partitionLeader;
        m_groupId = groupId;
        m_fetchSize = fetchSize;
        m_soTimeout = soTimeout;
        m_procedure = procedure;
        KafkaImporterCommitPolicy cp;
        m_commitPolicy = KafkaImporterCommitPolicy.fromString(commitPolicy);
        m_triggerValue = KafkaImporterCommitPolicy.fromStringTriggerValue(commitPolicy, m_commitPolicy);

        m_formatterBuilder = formatterBuilder;
    }


    public List<HostAndPort> getBrokers()
    {
        return m_brokers;
    }


    public String getTopic()
    {
        return m_topic;
    }


    public String getGroupId()
    {
        return m_groupId;
    }


    public int getFetchSize()
    {
        return m_fetchSize;
    }


    public int getSocketTimeout()
    {
        return m_soTimeout;
    }


    public String getProcedure()
    {
        return m_procedure;
    }


    public int getPartition()
    {
        return m_partition;
    }


    public HostAndPort getPartitionLeader()
    {
        return m_partitionLeader;
    }

    public void setPartitionLeader(HostAndPort leader)
    {
        m_partitionLeader = leader;
    }

    @Override
    public URI getResourceID()
    {
        return m_uri;
    }

    public KafkaImporterCommitPolicy getCommitPolicy() {
        return m_commitPolicy;
    }

    public long getTriggerValue() {
        return m_triggerValue;
    }

    public static Map<URI, ImporterConfig> createConfigEntries(Properties props,  FormatterBuilder formatterBuilder)
    {
        String brokers = props.getProperty("brokers", "").trim();
        if (brokers.isEmpty()) {
            throw new IllegalArgumentException("Missing kafka broker");
        }
        String key = getBrokerKey(brokers);
        List<String> brokerList = Arrays.asList(brokers.split("\\s*,\\s*"));
        if (brokerList == null || brokerList.isEmpty()) {
            throw new IllegalArgumentException("Missing kafka broker");
        }
        List<HostAndPort> hapList = new ArrayList<HostAndPort>();
        for (String broker : brokerList) {
            HostAndPort hap = HostAndPort.fromString(broker);
            hapList.add(hap);
        }
        if (hapList.isEmpty()) {
            throw new IllegalArgumentException("Missing or misconfigured kafka broker list. See brokers property");
        }

        String procedure = props.getProperty("procedure", "").trim();
        if (procedure.isEmpty()) {
            throw new IllegalArgumentException("Missing procedure.");
        }

        //comma separated list of topics.
        String topics = props.getProperty("topics", "").trim();
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("Missing topic(s).");
        }

        String groupId = props.getProperty("groupid", GROUP_ID).trim();
        //These are defaults picked up from kafka we save them so that they are passed around.
        int fetchSize = Integer.parseInt(props.getProperty("fetch.message.max.bytes", "65536"));
        int soTimeout = Integer.parseInt(props.getProperty("socket.timeout.ms", "30000"));

        List<String> ttopicList = Arrays.asList(topics.split("\\s*,\\s*"));
        if (ttopicList == null || ttopicList.isEmpty()) {
            throw new IllegalArgumentException("Missing topic(s).");
        }
        String commitPolicy = props.getProperty("commit.policy", "none");
        Map<URI, ImporterConfig> configs = new HashMap<>();
        for (String topic : ttopicList) {
            if (topic.length() > topicMaxNameLength) {
                throw new IllegalArgumentException("topic name is illegal, can't be longer than "
                        + topicMaxNameLength + " characters");
            }
            if (!legalTopicNamesPattern.matcher(topic).matches()) {
                throw new IllegalArgumentException("topic name " + topic + " is illegal, contains a character other than ASCII alphanumerics, '_' and '-'");
            }
            try {
                configs.putAll(getConfigsForPartitions(key, hapList, topic, groupId, procedure, soTimeout, fetchSize, commitPolicy, formatterBuilder));
            } catch(Exception e) {
                m_logger.warn(String.format("Error trying to get partition information for topic [%s] on host [%s]", topic, hapList.get(0).getHost()), e);
            }
        }

        return configs;
    }

    private final static class FailedMetaDataAttempt {
        final String msg;
        final Throwable cause;

        FailedMetaDataAttempt(String msg, Throwable cause) {
            this.cause = cause;
            this.msg = msg;
        }

        private void log() {
            m_logger.error(msg,cause);
        }
    }

    private static Map<URI, KafkaStreamImporterConfig> getConfigsForPartitions(String key, List<HostAndPort> brokerList,
            final String topic, String groupId, String procedure, int soTimeout, int fetchSize, String commitPolicy, FormatterBuilder formatterBuilder)
    {
        SimpleConsumer consumer = null;
        Map<URI, KafkaStreamImporterConfig> configs = new HashMap<>();
        List<FailedMetaDataAttempt> attempts = new ArrayList<>();

        Iterator<HostAndPort> hpitr = brokerList.iterator();
        while (configs.isEmpty() && hpitr.hasNext()) {
            HostAndPort hp = hpitr.next();
            try {
                consumer = new SimpleConsumer(hp.getHost(), hp.getPort(), soTimeout, fetchSize, CLIENT_ID);

                TopicMetadataRequest req = new TopicMetadataRequest(singletonList(topic));
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                if (metaData == null) {
                    attempts.add(new FailedMetaDataAttempt(
                            "Failed to get topic metadata for topic " + topic + " from host " + hp.getHost(), null
                            ));
                    continue;
                }
                int partitionCount = 0;
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        ++partitionCount;
                        URI uri;
                        try {
                            uri = new URI("kafka", key, topic + "/partition/" + part.partitionId());
                        } catch (URISyntaxException ex) { // Should not happen
                            throw new KafkaConfigurationException("unable to create topic resource URI", ex);
                        }
                        Broker leader = part.leader();
                        if (leader == null) {
                            attempts.add(new FailedMetaDataAttempt(
                                    "Failed to get leader broker for topic " + topic
                                    + " partition " + part.partitionId() + " from host " + hp.getHost(), null
                                    ));
                            continue;
                        }
                        KafkaStreamImporterConfig config = new KafkaStreamImporterConfig(uri, brokerList, topic,
                                part.partitionId(), new HostAndPort(leader.host(), leader.port()),
                                groupId, fetchSize, soTimeout, procedure, commitPolicy, formatterBuilder);
                        configs.put(uri, config);
                    }
                }
                if (configs.size() != partitionCount) {
                    configs.clear();
                    continue;
                }
            } catch (Exception e) {
                attempts.add(new FailedMetaDataAttempt(
                        "Failed to send topic metadata request for topic " + topic + " from host " + hp.getHost(), e
                        ));
                continue;
            } finally {
                closeConsumer(consumer);
            }
        }
        if (!attempts.isEmpty()) {
            for (FailedMetaDataAttempt attempt: attempts) {
                attempt.log();
            }
            attempts.clear();
            if (configs.isEmpty()) {
                throw new KafkaConfigurationException("Failed to get topic metadata for %s", topic);
            }
        }
        return configs;
    }

    private static String getBrokerKey(String brokers)
    {
        String key = brokers.replace(':', '_');
        key = key.replace(',', '_');
        return key.toLowerCase();
    }

    public static void closeConsumer(SimpleConsumer consumer) {
        if (consumer != null) try {
            consumer.close();
        } catch (Exception e) {
            m_logger.warn("Failed to close consumer connection.", e);
        }
    }

    //Simple Host and Port abstraction....dont want to use our big stuff here osgi bundle import nastiness.
    public static class HostAndPort {

        private final String m_host;
        private final int m_port;
        private final String m_connectionString;

        public HostAndPort(String h, int p) {
            m_host = h;
            m_port = p;
            m_connectionString = m_host + ":" + m_port;
        }

        public static HostAndPort fromString(String hap) {
            String s[] = hap.split(":");
            int p = KAFKA_DEFAULT_BROKER_PORT;
            if (s.length > 1 && s[1] != null && s[1].length() > 0) {
                p = Integer.parseInt(s[1].trim());
            }
            return new HostAndPort(s[0].trim(), p);
        }

        public String getHost() {
            return m_host;
        }

        public int getPort() {
            return m_port;
        }

        @Override
        public String toString() {
            return m_connectionString;
        }

        @Override
        public int hashCode() {
            return m_connectionString.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof HostAndPort)) {
                return false;
            }
            if (this.getClass() != o.getClass()) {
                return false;
            }
            HostAndPort hap = (HostAndPort )o;
            if (hap == this) {
                return true;
            }
            return (hap.getHost().equals(getHost()) && hap.getPort() == getPort());
        }
    }

    public static class KafkaConfigurationException extends ImportBaseException {

        private static final long serialVersionUID = -3413349105074207334L;

        public KafkaConfigurationException() {
            super();
        }

        public KafkaConfigurationException(String format, Object... args) {
            super(format, args);
        }

        public KafkaConfigurationException(String format, Throwable cause,
                Object... args) {
            super(format, cause, args);
        }

        public KafkaConfigurationException(Throwable cause) {
            super(cause);
        }
    }

    @Override
    public FormatterBuilder getFormatterBuilder()
    {
        return m_formatterBuilder;
    }
}
