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
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.voltcore.logging.VoltLogger;
import org.voltdb.importclient.kafka.util.BaseKafkaImporterConfig;
import org.voltdb.importclient.kafka.util.BaseKafkaLoaderCLIArguments;
import org.voltdb.importclient.kafka.util.HostAndPort;
import org.voltdb.importclient.kafka.util.KafkaImporterCommitPolicy;
import org.voltdb.importclient.kafka.util.KafkaImporterUtils;
import org.voltdb.importer.ImportDataProcessor;
import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.FormatterBuilder;

/**
 * Holds configuration information required to connect a consumer to a topic.
 */
public class Kafka10StreamImporterConfig extends BaseKafkaImporterConfig implements ImporterConfig {

    private static final VoltLogger LOGGER = new VoltLogger("KAFKAIMPORTER");

    private URI m_uri;
    private String m_brokers;
    private String m_topics;
    private String m_groupId;
    private int m_consumerTimeoutMillis;
    private final KafkaImporterCommitPolicy m_commitPolicy;
    private final long m_triggerValue;

    private int m_maxMessageFetchSize = ConsumerConfig.DEFAULT_FETCH_MAX_BYTES;
    private String m_brokerKey;
    private int m_maxPartitionFetchBytes = ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES;
    private int m_maxPollRecords = -1;
    private String m_autoOffsetReset = "earliest";
    private long m_retryBackOff = -1L;
    private long m_sessionTimeOut = -1L;

    private Map<String, String> m_procedureMap = new HashMap<String, String>();
    private Map<String, FormatterBuilder> m_formatterBuilderMap = new HashMap<String, FormatterBuilder>();

    /**
     * Importer configuration constructor.
     * @param properties Properties read from the deployment XML.
     * @param formatterBuilder FormatterBuilder for this importer configuration
     */
    @SuppressWarnings("unchecked")
    public Kafka10StreamImporterConfig(Properties properties, FormatterBuilder formatterBuilder) {
        initializeBrokerConfig(null, properties.getProperty("brokers", null));
        m_topics = properties.getProperty("topics");
        m_groupId = properties.getProperty("groupid");
        String commitPolicy = properties.getProperty("commit.policy");
        m_commitPolicy = KafkaImporterCommitPolicy.fromString(commitPolicy);
        m_triggerValue = KafkaImporterCommitPolicy.fromStringTriggerValue(commitPolicy, m_commitPolicy);

        m_consumerTimeoutMillis = Integer.parseInt(properties.getProperty("socket.timeout.ms", "30000"));

        String maxMessageFetchSize = properties.getProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG);
        if (maxMessageFetchSize != null && !maxMessageFetchSize.trim().isEmpty()) {
            m_maxMessageFetchSize = Integer.parseInt(maxMessageFetchSize);
        }

        String maxPartitionFetchBytes = properties.getProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
        if (maxPartitionFetchBytes != null && !maxPartitionFetchBytes.trim().isEmpty()) {
            m_maxPartitionFetchBytes = Integer.parseInt(maxPartitionFetchBytes);
        }
        String maxPollRecords = properties.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        if (maxPollRecords != null && !maxPollRecords.trim().isEmpty()) {
            m_maxPollRecords= Integer.parseInt(maxPollRecords);
        }

        String autoOffsetReset = properties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        if (autoOffsetReset != null && !autoOffsetReset.trim().isEmpty()) {
            m_autoOffsetReset = autoOffsetReset.trim();
        }

        String retryBackOff = properties.getProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        if (retryBackOff != null && !retryBackOff.trim().isEmpty()) {
            m_retryBackOff = Long.parseLong(retryBackOff);
        }

        String sessionTimeOut = properties.getProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        if (sessionTimeOut != null && !sessionTimeOut.trim().isEmpty()) {
            m_sessionTimeOut = Long.parseLong(sessionTimeOut);
        }

        m_procedureMap = (Map<String, String>) properties.get(ImportDataProcessor.IMPORTER_KAFKA_PROCEDURES);
        m_formatterBuilderMap = (Map<String, FormatterBuilder>) properties.get(ImportDataProcessor.IMPORTER_KAFKA_FORMATTERS);

        validate(true);
        m_uri = createURI(m_brokers, m_topics, m_groupId);
        debug();
    }

    public Kafka10StreamImporterConfig(Kafka10LoaderCLIArguments args, FormatterBuilder formatterBuilder) {
        initializeBrokerConfig(args.zookeeper, args.brokers);
        m_topics = args.topic;
        m_groupId = args.groupid;
        m_commitPolicy = KafkaImporterCommitPolicy.fromString(args.commitpolicy);
        m_triggerValue = KafkaImporterCommitPolicy.fromStringTriggerValue(args.commitpolicy, m_commitPolicy);

        m_consumerTimeoutMillis = args.timeout;
        m_maxMessageFetchSize = args.buffersize;
        if (formatterBuilder != null) {
            m_formatterBuilderMap.put(m_topics, formatterBuilder);
        }
        m_procedureMap.put(m_topics, args.procedure);
        validate(false);
        m_uri = createURI(m_brokers, m_topics, m_groupId);
        debug();

    }

    private void debug() {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }

        StringBuilder builder = new StringBuilder();
        builder.append("Kafka importer configurations:\n" + "\tTopics:" + m_topics + "\n\tConsumer Timeout:" + m_consumerTimeoutMillis);
        builder.append("\n\tMaxMessageFetchSize: " + m_maxMessageFetchSize);
        builder.append("\n\tMaxPartitionFetchBytes: " + m_maxPartitionFetchBytes);
        if (m_maxPollRecords > 0) {
            builder.append("\n\tMaxPollRecords: " + m_maxPollRecords);
        }
        builder.append("\n\tAutoOffsetReset: " + m_autoOffsetReset);

        if (m_retryBackOff > 0) {
        builder.append("\n\tRetryBackOff: " + m_retryBackOff);
        }

        if (m_sessionTimeOut > 0) {
            builder.append("\n\tSessionTimeOut: " + m_sessionTimeOut);
        }
        builder.append("\n\tURI: " + m_uri);
        builder.append("\n\tProcedures: " + m_procedureMap);
        builder.append("\n\tFormatterBuilder: " + m_formatterBuilderMap);
        LOGGER.debug(builder.toString());
    }

    private void validate(boolean forImporter) {

        if (m_topics == null || m_topics.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing topic(s).");
        }

        List<String> topicList = Arrays.asList(m_topics.split("\\s*,\\s*"));
        if (topicList == null || topicList.isEmpty()) {
            throw new IllegalArgumentException("Missing topic(s).");
        }

        Set<String> topicSet = new HashSet<String>();
        topicSet.addAll(topicList);
        if (topicSet.size() != topicList.size()) {
            throw new IllegalArgumentException("Dupliacted topics " + topicList + " for brokers " + m_brokers);
        }

        for (String topic : topicList) {
            if (topic.length() > TOPIC_MAX_NAME_LENGTH) {
                throw new IllegalArgumentException("topic name can't be longer than "
                        + TOPIC_MAX_NAME_LENGTH + " characters");
            }

            if (!TOPIC_LEGAL_NAMES_PATTERN.matcher(topic).matches()) {
                throw new IllegalArgumentException("topic name " + topic + " contains a character other than ASCII alphanumerics, '_' and '-'");
            }

            if (!m_procedureMap.containsKey(topic)) {
                throw new IllegalArgumentException("Missing procedure for topic " + topic);
            }

            if (forImporter && !m_formatterBuilderMap.containsKey(topic)) {
                throw new IllegalArgumentException("Missing formatter for topic " + topic);
            }
        }
    }

    private URI createURI(String brokers, String topics, String groupId) {
        try {
            return  new URI("kafka://" + m_brokerKey + "/" + KafkaImporterUtils.getNormalizedKey(topics) + "/" + groupId);
        } catch (URISyntaxException e) {
            return null;
        }
    }

    private void initializeBrokerConfig(String zookeeper, String brokers) {

        List<HostAndPort> brokerList;
        String brokerListString = null;

        try {
            if (zookeeper != null && !zookeeper.trim().isEmpty()) {
                brokerList = KafkaImporterUtils.getBrokersFromZookeeper(zookeeper, BaseKafkaLoaderCLIArguments.ZK_CONNECTION_TIMEOUT_MILLIS);
                brokerListString = StringUtils.join(brokerList.stream().map(s -> s.getHost() + ":" + s.getPort()).collect(Collectors.toList()), ",");
            } else {
                if (brokers == null || brokers.isEmpty()) {
                    throw new IllegalArgumentException("Kafka broker configuration is missing.");
                }
                brokerListString = brokers.trim();
                brokerList = Arrays.stream(brokerListString.split(",")).map(s -> HostAndPort.fromString(s)).collect(Collectors.toList());
            }
        } catch (Exception e) {
            brokerListString = brokers;
        }

        if (brokerListString == null || brokerListString.isEmpty()) {
            throw new IllegalArgumentException("Kafka broker configuration is missing.");
        }

        m_brokers = brokerListString;
        m_brokerKey = KafkaImporterUtils.getNormalizedKey(brokerListString);
    }

    public URI getURI() {
        return m_uri;
    }

    public String getBrokers() {
        return m_brokers;
    }

    public String getTopics() {
        return m_topics;
    }

    public String getGroupId() {
        return m_groupId;
    }

    public String getProcedure(String topic) {
        return m_procedureMap.get(topic);
    }

    public FormatterBuilder getFormatterBuilder(String topic) {
        return m_formatterBuilderMap.get(topic);
    }

    public int getConsumerTimeoutMillis() {
        return m_consumerTimeoutMillis;
    }

    public KafkaImporterCommitPolicy getCommitPolicy() {
        return m_commitPolicy;
    }

    public long getTriggerValue() {
        return m_triggerValue;
    }

    public int getMaxMessageFetchSize() {
        return m_maxMessageFetchSize;
    }

    @Override
    public URI getResourceID() {
        return m_uri;
    }

    public int getMaxPartitionFetchBytes() {
        return m_maxPartitionFetchBytes;
    }

    public int getMaxPollRecords() {
        return m_maxPollRecords;
    }

    public String getAutoOffsetReset() {
        return m_autoOffsetReset;
    }

    public long getRetyBackOff() {
        return m_retryBackOff;
    }

    public long getSessionTimeOut() {
        return m_sessionTimeOut;
    }

    @Override
    public FormatterBuilder getFormatterBuilder() {
        if (m_formatterBuilderMap.isEmpty()) {
            return null;
        }
        return m_formatterBuilderMap.values().iterator().next();
    }
}