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
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.voltcore.logging.VoltLogger;
import org.voltdb.importclient.kafka.util.KafkaConstants;
import org.voltdb.importclient.kafka.util.KafkaCommitPolicy;
import org.voltdb.importclient.kafka.util.KafkaUtils;
import org.voltdb.importer.ImportDataProcessor;
import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.FormatterBuilder;

/**
 * Holds configuration information required to connect a consumer to a topic.
 */
public class KafkaStreamImporterConfig implements ImporterConfig {

    private static final VoltLogger LOGGER = new VoltLogger("KAFKAIMPORTER");

    private final URI m_uri;
    private final String m_brokers;
    private final String m_topics;
    private String m_groupId;
    private final KafkaCommitPolicy m_commitPolicy;
    private final long m_triggerValue;
    private String m_brokerKey;
    private String m_autoOffsetReset = "earliest";
    private Map<String, String> m_procedureMap = new HashMap<>();
    private Map<String, FormatterBuilder> m_formatterBuilderMap = new HashMap<>();
    private int m_dbHostCount;

    /**
     * <code>m_consumerRequestTimeout</code> The configuration controls the maximum amount of time the client will wait
     * for the response of a request.
     * Configured via property <code>request.timeout.ms</code> Default:305000
     */
    private int m_consumerRequestTimeout;


    /**
     * <code>m_maxMessageFetchSize</code> The maximal bytes per batch.
     * Configured via property <code>fetch.max.bytes</code> Default:52428800
     */
    private int m_maxMessageFetchSize;

    /**
     * <code>m_maxPartitionFetchBytes</code> The maximum amount of data per-partition the server will return.
     * Configured via property <code>max.partition.fetch.bytes</code> Default:1048576
     */
    private int m_maxPartitionFetchBytes = ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES;

    /**
     * <code>m_maxPollRecords</code> The maximum number of records returned in a single fetch call
     * Configured via property <code>max.poll.records</code> Default:2000
     */
    private int m_maxPollRecords;

    /**
     * <code>m_sessionTimeOut</code> The consumer sends periodic heartbeats to indicate its liveness to the broker.
     * If the broker does not receive any heartbeats from a consumer before the expiration of this session timeout, the consumer will be removed.
     * Configured via property <code>session.timeout.ms</code> Default:20000
     */
    private int m_sessionTimeOut;

    /**
     * <code>m_heartBeatInterval</code> The expected time between heartbeats to the consumer coordinator.
     * The value must be set lower than <code>m_sessionTimeOut</code>, but typically should be set no higher than 1/3 of that value.
     * Configured via property <code>heartbeat.interval.ms</code> Default:3000
     */
    private int m_heartBeatInterval;

    /**
     * <code>m_maxPollInterval</code> The maximum delay between invocations of poll(). If poll() is not called before expiration of this timeout,
     * then the consumer is considered failed and the group will rebalance in order to reassign the partitions to another member.
     * Configured via property <code>max.poll.interval.ms</code> Default:300000
     */
    private int m_maxPollInterval;

    /**
     * <code>m_pollTimeout</code> The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
     * If 0, returns immediately with any records that are available currently in the buffer, else returns empty. Must not be negative.
     * Configured via property <code>poll.timeout.ms</code> Default: 500ms
     */
    private int m_pollTimeout;

    //The total number of consumers for the importer, which are distributed among the hosts.
    private int m_consumerCount;

    private String m_securityProtocol;
    private String m_saslKerberosServiceName;
    /**
     * Importer configuration constructor.
     * @param properties Properties read from the deployment XML.
     */
    @SuppressWarnings("unchecked")
    public KafkaStreamImporterConfig(Properties properties) {
        m_brokers = KafkaUtils.getBrokers(null,  properties.getProperty("brokers", null));
        m_brokerKey = KafkaUtils.getNormalizedKey(m_brokers);
        m_topics = properties.getProperty("topics");
        m_groupId = properties.getProperty("groupid", KafkaConstants.GROUP_ID);
        String commitPolicy = properties.getProperty("commit.policy");
        m_commitPolicy = KafkaCommitPolicy.fromString(commitPolicy);
        m_triggerValue = KafkaCommitPolicy.fromStringTriggerValue(commitPolicy, m_commitPolicy);
        m_securityProtocol = properties.getProperty("security.protocol", null);
        m_saslKerberosServiceName = properties.getProperty("sasl.kerberos.service.name", null);

        m_consumerRequestTimeout = parseProperty(properties, ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 305000);

        m_maxMessageFetchSize = parseProperty(properties, ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                                                ConsumerConfig.DEFAULT_FETCH_MAX_BYTES);

        m_maxPartitionFetchBytes = parseProperty(properties, ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                                                ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES);

        m_maxPollRecords = parseProperty(properties, ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2000);

        m_sessionTimeOut = parseProperty(properties, ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                                                (int)TimeUnit.SECONDS.toMillis(20));

        m_heartBeatInterval = parseProperty(properties, ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
                                                (int)TimeUnit.SECONDS.toMillis(3));

        if (m_heartBeatInterval >= (m_sessionTimeOut/3)) {
            throw new IllegalArgumentException("heartbeat interval should not be higher than 1/3 of the session timeout value");
        }

        //introduced in Kafka 0.10.1
        m_maxPollInterval = parseProperty(properties, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                                                (int)TimeUnit.SECONDS.toMillis(300));

        //the smaller, the quicker response from kafka, minimize the blocking time.
        m_pollTimeout = parseProperty(properties, ImportDataProcessor.POLL_TIMEOUT_MS, 100);

        m_procedureMap = (Map<String, String>) properties.get(ImportDataProcessor.KAFKA10_PROCEDURES);
        if (m_procedureMap == null) {
            m_procedureMap = new HashMap<String, String>();
            String procedure = properties.getProperty("procedure");
            if (procedure != null && !procedure.trim().isEmpty()) {
                m_procedureMap.put(m_topics, procedure.trim());
            }
        }
        m_formatterBuilderMap = (Map<String, FormatterBuilder>) properties.get(ImportDataProcessor.KAFKA10_FORMATTERS);
        if (m_formatterBuilderMap == null) {
            m_formatterBuilderMap = new HashMap<String, FormatterBuilder>();
        }

        String autoOffsetReset = properties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        if (autoOffsetReset != null && !autoOffsetReset.trim().isEmpty()) {
            m_autoOffsetReset = autoOffsetReset.trim();
        }

        m_consumerCount = parseProperty(properties, ImportDataProcessor.KAFKA10_CONSUMER_COUNT, 0);

        //host count, will not be missing in the property because is is explicitly set.
        //set default to 1 anyway.
        m_dbHostCount = parseProperty(properties, ImportDataProcessor.VOLTDB_HOST_COUNT, 1);
        validate(true);
        m_uri = createURI(m_brokers, m_topics, m_groupId);
        debug();
    }

    private int parseProperty(Properties props, String propName, int defaultValue) {
        String value = props.getProperty(propName);
        if (value != null && !value.trim().isEmpty()) {
            int parsedValue = Integer.parseInt(value.trim());
            if (parsedValue > 0) {
                return parsedValue;
            }
        }
        return defaultValue;
    }

    public KafkaStreamImporterConfig(KafkaLoaderCLIArguments args, FormatterBuilder formatterBuilder) {
        m_brokers = KafkaUtils.getBrokers(args.zookeeper, args.brokers);
        m_brokerKey = KafkaUtils.getNormalizedKey(m_brokers);
        m_topics = args.topic;
        m_groupId = args.groupid;
        m_commitPolicy = KafkaCommitPolicy.fromString(args.commitpolicy);
        m_triggerValue = KafkaCommitPolicy.fromStringTriggerValue(args.commitpolicy, m_commitPolicy);

        m_consumerRequestTimeout = args.timeout;
        m_maxMessageFetchSize = args.buffersize;
        m_maxPollRecords = args.maxpollrecords;
        m_pollTimeout = 100;
        m_maxPollInterval = args.maxpollinterval;
        m_sessionTimeOut = args.maxsessiontimeout;

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
        builder.append("Kafka importer configurations:\n" + "\tTopics:" + m_topics + "\n\tConsumer Timeout:" + m_consumerRequestTimeout);
        builder.append("\n\tMaxMessageFetchSize: " + m_maxMessageFetchSize);
        builder.append("\n\tMaxPartitionFetchBytes: " + m_maxPartitionFetchBytes);
        builder.append("\n\tMaxPollRecords: " + m_maxPollRecords);
        builder.append("\n\tAutoOffsetReset: " + m_autoOffsetReset);
        builder.append("\n\tSessionTimeOut: " + m_sessionTimeOut);
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
            if (topic.length() > KafkaConstants.TOPIC_MAX_NAME_LENGTH) {
                throw new IllegalArgumentException("topic name can't be longer than "
                        + KafkaConstants.TOPIC_MAX_NAME_LENGTH + " characters");
            }

            if (!KafkaConstants.TOPIC_LEGAL_NAMES_PATTERN.matcher(topic).matches()) {
                throw new IllegalArgumentException("topic name " + topic + " contains a character other than ASCII alphanumerics, '_' and '-'");
            }

            if (!m_procedureMap.containsKey(topic)) {
                throw new IllegalArgumentException("Missing procedure name");
            }
        }
    }

    private URI createURI(String brokers, String topics, String groupId) {
        try {
            return  new URI("kafka://" + m_brokerKey + "/" + KafkaUtils.getNormalizedKey(topics) + "/" + groupId);
        } catch (URISyntaxException e) {
            return null;
        }
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

    public int getConsumerRequestTimeout() {
        return m_consumerRequestTimeout;
    }

    public KafkaCommitPolicy getCommitPolicy() {
        return m_commitPolicy;
    }

    public long getTriggerValue() {
        return m_triggerValue;
    }

    public int getMaxMessageFetchSize() {
        return (int)m_maxMessageFetchSize;
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

    public int getSessionTimeOut() {
        return m_sessionTimeOut;
    }

    public int getHeartBeatInterval() {
        return m_heartBeatInterval;
    }

    @Override
    public FormatterBuilder getFormatterBuilder() {
        if (m_formatterBuilderMap.isEmpty()) {
            return null;
        }
        return m_formatterBuilderMap.values().iterator().next();
    }

    public int getMaxPollInterval() {
        return m_maxPollInterval;
    }

    public int getPollTimeout() {
        return m_pollTimeout;
    }

    public int getConsumerCount() {
        return m_consumerCount;
    }

    public int getDBHostCount() {
        return m_dbHostCount;
    }

    public String getProcedures() {
        return m_procedureMap.toString();
    }

    public String getSecurityProtocol() {
        return m_securityProtocol;
    }

    public String getSaslKerberosServiceName() {
        return m_saslKerberosServiceName;
    }
}
