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
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.importclient.kafka.util.BaseKafkaImporterConfig;
import org.voltdb.importclient.kafka.util.BaseKafkaLoaderCLIArguments;
import org.voltdb.importclient.kafka.util.HostAndPort;
import org.voltdb.importclient.kafka.util.KafkaImporterUtils;
import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.FormatterBuilder;

/**
 * Holds configuration information required to connect a consumer to a topic.
 */
public class Kafka10StreamImporterConfig extends BaseKafkaImporterConfig implements ImporterConfig
{
    private URI m_uri;
    private String m_brokers;
    private String m_topics;
    private String m_groupId;
    private String m_procedure;
    private FormatterBuilder m_formatterBuilder;
    private int m_consumerTimeoutMillis;
    private String m_commitPolicy = null;
    private int m_maxMessageFetchSize;
    private String m_brokerKey;

    /**
     * Importer configuration constructor.
     *
     * @param properties Properties read from the deployment XML.
     * @param formatterBuilder FormatterBuilder for this importer configuration
     */
    public Kafka10StreamImporterConfig(Properties properties, FormatterBuilder formatterBuilder) {
        m_formatterBuilder = formatterBuilder;
        initializeBrokerConfig(null, properties.getProperty("brokers", null));
        m_topics = properties.getProperty("topics");
        m_groupId = properties.getProperty("groupid");
        m_procedure = properties.getProperty("procedure");
        m_commitPolicy = properties.getProperty("commit.policy");
        m_consumerTimeoutMillis = Integer.parseInt(properties.getProperty("socket.timeout.ms", "30000"));
        m_maxMessageFetchSize =  Integer.parseInt(properties.getProperty("fetch.message.max.bytes", "65536"));

        validate();
        m_uri = createURI(m_brokers, m_topics, m_groupId);
    }

    public Kafka10StreamImporterConfig(Kafka10LoaderCLIArguments args, FormatterBuilder formatterBuilder) {
        m_formatterBuilder = formatterBuilder;
        initializeBrokerConfig(args.zookeeper, args.brokers);
        m_topics = args.topic;
        m_groupId = args.groupid;
        m_procedure = args.procedure;
        m_commitPolicy = args.commitpolicy;
        m_consumerTimeoutMillis = args.timeout;
        m_maxMessageFetchSize = args.buffersize;

        validate();
        m_uri = createURI(m_brokers, m_topics, m_groupId);

    }

    private void validate() {

        if (m_topics == null || m_topics.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing topic(s).");
        }
        if (m_procedure == null || m_procedure.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing procedure name.");
        }

        List<String> topicList = Arrays.asList(m_topics.split("\\s*,\\s*"));
        if (topicList == null || topicList.isEmpty()) {
            throw new IllegalArgumentException("Missing topic(s).");
        }
        for (String topic : topicList) {
            if (topic.length() > TOPIC_MAX_NAME_LENGTH) {
                throw new IllegalArgumentException("topic name is illegal, can't be longer than "
                        + TOPIC_MAX_NAME_LENGTH + " characters");
            }
            if (!TOPIC_LEGAL_NAMES_PATTERN.matcher(topic).matches()) {
                throw new IllegalArgumentException("topic name " + topic + " is illegal, contains a character other than ASCII alphanumerics, '_' and '-'");
            }
        }
    }

    private URI createURI(String brokers, String topics, String groupId) {
        try {
            return  new URI("kafka://" + m_brokerKey + "/" + KafkaImporterUtils.getNormalizedKey(topics) + "/" + groupId);
        }
        catch (URISyntaxException e) {
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
            }
            else {

                if (brokers == null || brokers.isEmpty()) {
                    throw new IllegalArgumentException("Kafka broker configuration is missing.");
                }
                brokerListString = brokers.trim();
                brokerList = Arrays.stream(brokerListString.split(",")).map(s -> HostAndPort.fromString(s)).collect(Collectors.toList());
            }
        }
        catch (Exception e) {
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

    public String getProcedure() {
        return m_procedure;
    }

    public int getConsumerTimeoutMillis() {
        return m_consumerTimeoutMillis;
    }

    public String getCommitPolicy() {
        return m_commitPolicy;
    }

    public int getMaxMessageFetchSize() {
        return m_maxMessageFetchSize;
    }

    @Override
    public URI getResourceID() {
        return m_uri;
    }

    @Override
    public FormatterBuilder getFormatterBuilder() {
        return m_formatterBuilder;
    }
}