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

package org.voltdb.importclient.kinesis;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.voltcore.logging.VoltLogger;
import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.FormatterBuilder;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;

/**
 * ImporterConfig implementation for AWS Kinesis Stream importer.
 */
public class KinesisStreamImporterConfig implements ImporterConfig {

    public static final String APP_NAME = "KinesisStreamImporter";
    public static final String APP_VERSION = "1.0.0";

    private static VoltLogger LOGGER = new VoltLogger(KinesisStreamImporterConfig.APP_NAME);

    private final String m_appName;
    private final URI m_resourceID;
    private final String m_region;
    private final String m_streamName;
    private final String m_procedure;
    private final String m_secretKey;
    private final String m_accessKey;
    private final long m_idleTimeBetweenReadsInMillis;
    private final long m_maxReadBatchSize;
    private final long m_taskBackoffTimeMillis;
    private final FormatterBuilder m_formatterBuilder;

    /**
     * Kinesis stream importer configurations
     * @param appName applicationName Name of the Kinesis application
     * @param region TThe region name for the service
     * @param streamName  Name of the Kinesis stream
     * @param procedure The store procedure which will bring the data into VoltDB
     * @param secretKey The Kinesis stream secret key
     * @param accessKey The kinesis stream access key id
     * @param idleTimeBetweenReadsInMillis Idle time between calls to fetch data from Kinesis
     * @param maxReadBatchSize Max records to read per Kinesis get request
     * @param resourceId The URI per stream, per shard, per app
     * @param taskBackoffTimeMillis  Backoff period when tasks encounter an exception
     * @param formatterBuilder FormatterBuilder
     */
    private KinesisStreamImporterConfig(final String appName, final String region, final String streamName,
            final String procedure, final String secretKey, final String accessKey,
            final long idleTimeBetweenReadsInMillis, final long maxReadBatchSize, final URI resourceId,
            final long taskBackoffTimeMillis, final FormatterBuilder formatterBuilder) {

        m_appName = appName;
        m_region = region;
        m_streamName = streamName;
        m_procedure = procedure;
        m_secretKey = secretKey;
        m_accessKey = accessKey;
        m_idleTimeBetweenReadsInMillis = idleTimeBetweenReadsInMillis;
        m_maxReadBatchSize = maxReadBatchSize;
        m_resourceID = resourceId;
        m_taskBackoffTimeMillis = taskBackoffTimeMillis;
        m_formatterBuilder = formatterBuilder;
    }

    @Override
    public URI getResourceID() {
        return m_resourceID;
    }

    String getProcedure() {
        return m_procedure;
    }

    String getRegion() {
        return m_region;
    }

    String getStreamName() {
        return m_streamName;
    }

    public String getSecretKey() {
        return m_secretKey;
    }

    public String getAccessKey() {
        return m_accessKey;
    }

    public long getIdleTimeBetweenReads() {
        return m_idleTimeBetweenReadsInMillis;
    }

    public long getMaxReadBatchSize() {
        return m_maxReadBatchSize;
    }

    public String getAppName() {
        return m_appName;
    }

    public long getTaskBackoffTimeMillis() {
        return m_taskBackoffTimeMillis;
    }

    public static Map<URI, ImporterConfig> createConfigEntries(Properties props, FormatterBuilder formatterBuilder) {

        Map<URI, ImporterConfig> configs = new HashMap<>();

        String appName = getProperty(props, "app.name", "");
        String streamName = getProperty(props, "stream.name", "");
        String region = getProperty(props, "region", "");
        String procedure = getProperty(props, "procedure", "");
        String secretKey = getProperty(props, "secret.key", "");
        String accessKey = getProperty(props, "access.key", "");
        long readInterval = getPropertyAsLong(props, "idle.time.between.reads", 1000);
        long maxReadBatchSize = getPropertyAsLong(props, "max.read.batch.size", 10000);
        long taskBackoffTimeMillis = getPropertyAsLong(props, "task.backoff.time.millis", 500);

        List<Shard> shards = discoverShards(region, streamName, accessKey, secretKey, appName);
        if (shards == null || shards.isEmpty()) {
            LOGGER.warn(String.format("Kinesis stream %s or regions %s are not configured.", streamName, region));
            return configs;
        }

        // build URI per stream, per shard and per application
        int shardCnt = 0;
        for (Shard shard : shards) {

            StringBuilder builder = new StringBuilder(128);
            builder.append("kinesis://").append(region).append("/").append(streamName).append("/").append("shard-")
                    .append(shardCnt++).append("/").append(appName);

            URI uri = URI.create(builder.toString());

            ImporterConfig config = new KinesisStreamImporterConfig(appName, region, streamName, procedure, secretKey,
                    accessKey, readInterval, maxReadBatchSize, uri, taskBackoffTimeMillis, formatterBuilder);

            configs.put(uri, config);
        }

        return configs;
    }

    /**
     * connect to kinesis stream to discover the shards on the stream
     *
     * @param regionName The region name where the stream resides
     * @param streamName The kinesis stream name
     * @param accessKey The user access key
     * @param secretKey The user secret key
     * @param appName  The name of stream application
     * @return a list of shards
     */
    public static List<Shard> discoverShards(String regionName, String streamName, String accessKey, String secretKey,
            String appName) {
        try {
            Region region = RegionUtils.getRegion(regionName);
            if (region != null) {
                final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
                AmazonKinesis kinesisClient = new AmazonKinesisClient(credentials,
                        getClientConfigWithUserAgent(appName));
                kinesisClient.setRegion(region);

                DescribeStreamResult result = kinesisClient.describeStream(streamName);
                if (!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
                    throw new IllegalArgumentException("Kinesis stream " + streamName + " is not active.");
                }
                return result.getStreamDescription().getShards();
            }
        } catch (ResourceNotFoundException e) {
            LOGGER.warn("Kinesis stream " + streamName + " does not exist.", e);
        } catch (Exception e) {
            LOGGER.warn("Error found while describing the kinesis stream " + streamName, e);
        }
        return null;
    }

    /**
     * get property value. If no value is available, throw IllegalArgumentException
     * @param props  The properties
     * @param propertyName  property name
     * @param defaultValue  The default value
     * @return property value
     */
    public static String getProperty(Properties props, String propertyName, String defaultValue) {
        String value = props.getProperty(propertyName, defaultValue).trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException(
                    "Property " + propertyName + " is missing in Kinesis importer configuration.");
        }
        return value;
    }

    /**
     * get property value as long.
     * @param props  The properties
     * @param propertyName  property name
     * @param defaultValue  The default value
     * @return property value
     */
    public static long getPropertyAsLong(Properties props, String propertyName, long defaultValue) {
        String value = props.getProperty(propertyName, "").trim();
        if (value.isEmpty()) {
            return defaultValue;
        }
        try {
            long val = Long.parseLong(value);
            if (val <= 0) {
                throw new IllegalArgumentException(
                        "Value of " + propertyName + " should be positive, but current value is " + val);
            }
            return val;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Property " + propertyName + " must be a number in Kinesis importer configuration.");
        }
    }

    public static ClientConfiguration getClientConfigWithUserAgent(String appName) {

        final ClientConfiguration config = new ClientConfiguration();
        final StringBuilder userAgent = new StringBuilder(ClientConfiguration.DEFAULT_USER_AGENT);
        userAgent.append(" ").append(appName).append("/").append(APP_VERSION);
        config.setUserAgent(userAgent.toString());
        return config;
    }


    @Override
    public FormatterBuilder getFormatterBuilder()
    {
        return m_formatterBuilder;
    }
}
