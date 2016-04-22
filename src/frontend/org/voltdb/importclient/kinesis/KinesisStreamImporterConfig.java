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

package org.voltdb.importclient.kinesis;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.AbstractFormatterFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;

/**
 * ImporterConfig implementation for AWS Kinesis Stream importer. 
 */
public class KinesisStreamImporterConfig implements ImporterConfig
{   
    public static final String APP_NAME = "VoltDBKinesisStreamImporter";
    public static final String APP_VERSION = "1.0.0";
    
    private final String m_appName;
    private final URI m_resourceID;
    private final String m_region;
    private final String m_streamName;
    private final String m_procedure;
    private final String m_secretKey;
    private final String m_accessKey;
    private final long m_idleTimeBetweenReadsInMillis;
    private final int m_maxReadBatchSize;
    private final int m_maxShardsForWorker;
    private final AbstractFormatterFactory m_formatterFactory;

    private KinesisStreamImporterConfig(String appName, String region,
            String streamName,
            String procedure,
            String secretKey,
            String accessKey,
            long idleTimeBetweenReadsInMillis,
            int maxReadBatchSize,
            int maxShardsForWorker,
            URI resourceId,
            AbstractFormatterFactory formatterFactory)
    {
        m_appName = appName;
        m_region = region;
        m_streamName = streamName;
        m_procedure = procedure;
        m_secretKey = secretKey;
        m_accessKey = accessKey;
        m_idleTimeBetweenReadsInMillis = idleTimeBetweenReadsInMillis;
        m_maxReadBatchSize = maxReadBatchSize;
        m_maxShardsForWorker = maxShardsForWorker;
        m_resourceID = resourceId;
        m_formatterFactory = formatterFactory;
    }

    @Override
    public URI getResourceID()
    {
        return m_resourceID;
    }

    @Override
    public AbstractFormatterFactory getFormatterFactory()
    {
        return m_formatterFactory;
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

    public int getMaxReadBatchSize() {
        return m_maxReadBatchSize;
    }

    public int getMaxShardsForWorker() {
        return m_maxShardsForWorker;
    }
    
    public static Map<URI, ImporterConfig> createConfigEntries(Properties props, AbstractFormatterFactory formatterFactory)
    {
        String appName = getProperty(props, "app.name", "");
        String streamName = getProperty(props, "stream.name", "");
        String region = getProperty(props, "region", "");
        String procedure = getProperty(props, "procedure","");
        String secretKey = getProperty(props, "secret.key", "");
        String accessKey = getProperty(props, "access.key", "");
        String readInterval = getProperty(props, "idle.time.between.reads", Long.toString(KinesisClientLibConfiguration.DEFAULT_IDLETIME_BETWEEN_READS_MILLIS));
        String maxReadBatchSize = getProperty(props, "max.read.batch.size", Integer.toString(KinesisClientLibConfiguration.DEFAULT_MAX_RECORDS));
        String maxShardsForWorker = getProperty(props, "max.shards.for.worker", Integer.toString(KinesisClientLibConfiguration.DEFAULT_MAX_LEASES_FOR_WORKER));     
            
        List<Shard> shards = discoverShards(region, streamName, accessKey, secretKey);
        if(shards == null || shards.isEmpty()){
            throw new IllegalArgumentException("Kinesis stream " + streamName + " does not have any shards.");
        }
        Map<URI, ImporterConfig> configs = new HashMap<>();
        for(Shard shard : shards){
            StringBuilder builder = new StringBuilder(128);
            builder.append("kinesis://").append(region).append(".").append(streamName).append(".").append(shard.getShardId());
            URI  uri = URI.create(builder.toString()); 
            ImporterConfig config = new KinesisStreamImporterConfig(appName, region, streamName, 
                    procedure, secretKey, accessKey, 
                    Long.parseLong(readInterval), 
                    Integer.parseInt(maxReadBatchSize), 
                    Integer.parseInt(maxShardsForWorker),
                    uri,
                    formatterFactory);
          
            configs.put(uri, config);
        }
        
        return configs;
    }
    
    /**
     * connect to kinesis stream to discover the shards on the stream
     * @param regionName  The region name where the stream resides
     * @param streamName  The kinesis stream name
     * @param accessKey   The user access key
     * @param secretKey   The user secret key
     * @return a list of shards
     */
    private static List<Shard> discoverShards(String regionName, String streamName, String accessKey, String secretKey) {
        
        Region region = RegionUtils.getRegion(regionName);
        if (region == null) {
            throw new IllegalArgumentException(regionName + " is not a valid AWS region.");
        }
        
        final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonKinesis kinesisClient = new AmazonKinesisClient(credentials,getClientConfigWithUserAgent());      
        kinesisClient.setRegion(region);
       
        try {
            DescribeStreamResult result = kinesisClient.describeStream(streamName);
            if(!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
                throw new IllegalArgumentException("Kinesis stream " + streamName + " is not active.");
            }
            return result.getStreamDescription().getShards();          
        } catch (ResourceNotFoundException e) {
            throw new IllegalArgumentException("Kinesis stream " + streamName + " does not exist. Please create it Kinesis.");
        } catch (Exception e) {
            throw new IllegalArgumentException("Error found while describing the kinesis stream " + streamName);
        }
    }
    
    private static String getProperty(Properties props, String propertyName, String defaultValue){
        String value = props.getProperty(propertyName, defaultValue).trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Property " + propertyName + " is missing in Kinesis importer configuration.");
        }
        return value;
    }

    public static ClientConfiguration getClientConfigWithUserAgent() {
        
        final ClientConfiguration config = new ClientConfiguration();
        final StringBuilder userAgent = new StringBuilder(ClientConfiguration.DEFAULT_USER_AGENT);
        userAgent.append(" ").append(APP_NAME).append("/").append(APP_VERSION);
        config.setUserAgent(userAgent.toString());
        return config;
    }

    public String getAppName() {
        return m_appName;
    }
}
