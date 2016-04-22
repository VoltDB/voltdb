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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.Level;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.Invocation;
import org.voltdb.importer.formatter.Formatter;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Importer implementation for pull socket importer. At runtime, there will
 * one instance of this per host and socket combination.
 */
public class KinesisStreamImporter extends AbstractImporter {
   
    private KinesisStreamImporterConfig m_config;
    private final AtomicBoolean m_eos = new AtomicBoolean(false);
 
    private Worker m_streamPuller;
    
    public KinesisStreamImporter(KinesisStreamImporterConfig config)
    {
        m_config = config;
    }

    @Override
    public URI getResourceID()
    {
        return m_config.getResourceID();
    }

    @Override
    public void accept() {
        try {
            if (m_eos.get()) return;

            KinesisClientLibConfiguration kclConfig = new KinesisClientLibConfiguration(
                    m_config.getAppName(), 
                    m_config.getStreamName(), 
                    credentials(), 
                    UUID.randomUUID().toString());
            
            kclConfig.withRegionName(m_config.getRegion())
                .withMaxRecords(m_config.getMaxReadBatchSize())
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .withIdleTimeBetweenReadsInMillis(m_config.getIdleTimeBetweenReads())
                .withKinesisClientConfig(KinesisStreamImporterConfig.getClientConfigWithUserAgent());
            
            m_streamPuller = new Worker.Builder()
                    .recordProcessorFactory(new RecordProcessorFactory())
                    .config(kclConfig)
                    .build();
  
            m_streamPuller.run();  
           
        } catch(Throwable t) {
            rateLimitedLog(Level.ERROR, t, "Error in Kinesis stream importer %s", m_config.getResourceID());
        }
    }

    @Override
    public void stop() {
        if (m_eos.compareAndSet(false, true)) {
            m_streamPuller.shutdown();
        }
    }

    @Override
    public String getName() {
        return KinesisStreamImporterConfig.APP_NAME;
    }
    
    /**
     * Create AWSCredentialsProvider with access key id and secret key for the user. Make sure that the suer has permission to
     * have access to Kinesis Stream and DynamoDB
     * @return AWSCredentialsProvider
     * @throws AmazonClientException
     */
    public AWSCredentialsProvider credentials() throws AmazonClientException {
        return new StaticCredentialsProvider(
             new BasicAWSCredentials(m_config.getAccessKey(), m_config.getSecretKey())
        );
    }
    
    private class RecordProcessorFactory implements IRecordProcessorFactory {
        
        @Override
        public IRecordProcessor createProcessor()
        {
            return new StreamConsumer();
        }
    }
    
    private class StreamConsumer implements IRecordProcessor {

        /**
         * Number of milliseconds between checkpoint retry
         */
        private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
        
        /**
         * Number of times of checkpoint retry
         */
        private static final int NUM_RETRIES = 10;
        
        
        private String m_shardId;
        private Formatter<String> m_formatter;
        private String m_sequenceNumber;
        
        public StreamConsumer() { 
        }
  
        @SuppressWarnings("unchecked")
        @Override
        public void initialize(InitializationInput initInput) {
            m_shardId = initInput.getShardId();
            m_sequenceNumber = initInput.getExtendedSequenceNumber().getSequenceNumber();
            m_formatter = ((Formatter<String>) m_config.getFormatterFactory().create());
            info(null, "Initializing record processor for shard: %s, starting seqeunce number: %s",m_shardId, m_sequenceNumber);
        }
        @Override
        public void processRecords(ProcessRecordsInput records) {       
           for (Record record : records.getRecords()) {
               String data = null;
               try {
                   data = new String(record.getData().array(), "UTF-8");
                   if(isDebugEnabled()){
                       info(null, m_config.getResourceID() + "/sequence num: %s, data: %s", record.getSequenceNumber(), data);
                   }
               } catch (UnsupportedEncodingException e) {
                   rateLimitedLog(Level.ERROR, e, "Error in Kinesis stream importer in shard %s", m_shardId);
                   continue;
               }
               try{
                   Invocation invocation = new Invocation(m_config.getProcedure(), m_formatter.transform(data));
                   if (!callProcedure(invocation)) {
                       rateLimitedLog(Level.ERROR, null, "Error in Kinesis stream importer in shard %s", m_shardId);
                   }
               } catch (Exception e) {
                   rateLimitedLog(Level.ERROR, null, "Procedure error with data %s in shard %s", data, m_shardId);
               }
           }
           if(!records.getRecords().isEmpty()){
               checkpoint(records.getCheckpointer());
           }
        }

        @Override
        public void shutdown(ShutdownInput shutDownInput) {
            
            if(shutDownInput.getShutdownReason().equals(ShutdownReason.TERMINATE)){
                checkpoint(shutDownInput.getCheckpointer());
            }
        }
        
        /**
         * Mark the records as processed.
         */
        private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    checkpointer.checkpoint();
                    break;
                } catch (ShutdownException se) {
                    rateLimitedLog(Level.ERROR, se, "Caught shutdown exception, skipping checkpoint. shard id:%s, sequence number:%s.", m_shardId, m_sequenceNumber);
                    break;
                } catch (ThrottlingException e) {
                    if (i >= (NUM_RETRIES - 1)) {
                        rateLimitedLog(Level.ERROR, e,"Checkpoint failed after %s attempts: shard id:%s, sequence number:%s.", (i + 1), m_shardId, m_sequenceNumber);
                        break;
                    } else {
                        rateLimitedLog(Level.INFO, null,"Transient issue when checkpointing - attempt %s of %s", (i+1), NUM_RETRIES);
                    }
                } catch (InvalidStateException e) {
                    rateLimitedLog(Level.ERROR, e,"Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library. shard id:%s, sequence number:%s.",  m_shardId, m_sequenceNumber);
                    break;
                }
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    rateLimitedLog(Level.ERROR, e,"Interrupted sleep when checkpointing.");
                }
            }
        }
    }
 }
