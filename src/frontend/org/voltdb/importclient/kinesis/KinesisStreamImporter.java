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
import java.math.BigInteger;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.math.NumberUtils;
import org.voltcore.logging.Level;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.Invocation;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
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
 * Importer implementation for Kinesis Stream importer. one instance of this per stream, per shard, per app
 */
public class KinesisStreamImporter extends AbstractImporter {

    private KinesisStreamImporterConfig m_config;
    private final AtomicBoolean m_eos = new AtomicBoolean(false);

    private Worker m_worker;

    private AtomicLong m_recordCnt = new AtomicLong(0L);
    private AtomicLong m_skipCommitCnt = new AtomicLong(0L);

    public KinesisStreamImporter(KinesisStreamImporterConfig config) {
        m_config = config;
    }

    @Override
    public URI getResourceID() {
        return m_config.getResourceID();
    }

    @Override
    public void accept() {

        info(null, "Starting  kinesis stream importer for  %s", m_config.getResourceID().toString());

        if (m_eos.get())
            return;

        AtomicBoolean workerStatus = new AtomicBoolean(true);
        while (shouldRun()) {

            if (workerStatus.get() && !m_eos.get()) {

                workerStatus.compareAndSet(true, false);

                try {
                    KinesisClientLibConfiguration kclConfig = new KinesisClientLibConfiguration(m_config.getAppName(),
                            m_config.getStreamName(), credentials(), UUID.randomUUID().toString());

                    kclConfig.withRegionName(m_config.getRegion()).withMaxRecords(m_config.getMaxReadBatchSize())
                            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                            .withIdleTimeBetweenReadsInMillis(m_config.getIdleTimeBetweenReads())
                            .withTaskBackoffTimeMillis(m_config.getTaskBackoffTimeMillis()).withKinesisClientConfig(
                                    KinesisStreamImporterConfig.getClientConfigWithUserAgent(m_config.getAppName()));

                    m_worker = new Worker.Builder().recordProcessorFactory(new RecordProcessorFactory())
                            .config(kclConfig).build();

                    info(null, "Starting worker for Kinesis stream  %s", m_config.getStreamName());
                    m_worker.run();

                } catch (Exception e) {

                    rateLimitedLog(Level.ERROR, e, "Error in Kinesis stream importer %s", m_config.getResourceID());
                    if (null != m_worker)
                        m_worker.shutdown();

                    // The worker fails, backoff and restart
                    if (shouldRun()) {
                        backoffSleep(3);
                        workerStatus.compareAndSet(false, true);
                    }
                }
            }
        }

        m_eos.compareAndSet(false, true);

        info(null, "The importer stops %s", m_config.getResourceID().toString());
    }

    @Override
    public void stop() {
        m_eos.compareAndSet(false, true);
        if (null != m_worker) {
            m_worker.shutdown();
        }
    }

    @Override
    public String getName() {
        return KinesisStreamImporterConfig.APP_NAME;
    }

    /**
     * Create AWSCredentialsProvider with access key id and secret key for the
     * user. The user should have read/write permission to Kinesis Stream and DynamoDB
     * @return AWSCredentialsProvider Provides credentials used to sign AWS requests
     * @throws AmazonClientException
     */
    public AWSCredentialsProvider credentials() throws AmazonClientException {
        return new StaticCredentialsProvider(new BasicAWSCredentials(m_config.getAccessKey(), m_config.getSecretKey()));
    }

    private class RecordProcessorFactory implements IRecordProcessorFactory {

        @Override
        public IRecordProcessor createProcessor() {
            return new StreamConsumer();
        }
    }

    private class StreamConsumer implements IRecordProcessor {

        private String m_shardId;
        private Formatter<String> m_formatter;
        Gap m_gapTracker = new Gap(Integer.getInteger("KINESIS_IMPORT_GAP_LEAD", 32_768));
        private BigInteger m_lastFetchCommittedSequenceNumber = BigInteger.ZERO;

        public StreamConsumer() {
        }

        @SuppressWarnings("unchecked")
        @Override
        public void initialize(InitializationInput initInput) {

            m_shardId = initInput.getShardId();
            m_formatter = ((Formatter<String>) m_config.getFormatterFactory().create());

            String seq = initInput.getExtendedSequenceNumber().getSequenceNumber();
            if (NumberUtils.isDigits(seq)) {
                m_lastFetchCommittedSequenceNumber = new BigInteger(seq);
            }

            info(null, "Initializing record processor for shard: %s, last committed on: %s", m_shardId, seq);

        }

        @Override
        public void processRecords(ProcessRecordsInput records) {

            info(null, "Processing %d records on shard %s", records.getRecords().size(), m_shardId);

            BigInteger seq = BigInteger.ZERO;
            AtomicLong cbcnt = new AtomicLong(0);
            m_gapTracker.allocateTrackObj(records.getRecords().size());
            int offset = 0;
            for (Record record : records.getRecords()) {

                BigInteger seqNum = new BigInteger(record.getSequenceNumber());
                if (seqNum.compareTo(m_lastFetchCommittedSequenceNumber) < 0) {
                    continue;
                }

                if (isDebugEnabled()) {
                    debug(null, "last committed seq: %s  shard %s", m_lastFetchCommittedSequenceNumber.toString(),
                            record.getSequenceNumber());
                    if (seqNum.compareTo(seq) < 0) {
                        debug(null, "Record %d is out of sequence on shard %s", seqNum, m_shardId);
                    } else {
                        seq = seqNum;
                    }
                }

                String data = null;
                try {
                    data = new String(record.getData().array(), "UTF-8");

                    Invocation invocation = new Invocation(m_config.getProcedure(), m_formatter.transform(data));

                    StreamProcedureCallback cb = new StreamProcedureCallback(m_gapTracker, offset, seqNum, cbcnt,
                            m_eos);
                    if (!callProcedure(invocation, cb)) {
                        rateLimitedLog(Level.ERROR, null, "Call procedure error on shard %s", m_shardId);
                        m_gapTracker.commit(offset);
                    }
                } catch (UnsupportedEncodingException | FormatException e) {
                    rateLimitedLog(Level.ERROR, e, "Data encoding or call procedure error on shard %s, data: %s",
                            m_shardId, data);
                    m_gapTracker.commit(offset);
                } catch (Exception e) {
                    rateLimitedLog(Level.ERROR, e, "Call procedure error with data %s on shard %s", data, m_shardId);
                    break;
                }
                if (!shouldRun()) {
                    break;
                }
                offset++;
            }

            checkpoint(records.getCheckpointer());
        }

        @Override
        public void shutdown(ShutdownInput shutDownInput) {

            if (shutDownInput.getShutdownReason().equals(ShutdownReason.TERMINATE)) {
                //shard may be split or merged. let us checkpoint one last time
                checkpoint(shutDownInput.getCheckpointer());
            }
        }

        /**
        * set a checkpoint to dynamoDB so we know the records prior to the point are processed by this app
        * @param checkpointer  The checkpoint processor
        */
        private void checkpoint(IRecordProcessorCheckpointer checkpointer) {

            int retries = 1;
            while (retries < 4 && shouldRun()) {
                final BigInteger safe = m_gapTracker.getSafePoint();

                if (isDebugEnabled()) {
                    debug(null, "New commit checkpoint %s, last checkpoint %s on shard %s", safe.toString(),
                            m_lastFetchCommittedSequenceNumber.toString(), m_shardId);
                }
                if (safe.compareTo(m_lastFetchCommittedSequenceNumber) > 0) {

                    if (isDebugEnabled()) {
                        debug(null, "Trying to checkpoint %s on shard %s", safe.toString(), m_shardId);
                    }

                    try {
                        checkpointer.checkpoint(safe.toString());
                        m_lastFetchCommittedSequenceNumber = new BigInteger(safe.toByteArray());
                        break;
                    } catch (ThrottlingException e) {
                        rateLimitedLog(Level.INFO, null, "Checkpoint attempt  %s on shard %s", retries, m_shardId);
                    } catch (Exception e) {
                        //committed on other nodes
                        rateLimitedLog(Level.WARN, e, "Skipping checkpoint %s on shard %s. Reason: %s", safe.toString(),
                                m_shardId, e.getMessage());
                        break;
                    }

                    backoffSleep(retries++);
                }
            }
        }
    }

    private int backoffSleep(int failedCount) {
        try {
            Thread.sleep(1000 * failedCount++);
            if (failedCount > 10)
                failedCount = 1;
        } catch (InterruptedException e) {
            rateLimitedLog(Level.ERROR, e, "Interrupted sleep when checkpointing.");
        }
        return failedCount;
    }

    private final static class StreamProcedureCallback implements ProcedureCallback {

        private final AtomicLong m_cbcnt;
        private final Gap m_tracker;
        private final AtomicBoolean m_dontCommit;
        private final int m_offset;

        public StreamProcedureCallback(final Gap tracker, final int offset, BigInteger seq, final AtomicLong cbcnt,
                final AtomicBoolean dontCommit) {
            m_cbcnt = cbcnt;
            m_tracker = tracker;
            m_dontCommit = dontCommit;
            m_offset = offset;
            m_tracker.submit(m_offset, seq);
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {

            m_cbcnt.incrementAndGet();
            if (!m_dontCommit.get()) {
                m_tracker.commit(m_offset);
            }
        }
    }

    final class Gap {
        long c = 0;
        long s = -1L;
        final long[] lag;
        BigInteger[] trackedObj;

        Gap(int leeway) {
            if (leeway <= 0) {
                throw new IllegalArgumentException("leeways is zero or negative");
            }
            lag = new long[leeway];
        }

        synchronized void allocateTrackObj(int size) {
            trackedObj = new BigInteger[size];
            c = 0;
            s = -1L;
            for (int i = 0; i < lag.length; i++)
                lag[i] = -1L;
        }

        synchronized void submit(long offset, BigInteger obj) {
            if (s == -1L && offset >= 0) {
                lag[idx(offset)] = c = s = offset;
            }
            if (offset > s) {
                s = offset;
            }

            trackedObj[(int) offset] = obj;
        }

        private final int idx(long offset) {
            return (int) (offset % lag.length);
        }

        synchronized void resetTo(long offset) {
            if (offset < 0) {
                throw new IllegalArgumentException("offset is negative");
            }
            lag[idx(offset)] = s = c = offset;
        }

        synchronized long commit(long offset) {
            if (offset <= s && offset > c) {
                int ggap = (int) Math.min(lag.length, offset - c);
                if (ggap == lag.length) {
                    c = offset - lag.length + 1;
                    lag[idx(c)] = c;
                }
                lag[idx(offset)] = offset;
                while (ggap > 0 && lag[idx(c)] + 1 == lag[idx(c + 1)]) {
                    ++c;
                }
            }
            return c;
        }

        synchronized BigInteger getSafePoint() {
            return trackedObj[(int) commit(-1L)];
        }
    }
}
