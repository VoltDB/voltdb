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
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

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

    public KinesisStreamImporter(KinesisStreamImporterConfig config) {
        m_config = config;
    }

    @Override
    public URI getResourceID() {
        return m_config.getResourceID();
    }

    @Override
    public void accept() {

        if (m_eos.get())
            return;

        AtomicBoolean workerInService = new AtomicBoolean(false);
        while (shouldRun()) {

            if (!workerInService.get() && !m_eos.get()) {
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
                    workerInService.compareAndSet(false, true);
                    m_worker.run();

                } catch (Exception e) {

                    rateLimitedLog(Level.ERROR, e, "Error in Kinesis stream importer %s", m_config.getResourceID());
                    if (null != m_worker)
                        m_worker.shutdown();

                    // The worker fails, backoff and restart
                    if (shouldRun()) {
                        backoffSleep(3);
                        workerInService.compareAndSet(true, false);
                    }
                }
            }
        }

        m_eos.compareAndSet(false, true);

        info(null, "The importer stops: %s", m_config.getResourceID().toString());
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
        Gap<BigInteger> m_gapTracker = new Gap<BigInteger>(Integer.getInteger("KINESIS_IMPORT_GAP_LEAD", 32768));
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

            info(null, "Initializing Kinesis stream processing for shard %s, last committed on: %s", m_shardId, seq);
        }

        @Override
        public void processRecords(ProcessRecordsInput records) {

            if (records.getRecords().isEmpty()) {
                return;
            }

            BigInteger seq = BigInteger.ZERO;
            m_gapTracker.allocate(BigInteger.class, records.getRecords().size());
            int offset = 0;
            for (Record record : records.getRecords()) {

                BigInteger seqNum = new BigInteger(record.getSequenceNumber());
                if (seqNum.compareTo(m_lastFetchCommittedSequenceNumber) < 0) {
                    continue;
                }

                if (isDebugEnabled()) {
                    debug(null, "last committed seq: %s, current seq:%s shard %s",
                            m_lastFetchCommittedSequenceNumber.toString(), record.getSequenceNumber(), m_shardId);
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
                    StreamProcedureCallback cb = new StreamProcedureCallback(m_gapTracker, offset, seqNum, m_eos);
                    if (!callProcedure(invocation, cb)) {
                        rateLimitedLog(Level.ERROR, null, "Call procedure error on shard %s", m_shardId);
                        m_gapTracker.commit(offset, seqNum);
                    }
                } catch (UnsupportedEncodingException | FormatException e) {
                    rateLimitedLog(Level.ERROR, e, "Data error on shard %s, data: %s", m_shardId, data);
                    m_gapTracker.commit(offset, seqNum);
                } catch (Exception e) {
                    rateLimitedLog(Level.ERROR, e, "Call procedure error with data %s on shard %s", data, m_shardId);
                    break;
                }
                if (!shouldRun()) {
                    break;
                }
                offset++;
            }

            commitCheckPoint(records.getCheckpointer());
        }

        @Override
        public void shutdown(ShutdownInput shutDownInput) {

            if (shutDownInput.getShutdownReason().equals(ShutdownReason.TERMINATE)) {
                //The shard may be split or merged. checkpoint one last time
                commitCheckPoint(shutDownInput.getCheckpointer());
            }
        }

        /**
        * set a checkpoint to dynamoDB so we know the records prior to the point are processed by this app
        * @param checkpointer  The checkpoint processor
        */
        private void commitCheckPoint(IRecordProcessorCheckpointer checkpointer) {

            int retries = 1;
            while (retries < 4 && shouldRun()) {

                final BigInteger safe = m_gapTracker.getSafeCommitPoint();
                if (safe == null) {
                    break;
                }
                if (isDebugEnabled()) {
                    debug(null, "New checkpoint %s, last checkpoint %s on shard %s", safe.toString(),
                            m_lastFetchCommittedSequenceNumber.toString(), m_shardId);
                }
                if (safe.compareTo(m_lastFetchCommittedSequenceNumber) > 0) {

                    if (isDebugEnabled()) {
                        debug(null, "Trying to checkpoint %s on shard %s", safe.toString(), m_shardId);
                    }

                    try {
                        checkpointer.checkpoint(safe.toString());
                        m_lastFetchCommittedSequenceNumber = safe;
                        break;
                    } catch (ThrottlingException e) {
                        rateLimitedLog(Level.INFO, null, "Checkpoint attempt  %d on shard %s", retries, m_shardId);
                    } catch (Exception e) {
                        //committed on other nodes
                        rateLimitedLog(Level.WARN, e, "Skipping checkpoint %s on shard %s. Reason: %s", safe.toString(),
                                m_shardId, e.getMessage());
                        break;
                    }
                }
                backoffSleep(retries++);
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

        private final Gap<BigInteger> m_tracker;
        private final AtomicBoolean m_dontCommit;
        private final int m_offset;
        private final BigInteger m_seq;

        public StreamProcedureCallback(final Gap<BigInteger> tracker, final int offset, BigInteger seq,
                final AtomicBoolean dontCommit) {

            m_tracker = tracker;
            m_dontCommit = dontCommit;
            m_offset = offset;
            m_seq = seq;
            m_tracker.submit(m_offset, m_seq);
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {

            if (!m_dontCommit.get()) {
                m_tracker.commit(m_offset, m_seq);
            }
        }
    }

    /**
     * The class take an array of checkpoint objects and use their indices within the array as offsets to keep track of the safe
     * commit point. use the safe commit offset to look up the commit point for target system.
     */
    final class Gap<T> {
        long c = 0;
        long s = -1L;
        long[] lag;
        final int lagLen;
        T[] chceckpoints;

        Gap(int leeway) {
            if (leeway <= 0) {
                throw new IllegalArgumentException("leeways is zero or negative");
            }
            lagLen = leeway;
        }

        @SuppressWarnings("unchecked")
        synchronized void allocate(Class<T> clazz, int capacity) {

            chceckpoints = (T[]) Array.newInstance(clazz, capacity);
            c = 0;
            s = -1L;
            lag = new long[lagLen];
        }

        synchronized void submit(long offset, T v) {

            if (!validateOffset((int) offset) || v == null || chceckpoints[(int) offset] != null) {
                return;
            }

            if (s == -1L && offset >= 0) {
                lag[idx(offset)] = c = s = offset;
            }
            if (offset > s) {
                s = offset;
            }

            chceckpoints[(int) offset] = v;
        }

        private final int idx(long offset) {
            return (int) (offset % lagLen);
        }

        synchronized void commit(long offset, T v) {

            if (!validateOffset((int) offset) || v == null || chceckpoints[(int) offset] == null) {
                return;
            }

            if (offset <= s && offset > c && v.equals(chceckpoints[(int) offset])) {
                int ggap = (int) Math.min(lagLen, offset - c);
                if (ggap == lagLen) {
                    c = offset - lagLen + 1;
                    lag[idx(c)] = c;
                }
                lag[idx(offset)] = offset;
                while (ggap > 0 && lag[idx(c)] + 1 == lag[idx(c + 1)]) {
                    ++c;
                }
            }
        }

        synchronized T getSafeCommitPoint() {
            return chceckpoints[(int) c];
        }

        private boolean validateOffset(int offset) {
            return (offset >= 0 && offset < chceckpoints.length);
        }
    }
}
