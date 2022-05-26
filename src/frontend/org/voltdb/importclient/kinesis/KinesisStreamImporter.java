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

import java.math.BigInteger;
import java.net.URI;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.math.NumberUtils;

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
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.amazonaws.services.kinesis.model.Record;

import org.voltcore.logging.Level;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.Invocation;
import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;
/**
 * Importer implementation for Kinesis Stream importer. one instance of this per stream, per shard, per app
 */
public class KinesisStreamImporter extends AbstractImporter {

    private KinesisStreamImporterConfig m_config;
    private AtomicLong m_submitCount = new AtomicLong(0);
    private AtomicLong m_cbcnt = new AtomicLong(0);

    private Worker m_worker;
    private String m_workerId;

    public KinesisStreamImporter(KinesisStreamImporterConfig config) {
        m_config = config;
        m_workerId = UUID.randomUUID().toString();
        KinesisClientLibConfiguration kclConfig = new KinesisClientLibConfiguration(m_config.getAppName(),
                m_config.getStreamName(), credentials(), m_workerId);

        kclConfig.withRegionName(m_config.getRegion()).withMaxRecords((int) m_config.getMaxReadBatchSize())
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .withIdleTimeBetweenReadsInMillis(m_config.getIdleTimeBetweenReads())
                .withMetricsLevel(MetricsLevel.NONE)
                .withTaskBackoffTimeMillis(m_config.getTaskBackoffTimeMillis()).withKinesisClientConfig(
                        KinesisStreamImporterConfig.getClientConfigWithUserAgent(m_config.getAppName()));
        m_worker = new Worker.Builder().recordProcessorFactory(new RecordProcessorFactory()).config(kclConfig)
                .build();
    }

    @Override
    public URI getResourceID() {
        return m_config.getResourceID();
    }

    @Override
    public String getTaskThreadName() {
        return getName() + "-" + m_config.getAppName() + "-" + m_workerId;
    };

    @Override
    public void accept() {
        info(null, "Starting data stream fetcher for " + m_config.getResourceID().toString());
        try {
            m_worker.run();
        } catch (RuntimeException e) {
            //aws silences all the exceptions but IllegalArgumentException, throw RuntimeException.
            rateLimitedLog(Level.ERROR, e, "Error in Kinesis stream importer %s", m_config.getResourceID());
            m_worker.shutdown();
        }

        info(null, "Data stream fetcher stopped for %s. Callback Rcvd: %d. Submitted: %d",
                m_config.getResourceID().toString(), m_cbcnt.get(), m_submitCount.get());

    }

    @Override
    public void stop() {
        m_worker.shutdown();
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

        private String m_shardId = new String("unknown");
        private Formatter m_formatter;
        Gap m_gapTracker = new Gap(Integer.getInteger("KINESIS_IMPORT_GAP_LEAD", 32768));
        private BigInteger m_lastFetchCommittedSequenceNumber = BigInteger.ZERO;

        public StreamConsumer() {
        }

        @SuppressWarnings("unchecked")
        @Override
        public void initialize(InitializationInput initInput) {

            m_shardId = initInput.getShardId();
            m_formatter = m_config.getFormatterBuilder().create();

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
            m_gapTracker.resetTo();
            int offset = 0;
            for (Record record : records.getRecords()) {
                m_submitCount.incrementAndGet();
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

                Object params[] = null;
                try {
                    params = m_formatter.transform(record.getData());
                    Invocation invocation = new Invocation(m_config.getProcedure(), params);

                    StreamProcedureCallback cb = new StreamProcedureCallback(m_gapTracker, offset, seqNum, m_cbcnt);
                    if (!callProcedure(invocation, cb)) {
                        rateLimitedLog(Level.ERROR, null, "Call procedure error on shard %s", m_shardId);
                        m_gapTracker.commit(offset, seqNum);
                    }
                } catch (FormatException e) {
                    rateLimitedLog(Level.ERROR, e, "Data error on shard %s, data: %s", m_shardId, Arrays.toString(params));
                    m_gapTracker.commit(offset, seqNum);
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

            if (isDebugEnabled()) {
                debug(null, "shard ID: " + m_shardId + ", shutdown reason: " + shutDownInput.getShutdownReason().name());
            }

            if (ShutdownReason.TERMINATE.equals(shutDownInput.getShutdownReason())) {
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

    private void backoffSleep(int failedCount) {
        try {
            Thread.sleep(200 * failedCount);
        } catch (InterruptedException e) {

            //do not propagate exception since aws will swallow all.
            rateLimitedLog(Level.WARN, e, "Interrupted sleep when checkpointing.");
        }
    }

    private final static class StreamProcedureCallback implements ProcedureCallback {

        private final Gap m_tracker;
        private final int m_offset;
        private final BigInteger m_seq;
        private final AtomicLong m_cbcnt;

        public StreamProcedureCallback(final Gap tracker, final int offset, BigInteger seq, AtomicLong cbcnt) {

            m_tracker = tracker;
            m_offset = offset;
            m_seq = seq;
            m_cbcnt = cbcnt;
            m_tracker.submit(m_offset, m_seq);
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            m_tracker.commit(m_offset, m_seq);
            m_cbcnt.incrementAndGet();
        }
    }

    /**
     * The class take an array of checkpoint objects and use their indices within the array as offsets to keep track of the safe
     * commit point. use the safe commit offset to look up the commit point for target system.
     */
    final class Gap {
        long c = 0;
        long s = -1L;
        long[] lag;
        final int lagLen;
        BigInteger[] checkpoints;
        long offer = -1L;
        private final long gapTrackerCheckMaxTimeMs = 2_000;

        Gap(int leeway) {
            if (leeway <= 0) {
                throw new IllegalArgumentException("leeways is zero or negative");
            }
            lagLen = leeway;
            checkpoints = new BigInteger[(int)m_config.getMaxReadBatchSize()];
        }

        synchronized void resetTo() {

            //reset this to take new checkpoints. The offsets after checkpoint commit are not relevant anymore
            Arrays.fill(checkpoints, null);

            //The offset is the index among the fetched records.
            c = 0;
            s = -1L;
            lag = new long[lagLen];
        }

        synchronized void submit(long offset, BigInteger v) {

            if (!validateOffset((int) offset) || v == null || checkpoints[(int) offset] != null) {
                return;
            }

            if (s == -1L && offset >= 0) {
                lag[idx(offset)] = c = s = offset;
            }

            if ((offset - c) >= lag.length) {
                offer = offset;
                try {
                    wait(gapTrackerCheckMaxTimeMs);
                } catch (InterruptedException e) {
                    rateLimitedLog(Level.WARN, e,
                            "Gap tracker wait was interrupted." + m_config.getResourceID().toString());
                }
            }

            if (offset > s) {
                s = offset;
            }

            checkpoints[(int) offset] = v;
        }

        private final int idx(long offset) {
            return (int) (offset % lagLen);
        }

        synchronized void commit(long offset, BigInteger v) {

            if (!validateOffset((int) offset) || v == null || checkpoints[(int) offset] == null) {
                return;
            }

            if (offset <= s && offset > c && v.equals(checkpoints[(int) offset])) {
                int ggap = (int) Math.min(lagLen, offset - c);
                if (ggap == lagLen) {
                    c = offset - lagLen + 1;
                    lag[idx(c)] = c;
                }
                lag[idx(offset)] = offset;
                while (ggap > 0 && lag[idx(c)] + 1 == lag[idx(c + 1)]) {
                    ++c;
                }

                if (offer >= 0 && (offer - c) < lag.length) {
                    offer = -1L;
                    notify();
                }
            }
        }

        synchronized BigInteger getSafeCommitPoint() {
            if(checkpoints != null && validateOffset((int) c)){
                 return checkpoints[(int) c];
            }
            return null;
        }

        private boolean validateOffset(int offset) {
            return (offset >= 0 && offset < checkpoints.length);
        }
    }
}
