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

package org.voltdb.export;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.Pair;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.ExportStatsBase.ExportStatsRow;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltZK;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.PersistedMetadata;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.snmp.SnmpTrapSender;
import org.voltdb.utils.BinaryDequeReader;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.io.Files;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

/**
 *  Allows an ExportDataProcessor to access underlying table queues
 */
public class ExportDataSource implements Comparable<ExportDataSource> {

    /**
     * Processor loggers: rate limited loggers are instance-specific
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");
    private final RateLimitedLogger exportLogLimited =  new RateLimitedLogger(TimeUnit.MINUTES.toMillis(1), exportLog, Level.WARN);
    private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");
    private final RateLimitedLogger consoleLogLimited =  new RateLimitedLogger(TimeUnit.MINUTES.toMillis(1), consoleLog, Level.WARN);

    private static final int SEVENX_AD_VERSION = 1;     // AD version for export format 7.x

    private final String m_tableName;
    private final byte [] m_signatureBytes;
    private final int m_partitionId;

    // For stats
    private final int m_siteId;
    private String m_exportTargetName = "";
    private long m_tupleCount = 0;
    private long m_lastQueuedTimestamp = 0;
    private long m_lastAckedTimestamp = 0;
    private long m_averageLatency = 0; // for current counting-session
    private long m_maxLatency = 0; // for current counting-session
    private long m_blocksSentSinceClear = 0;
    private long m_totalLatencySinceClearInMS = 0;
    private long m_overallBlocksSent = 0;
    private long m_overallLatencyInMS = 0;
    private long m_overallMaxLatency = 0;
    private long m_queueGap = 0;
    private StreamStatus m_status = StreamStatus.ACTIVE;

    private long m_firstUnpolledSeqNo = 1L; // sequence number starts from 1
    private long m_lastReleasedSeqNo = 0L;

    // The committed sequence number is the sequence number of the last row
    // of the last committed transaction pushed up from the EE, and acknowledged by
    // the export client.
    private long m_committedSeqNo = 0L;

    // Null value for last committed sequence number in EE buffer
    // See {@code ExportStreamBlock} in EE code.
    public static long NULL_COMMITTED_SEQNO = -1L;

    private volatile boolean m_closed = false;
    private final StreamBlockQueue m_committedBuffers;
    // m_pollFuture is used for a common case to improve efficiency, export decoder thread creates
    // future and passes to EDS executor thread, if EDS executor has no new buffer to poll, the future
    // is assigned to m_pollFuture. When site thread pushes buffer to EDS executor thread, m_pollFuture
    // is reused to notify export decoder to stop waiting.
    private PollTask m_pollTask;
    private final AtomicReference<Pair<Mailbox, ImmutableList<Long>>> m_ackMailboxRefs =
            new AtomicReference<>(Pair.of((Mailbox)null, ImmutableList.<Long>builder().build()));
    private final Semaphore m_bufferPushPermits = new Semaphore(16);

    private volatile ListeningExecutorService m_es;
    // A place to keep unfinished export buffer when processor shuts down.
    private final AtomicReference<AckingContainer> m_pendingContainer = new AtomicReference<>();
    // Is EDS from catalog or from disk pdb?
    private volatile boolean m_isInCatalog;
    private final Generation m_generation;
    private final File m_adFile;
    private ExportClientBase m_client;
    private boolean m_readyForPolling;
    // This flag is specifically added for XDCR conflicts stream, which export conflict logs
    // on every host. Every data source with this flag set to true is an export master.
    private boolean m_runEveryWhere = false;
    // *Generation ID* is actually a timestamp generated during catalog update(UpdateApplicationBase.java)
    // If a catalog update occurs, the generation ID will be updated and a new buffer needs to be
    // written to new PBD segment.
    private long m_currentGenerationId;

    private ExportSequenceNumberTracker m_gapTracker = new ExportSequenceNumberTracker();

    private MigrateRowsDeleter m_migrateRowsDeleter;

    // Export coordinator manages Export Leadership, Mastership, and gap correction.
    // Made package private for JUnit test support
    ExportCoordinator m_coordinator;

    private static final boolean ENABLE_AUTO_GAP_RELEASE = Boolean.getBoolean("ENABLE_AUTO_GAP_RELEASE");

    static enum StreamStatus {
        ACTIVE,
        DROPPED,
        BLOCKED
    }

    public static enum StreamStartAction {
        INITIALIZATION,
        REJOIN,
        RECOVER,
        SNAPSHOT_RESTORE
    }

    static class QueryResponse {
        long lastSeq;
        public QueryResponse(long lastSeq) {
            this.lastSeq = lastSeq;
        }
        public boolean canCoverGap() {
            return lastSeq != Long.MIN_VALUE;
        }
    }

    public static class ReentrantPollException extends ExecutionException {
        private static final long serialVersionUID = 1L;
        ReentrantPollException() { super(); }
        ReentrantPollException(String s) { super(s); }
    }

    private static class PollTask {
        private final SettableFuture<AckingContainer> m_pollFuture;

        public PollTask(SettableFuture<AckingContainer> fut) {
            m_pollFuture = fut;
        }

        public void setFuture(AckingContainer cont) {
            m_pollFuture.set(cont);
        }

        public void setException(Throwable t) {
            m_pollFuture.setException(t);
        }
    }

    /**
     * Create a new data source.
     *
     * @param db
     * @param tableName
     * @param partitionId
     */
    public ExportDataSource(
            Generation generation,
            ExportDataProcessor processor,
            String tableName,
            int partitionId,
            int siteId,
            long genId,
            CatalogMap<Column> catalogMap,
            Column partitionColumn,
            String overflowPath
            ) throws IOException
    {
        m_generation = generation;
        m_currentGenerationId = genId;
        m_tableName = tableName;
        m_signatureBytes = m_tableName.getBytes(StandardCharsets.UTF_8);
        String nonce = m_tableName + "_" + partitionId;

        // Create a NEW PBD: this will discard any stale data discovered on disk
        m_committedBuffers = new StreamBlockQueue(overflowPath, nonce, m_tableName, partitionId, genId, true);
        m_gapTracker = m_committedBuffers.scanForGap();
        // Set first unpolled to a safe place
        resetStateInRejoinOrRecover(0L, StreamStartAction.INITIALIZATION);

        /*
         * This is not the catalog relativeIndex(). This ID incorporates
         * a catalog version and a table id so that it is constant across
         * catalog updates that add or drop tables.
         */
        m_partitionId = partitionId;

        m_siteId = siteId;
        if (exportLog.isDebugEnabled()) {
            exportLog.debug(toString() + " reads gap tracker from PBD:" + m_gapTracker.toString());
        }

        m_adFile = new File(overflowPath, nonce + ".ad");
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Creating ad for " + nonce);
        }
        byte jsonBytes[] = null;
        try {
            JSONStringer stringer = new JSONStringer();
            stringer.object();
            writeAdvertisementTo(stringer);
            stringer.endObject();
            JSONObject jsObj = new JSONObject(stringer.toString());
            jsonBytes = jsObj.toString(4).getBytes(StandardCharsets.UTF_8);
        } catch (JSONException e) {
            exportLog.error("Failed to Write ad file for " + nonce);
            throw new RuntimeException(e);
        }

        try (FileOutputStream fos = new FileOutputStream(m_adFile)) {
            fos.write(jsonBytes);
            fos.getFD().sync();
        }
        m_isInCatalog = true;
        m_client = processor.getExportClient(m_tableName);
        if (m_client != null) {
            m_exportTargetName = m_client.getTargetName();
            m_runEveryWhere = m_client.isRunEverywhere();
            if (exportLog.isDebugEnabled() && m_runEveryWhere) {
                exportLog.debug(toString() + " is a replicated export stream");
            }
        }
        m_es = CoreUtils.getListeningExecutorService("ExportDataSource for table " +
                    m_tableName + " partition " + m_partitionId, 1);
    }

    public ExportDataSource(Generation generation, File adFile,
            Map<Integer, Integer> localPartitionsToSites,
            final ExportDataProcessor processor,
            final long genId) throws IOException {
        m_generation = generation;
        m_currentGenerationId = genId;
        m_adFile = adFile;
        String overflowPath = adFile.getParent();
        byte data[] = Files.toByteArray(adFile);
        try {
            JSONObject jsObj = new JSONObject(new String(data, StandardCharsets.UTF_8));

            long version = jsObj.getLong("adVersion");
            if (version != SEVENX_AD_VERSION) {
                throw new IOException("Unsupported ad file version " + version);
            }
            m_partitionId = jsObj.getInt("partitionId");
            // SiteId is outside the valid range if it is no longer local
            int partitionsLocalSite = MpInitiator.MP_INIT_PID + 1;
            if (localPartitionsToSites != null) {
                partitionsLocalSite = localPartitionsToSites.getOrDefault(m_partitionId, partitionsLocalSite);
            }
            m_siteId = partitionsLocalSite;
            m_tableName = jsObj.getString("tableName");
            m_signatureBytes = m_tableName.getBytes(StandardCharsets.UTF_8);
        } catch (JSONException e) {
            throw new IOException(e);
        }

        // Create a PBD, recovering any data present on disk
        final String nonce = m_tableName + "_" + m_partitionId;
        m_committedBuffers = new StreamBlockQueue(overflowPath, nonce, m_tableName, m_partitionId, genId);
        m_gapTracker = m_committedBuffers.scanForGap();

        // Pretend it's rejoin so we set first unpolled to a safe place
        resetStateInRejoinOrRecover(0L, StreamStartAction.INITIALIZATION);
        if (exportLog.isDebugEnabled()) {
            exportLog.debug(toString() + " at AD file reads gap tracker from PBD:" + m_gapTracker.toString());
        }
        //EDS created from adfile is always from disk.
        m_isInCatalog = false;
        m_client = processor.getExportClient(m_tableName);
        if (m_client != null) {
            m_exportTargetName = m_client.getTargetName();
            m_runEveryWhere = m_client.isRunEverywhere();
            if (exportLog.isDebugEnabled() && m_runEveryWhere) {
                exportLog.debug(toString() + " is a replicated export stream");
            }
        }
        m_es = CoreUtils.getListeningExecutorService("ExportDataSource for table " +
                m_tableName + " partition " + m_partitionId, 1);
    }

    /**
     * Set the {@code ExportCoordinator} - we expect this just after the constructor.
     *
     * Note: made separate from constructor for JUnit test support.
     *
     * @param zk
     * @param hostId
     */
    public void setCoordination(ZooKeeper zk, Integer hostId) {
        m_coordinator = new ExportCoordinator(zk, VoltZK.exportCoordination, hostId, this);
    }

    public void setReadyForPolling(boolean readyForPolling) {
        m_readyForPolling = readyForPolling;
        if (m_readyForPolling) {
            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    if (m_closed) {
                        if (exportLog.isDebugEnabled()) {
                            exportLog.debug("Closed, not ready for polling");
                        }
                        return;
                    }
                    if (!m_readyForPolling) {
                        return;
                    }
                    if (!m_coordinator.isCoordinatorInitialized()) {
                        m_coordinator.initialize(m_runEveryWhere);
                    }
                    if (isMaster() && m_pollTask != null) {
                        pollImpl(m_pollTask);
                    }
                }
            });
        }
    }

    public void markInCatalog(boolean inCatalog) {
        m_isInCatalog = inCatalog;
    }

    public boolean inCatalog() {
        return m_isInCatalog;
    }

    public long getGenerationIdCreated() {
        return m_committedBuffers.getGenerationIdCreated();
    }

    // Package private as only used for tests
    boolean isMaster() {
        return m_coordinator.isMaster();
    }

    public synchronized void updateAckMailboxes(final Pair<Mailbox, ImmutableList<Long>> ackMailboxes) {
        //export stream for run-everywhere clients doesn't need ack mailboxes
        if (m_runEveryWhere || m_closed) {
            return;
        }
        if (exportLog.isDebugEnabled()) {
            if (ackMailboxes.getSecond() != null) {
                exportLog.debug("Mailbox " + CoreUtils.hsIdToString(ackMailboxes.getFirst().getHSId()) + " is registered for " + this.toString() +
                        " : replicas " + CoreUtils.hsIdCollectionToString(ackMailboxes.getSecond()) );
            } else {
                exportLog.debug("Mailbox " + CoreUtils.hsIdToString(ackMailboxes.getFirst().getHSId()) + " is registered for " + this.toString());
            }
        }
        m_ackMailboxRefs.set( ackMailboxes);
    }

    public void setClient(ExportClientBase client) {
        m_exportTargetName = client != null ? client.getTargetName() : "";
        m_client = client;
    }

    public ExportClientBase getClient() {
        return m_client;
    }

    private synchronized void releaseExportBytes(long releaseSeqNo) throws IOException {

        // Return if released offset is in an already-released past.
        // Note: on recover the first RELEASE_BUFFER message received by a
        // replica will come with releaseSeqNo == m_lastReleasedSeqNo;
        // We must go over the release logic to properly truncate the gap
        // tracker and update the tuples pending.
        if (releaseSeqNo < m_lastReleasedSeqNo) {
            return;
        }

        // Check whether a pending container was completely acked
        AckingContainer pend = m_pendingContainer.get();
        if (pend != null) {
            if (releaseSeqNo > pend.m_lastSeqNo) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Discarding via ack a pending " + pend);
                }
                m_pendingContainer.set(null);
                pend.internalDiscard();
            }
        }

        // Release buffers
        while (!m_committedBuffers.isEmpty() && releaseSeqNo >= m_committedBuffers.peek().startSequenceNumber()) {
            StreamBlock sb = m_committedBuffers.peek();
            if (releaseSeqNo >= sb.lastSequenceNumber()) {
                try {
                    m_committedBuffers.pop();
                    m_lastAckedTimestamp = Math.max(m_lastAckedTimestamp, sb.getTimestamp());
                } finally {
                    sb.discard();
                }
            } else if (releaseSeqNo >= sb.startSequenceNumber()) {
                sb.releaseTo(releaseSeqNo);
                m_lastAckedTimestamp = Math.max(m_lastAckedTimestamp, sb.getTimestamp());
                break;
            }
        }

        // If blocked and we released past the first gap, revert to ACTIVE
        if (m_status == StreamStatus.BLOCKED &&
                m_gapTracker.getFirstGap() != null &&
                releaseSeqNo >= m_gapTracker.getFirstGap().getSecond()) {
            //master stream cannot resolve a gap by receiving
            // an ACK from itself, only replica stream can do.
            exportLog.info("Export queue gap resolved by releasing bytes at seqNo: "
                    + releaseSeqNo + ", resuming export, tracker map = " + m_gapTracker.toString());
            clearGap(true);
        }

        m_lastReleasedSeqNo = releaseSeqNo;
        m_gapTracker.truncateBefore(releaseSeqNo);
        // If persistent log contains gap, mostly due to node failures and rejoins, ACK from leader might
        // cover the gap gradually.
        // Next poll starts from this number, if it sit in between buffers and stream is active, next poll will
        // kick off gap detection/resolution.
        m_firstUnpolledSeqNo = Math.max(m_firstUnpolledSeqNo, releaseSeqNo + 1);
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Truncating tracker via ack to " + releaseSeqNo + ", next seqNo to poll is " +
                    m_firstUnpolledSeqNo + ", tracker map is " + m_gapTracker.toString() +
                    ", m_committedBuffers.isEmpty() " + m_committedBuffers.isEmpty());
        }
        return;
    }

    public String getTableName() {
        return m_tableName;
    }

    public int getPartitionId() {
        return m_partitionId;
    }

    public long getGeneration() {
        return m_currentGenerationId;
    }

    public final void writeAdvertisementTo(JSONStringer stringer) throws JSONException {
        stringer.keySymbolValuePair("adVersion", SEVENX_AD_VERSION);
        stringer.keySymbolValuePair("partitionId", getPartitionId());
        stringer.keySymbolValuePair("tableName", getTableName());
        stringer.keySymbolValuePair("startTime", ManagementFactory.getRuntimeMXBean().getStartTime());
    }

    /**
     * Compare two ExportDataSources for equivalence. This currently does not
     * compare column names, but it should once column add/drop is allowed.
     * This comparison is performed to decide if a datasource in a new catalog
     * needs to be passed to a proccessor.
     */
    @Override
    public int compareTo(ExportDataSource o) {
        int result;

        result = (m_partitionId - o.m_partitionId);
        if (result != 0) {
            return result;
        }

        result = m_tableName.compareTo(o.m_tableName);
        if (result != 0) {
            return result;
        }

        return 0;
    }

    /**
     * Make sure equal objects compareTo as 0.
     * @param o
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ExportDataSource)) {
            return false;
        }

        return compareTo((ExportDataSource)o) == 0;
    }

    @Override
    public int hashCode() {
        // based on implementation of compareTo
        int result = 0;
        result += m_tableName.hashCode();
        result += m_partitionId;
        // does not factor in replicated / unreplicated.
        // does not factor in column names / schema
        return result;
    }


    public long sizeInBytes() {
        try {
            ListeningExecutorService es = getExecutorService();
            return es.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    if (m_closed) {
                        return 0L;
                    }
                    return m_committedBuffers.sizeInBytes();
                }
            }).get();
        } catch (RejectedExecutionException e) {
            return 0;
        } catch (Throwable t) {
            Throwables.throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    public ListenableFuture<ExportStatsRow> getImmutableStatsRow(boolean interval) {
        return m_es.submit(new Callable<ExportStatsRow>() {
            @Override
            public ExportStatsRow call() throws Exception {
                long avgLatency;
                long maxLatency;
                if (m_maxLatency > m_overallMaxLatency) {
                    m_overallMaxLatency = m_maxLatency;
                }
                if (interval) {
                    avgLatency = m_averageLatency;
                    maxLatency = m_maxLatency;
                    m_overallBlocksSent += m_blocksSentSinceClear;
                    m_overallLatencyInMS += m_totalLatencySinceClearInMS;
                    m_blocksSentSinceClear = 0;
                    m_totalLatencySinceClearInMS = 0;
                    m_maxLatency = 0;
                    m_averageLatency = 0;
                }
                else {
                    if (m_blocksSentSinceClear + m_overallBlocksSent > 0) {
                        avgLatency = (m_totalLatencySinceClearInMS + m_overallLatencyInMS)
                                        / (m_blocksSentSinceClear + m_overallBlocksSent);
                    }
                    else {
                        avgLatency= 0;
                    }
                    maxLatency = m_overallMaxLatency;
                }
                String exportingRole;
                if (m_runEveryWhere) {
                    exportingRole = "XDCR";
                } else {
                    // Note that we are 'ACTIVE' == 'TRUE' only if we are export master AND we have
                    // an export client configured
                    exportingRole = (m_coordinator.isMaster() && m_client != null ? "TRUE" : "FALSE");
                }

                // Note: pending tuples are calculated regardless of any gaps
                int tPend = (int) (m_tupleCount - m_lastReleasedSeqNo);

                return new ExportStatsRow(m_partitionId, m_siteId, m_tableName, m_exportTargetName,
                        exportingRole, m_tupleCount, tPend,
                        m_lastQueuedTimestamp, m_lastAckedTimestamp,
                        avgLatency, maxLatency, m_queueGap, m_status.toString());
            }
        });
    }

    private long calcEndSequenceNumber(long startSeq, int tupleCount) {
        return startSeq + tupleCount - 1;
    }

    private boolean isAcked(long seqNo) {
        return m_lastReleasedSeqNo > 0 && seqNo <= m_lastReleasedSeqNo ;
    }

    private void pushExportBufferImpl(
            long startSequenceNumber,
            long committedSequenceNumber,
            int tupleCount,
            long uniqueId,
            BBContainer cont,
            boolean poll) throws Exception {
        long lastSequenceNumber = calcEndSequenceNumber(startSequenceNumber, tupleCount);
        if (exportLog.isTraceEnabled()) {
            exportLog.trace("pushExportBufferImpl [" + startSequenceNumber + "," +
                    lastSequenceNumber + "], poll=" + poll);
        }
        if (cont != null) {
            ByteBuffer buffer = cont.b();
            // header space along is 8 bytes
            assert (buffer.capacity() > StreamBlock.HEADER_SIZE);
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            // We should never try to push data on a source that is not in catalog
            if (!inCatalog()) {
                exportLog.warn("Source not in catalog, dropping buffer. " +
                        " Buffer info: [" + startSequenceNumber + "," + lastSequenceNumber + "] Size: " + tupleCount +
                        " last released seq: " + m_lastReleasedSeqNo);
                cont.discard();
                return;
            }

            // Count the tuples even if already acked by another replica
            assert(lastSequenceNumber > m_tupleCount);
            m_tupleCount = lastSequenceNumber;

            // Drop already acked buffer
            if (isAcked(lastSequenceNumber)) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Dropping already acked buffer. " +
                            " Buffer info: [" + startSequenceNumber + "," + lastSequenceNumber + "] Size: " + tupleCount +
                            " last released seq: " + m_lastReleasedSeqNo);
                }
                cont.discard();
                return;
            }

            try {
                // Create a block to offer: NOTE this block should not
                // be used for anything else than offering, as it doesn't have metadata.
                BinaryDequeReader.Entry<PersistedMetadata> entry = BinaryDequeReader.Entry.wrap(cont);

                StreamBlock sb = new StreamBlock(
                        entry,
                        startSequenceNumber,
                        committedSequenceNumber,
                        tupleCount,
                        uniqueId);

                // Mark release sequence number to partially acked buffer.
                if (isAcked(sb.startSequenceNumber())) {
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Setting releaseSeqNo as " + m_lastReleasedSeqNo +
                                " for SB [" + sb.startSequenceNumber() + "," + sb.lastSequenceNumber() +
                                "] for partition " + m_partitionId);
                    }
                    sb.releaseTo(m_lastReleasedSeqNo);
                }
                m_gapTracker.addRange(sb.unreleasedSequenceNumber(), lastSequenceNumber);
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Append [" + sb.unreleasedSequenceNumber() + "," + lastSequenceNumber +"] to gap tracker.");
                }

                m_lastQueuedTimestamp = sb.getTimestamp();
                m_committedBuffers.offer(sb);
            } catch (IOException e) {
                VoltDB.crashLocalVoltDB("Unable to write to export overflow.", true, e);
            }
        }
        if (poll) {
            // Note: this should only be executed in a runnable
            assert(!m_es.isShutdown());
            pollImpl(m_pollTask);
        }
    }

    public void pushExportBuffer(
            final long startSequenceNumber,
            final long committedSequenceNumber,
            final int tupleCount,
            final long uniqueId,
            final BBContainer cont) {
        try {
            m_bufferPushPermits.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (m_closed) {
            m_bufferPushPermits.release();
            exportLogLimited.log("Closed: ignoring export buffer with " + tupleCount + " rows",
                    EstTime.currentTimeMillis());
            if (cont != null) {
                cont.discard();
            }
            return;
        }
        try {
            m_es.execute((new Runnable() {
                @Override
                public void run() {
                    try {
                        if (!m_closed) {
                            pushExportBufferImpl(startSequenceNumber, committedSequenceNumber,
                                    tupleCount, uniqueId, cont, m_readyForPolling);
                        } else {
                            exportLogLimited.log("Closed: ignoring export buffer with " + tupleCount + " rows",
                                    EstTime.currentTimeMillis());
                            if (cont != null) {
                                cont.discard();
                            }
                        }
                    } catch (Throwable t) {
                        VoltDB.crashLocalVoltDB("Error pushing export  buffer", true, t);
                    } finally {
                        m_bufferPushPermits.release();
                    }
                }
            }));
        } catch (RejectedExecutionException rej) {
            m_bufferPushPermits.release();
            //We are shutting down very much rolling generation so dont passup for error reporting.
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Export buffer rejected by data source executor: ", rej);
            }
            if (cont != null) {
                cont.discard();
            }
        }
    }

    public ListenableFuture<?> truncateExportToSeqNo(StreamStartAction action, long sequenceNumber, long generationIdCreated) {
        return m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    if (sequenceNumber < 0) {
                        if (sequenceNumber == -1L && m_partitionId != 0 && m_gapTracker.isEmpty()) {
                            // ENG-17199: we are creating EDS instances for all partitions on replicated tables
                            // but only partition 0 is used and can be truncated
                            if (exportLog.isDebugEnabled()) {
                                exportLog.debug("Ignoring truncation for partition " + m_partitionId);
                            }
                        } else {
                            exportLog.error("Snapshot does not include valid truncation point for partition " +
                                    m_partitionId);
                        }
                        return;
                    }
                    // In rejoin node the initial generation ID first comes from catalog on ZK, if stream
                    // snapshot tells us a different initial generation ID, we trust it and use it as
                    // the source of truth.
                    if (generationIdCreated != m_committedBuffers.getGenerationIdCreated()) {
                        m_committedBuffers.setInitialGenerationId(generationIdCreated);
                    }
                    if (m_committedBuffers.deleteStaleBlocks(generationIdCreated)) {
                        // Stale export buffers are deleted , re-create the tracker.
                        m_gapTracker = m_committedBuffers.scanForGap();
                    }

                    if (action == StreamStartAction.RECOVER) {
                        m_committedBuffers.truncateToSequenceNumber(sequenceNumber);
                        // Export buffers are truncated to snapshot point
                        m_gapTracker.truncateAfter(sequenceNumber);
                        // export data after truncation point will be regenerated by c/l replay
                    }
                    // From snapshot restore the stream internal state and sequence number
                    // should start from beginning
                    m_tupleCount = 0L;
                    if (action == StreamStartAction.RECOVER || action == StreamStartAction.REJOIN) {
                        m_tupleCount  = sequenceNumber;
                        m_coordinator.setInitialSequenceNumber(m_tupleCount );
                    }

                    // Need to update pending tuples in rejoin
                    resetStateInRejoinOrRecover(m_tupleCount, action);
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Truncating tracker via snapshot truncation to " + m_tupleCount +
                                ", action is " + action +
                                ", generationId is " + generationIdCreated +
                                ", tracker map is " + m_gapTracker.toString());
                    }

                    // Need to handle drained source if truncate emptied the buffers
                    // Note, this always happen before the first poll
                    handleDrainedSource(null);
                } catch (Throwable t) {
                    VoltDB.crashLocalVoltDB("Error while trying to truncate export to seq " +
                            sequenceNumber, true, t);
                }
            }
        });
    }

    private class SyncRunnable implements Runnable {
        @Override
        public void run() {
            try {
                m_committedBuffers.sync();
            } catch (IOException e) {
                VoltDB.crashLocalVoltDB("Unable to write to export overflow.", true, e);
            }
        }
    }

    public ListenableFuture<?> sync() {
        return m_es.submit(new SyncRunnable());
    }

    public boolean isClosed() {
        return m_closed;
    }

    /**
     * This is called on updateCatalog when an exporting stream is dropped.
     * <p>
     * Note: The {@code ExportDataProcessor} must have been shut down prior
     * to calling this method.
     * <p>
     * The {@code closeAndDelete} sequence is executed asynchronously and the
     * {@link ExportManager} will be notified in {@code onCoordinatorShutdown}
     * when the sequence has completed. This is used to prevent the same table
     * being re-created by catalog updates while the data sources are closing.
     */
    public void closeAndDelete() {

        // We're going away, so shut ourselves from the external world
        m_closed = true;
        m_ackMailboxRefs.set(null);

        m_es.execute(new Runnable() {
            @Override
            public void run() {

                try {
                    // Returning null indicates end of stream
                    try {
                        if (m_pollTask != null) {
                            m_pollTask.setFuture(null);
                        }
                    } catch (RejectedExecutionException reex) {
                        // Ignore, {@code GuestProcessor} was closed
                    }
                    m_pollTask = null;

                    // Discard the pending container, shortcutting the standard discard logic
                    AckingContainer ack = m_pendingContainer.getAndSet(null);
                    if (ack != null) {
                        if (exportLog.isDebugEnabled()) {
                            exportLog.debug("Discard pending container, lastSeqNo: " + ack.getLastSeqNo());
                        }
                        ack.internalDiscard();
                    }
                    m_committedBuffers.closeAndDelete();
                    m_adFile.delete();
                    m_coordinator.shutdown();
                } catch(Exception e) {
                    exportLog.rateLimitedLog(60, Level.WARN, e, "Error closing commit buffers");
                }
            }
        });
    }

    /**
     * Shutdown: closes the data source.
     * <p>
     * {@link ExportManager} will be notified in {@code onCoordinatorShutdown}.
     *
     * @return a {@link ListenableFuture} to sync on the closing of PBDs,
     *      note that this future does not wait for the {@link ExportCoordinator} shutdown.
     */
    public ListenableFuture<?> shutdown() {
        m_closed = true;
        return m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    m_committedBuffers.close();
                    m_ackMailboxRefs.set(null);
                    m_coordinator.shutdown();
                } catch (Exception e) {
                    exportLog.error(e.getMessage(), e);
                }
            }
        });
    }

    /**
     * Callback from {@code ExportCoordinator} when its shutdown is complete,
     * this is called from a runnable invoked on this executor.
     */
    public void onCoordinatorShutdown() {
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Shutdown executor");
        }
        m_es.shutdown();
        VoltDB.getExportManager().onClosedSource(m_tableName, m_partitionId);
    }

    // Needs to be thread-safe, EDS executor, export decoder and site thread both touch m_pendingContainer.
    public void setPendingContainer(AckingContainer container) {
        Preconditions.checkNotNull(m_pendingContainer.get() != null, "Pending container must be null.");
        if (m_closed) {
            // A very slow export decoder must have noticed the export processor shutting down
            exportLog.info("Discarding stale pending container");
            container.internalDiscard();
        } else {
            m_pendingContainer.set(container);
        }
    }

    /**
     * Callback from {@code ExportCoordinator} to resume polling after trackers
     * were collected from the other nodes; will try to satisfy a pending poll.
     * Note that this callback is invoked from a runnable on the executor service.
     */
    public void resumePolling() {
        if (m_closed) {
            return;
        }
        if (m_pollTask != null) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Resuming polling...");
            }
            pollImpl(m_pollTask);

        } else {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("No pending poll request...");
            }
        }
    }

    /**
     * Poll request from {@code GuestProcessor}
     *
     * @return
     */
    public ListenableFuture<AckingContainer> poll() {
        //ENG-15763, create SettableFuture that lets us handle executor exceptions
        final SettableFuture<AckingContainer> fut = SettableFuture.create(false);
        PollTask pollTask = new PollTask(fut);
        try {
            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    /*
                     * The poll is blocking through the cached future, shouldn't
                     * call poll a second time until a response has been given
                     * which satisfies the cached future.
                     */
                    if (m_pollTask != null) {
                        try {
                            pollTask.setException(new ReentrantPollException("Reentrant poll detected: InCat = " + m_isInCatalog +
                                    " In ExportDataSource for Table " + getTableName() + ", Partition " + getPartitionId()));
                        } catch (RejectedExecutionException reex) {
                            // Ignore: the {@code GuestProcessor} was shut down...
                            if (exportLog.isDebugEnabled()) {
                                exportLog.debug("Reentrant Poll exception rejected ");
                            }
                        }
                        return;
                    }
                    try {
                        if (!m_closed) {
                            pollImpl(pollTask);
                        }
                    } catch (Exception e) {
                        exportLog.error("Exception polling export buffer", e);
                    } catch (Error e) {
                        VoltDB.crashLocalVoltDB("Error polling export buffer", true, e);
                    }
                }
            });
        } catch (RejectedExecutionException rej) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Polling from export data source rejected by data source executor.");
            }
        }
        return fut;
    }

    /**
     * Poll a pending container before moving to m_commitedBuffers.
     *
     * A pending container is set by the {@code GuestProcessor} instance when it is
     * shut down while processing it. Therefore it should be the reply to the first poll
     * of the new {@code GuestProcessor} instance.
     *
     * @param pollTask the polling task
     * @return true if the poll is completed, false if polling needs to proceed on m_committedBuffers
     */
    private boolean pollPendingContainer(PollTask pollTask) {

        AckingContainer cont = m_pendingContainer.getAndSet(null);
        if (cont == null) {
            return false;
        }

        try {
            // The pending container satisfies the poll
            pollTask.setFuture(cont);
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Picked up pending container with committedSeqNo " + cont.m_commitSeqNo);
            }
        } catch (RejectedExecutionException reex) {
            // The {@code GuestProcessor} instance wasn't able to handle the future (e.g. being
            // shut down by a catalog update): place the polled container in pending
            // so it is picked up by the new GuestProcessor.
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Pending a rejected " + cont);
            }
            setPendingContainer(cont);
        }
        // Clear the pending poll, if any
        m_pollTask = null;
        return true;
    }

    private synchronized void pollImpl(PollTask pollTask) {

        if (pollTask == null) {
            return;
        }

        try {
            if (handleDrainedSource(pollTask)) {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Exiting a drained source on poll");
                }
                return;
            }

            // If not ready for polling, memorize the outstanding poll
            if (!m_readyForPolling) {
                if (m_pollTask == null) {
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Not ready for polling, memorize polling for " + m_firstUnpolledSeqNo);
                    }
                    m_pollTask = pollTask;
                }
                return;
            }

            // Poll pending container before polling m_committedBuffers.
            // If a pending container is present, this means we were export master so
            // don't check the export coordinator for this buffer.
            if (pollPendingContainer(pollTask)) {
                return;
            }

            if (exportLog.isDebugEnabled()) {
                exportLog.debug("polling data from seqNo " + m_firstUnpolledSeqNo);
            }

            // Scan m_committedBuffers to determine the fist seqNo to poll.
            StreamBlock first_unpolled_block = null;

            // Assemble a list of blocks already acked so that they can be discarded
            // outside of the m_committedBuffers critical section.
            ArrayList<StreamBlock> blocksToDelete = new ArrayList<>();

            // Inside this critical section do the work to find out what block should
            // be returned by the next poll. Copying and sending the data will take place
            // outside the critical section.
            try {
                Iterator<StreamBlock> iter = m_committedBuffers.iterator();
                while (iter.hasNext()) {

                    StreamBlock block = iter.next();

                    // If the block is already acked list it to be discarded
                    if (block.lastSequenceNumber() < m_firstUnpolledSeqNo) {
                        blocksToDelete.add(block);
                        iter.remove();
                        if (exportLog.isDebugEnabled()) {
                            exportLog.debug("Delete polled buffer [" + block.startSequenceNumber() + "," +
                                    block.lastSequenceNumber() + "]");
                        }
                        continue;
                    }

                    // Are we the Export Master for the unpolled sequence number?
                    if (!m_coordinator.isExportMaster(m_firstUnpolledSeqNo)) {
                        if (exportLog.isDebugEnabled()) {
                            exportLog.debug("Not export master for seqNo " + m_firstUnpolledSeqNo);
                        }
                        // Put the poll aside until we become Export Master
                        m_pollTask = pollTask;
                        return;
                    }

                    // If the next block is not in sequence, and we were told we're Export Master,
                    // we are BLOCKED.
                    if (m_firstUnpolledSeqNo < block.startSequenceNumber()) {
                        // Block on the gap and put the poll aside until gap resolved or released
                        blockOnGap(m_firstUnpolledSeqNo, block.startSequenceNumber());
                        m_pollTask = pollTask;
                        return;
                    }

                    // We have our next block
                    first_unpolled_block = block;
                    m_firstUnpolledSeqNo = block.lastSequenceNumber() + 1;
                    break;
                }
            } catch (RuntimeException e) {
                if (e.getCause() instanceof IOException) {
                    VoltDB.crashLocalVoltDB("Error attempting to find unpolled export data", true, e);
                } else {
                    throw e;
                }
            } finally {
                // Discard the blocks
                for (StreamBlock sb : blocksToDelete) {
                    m_gapTracker.truncateBefore(sb.lastSequenceNumber());
                    sb.discard();
                }
            }

            if (first_unpolled_block == null) {
                //If there are no unpolled blocks, memorize the pending poll.
                m_pollTask = pollTask;
            } else {
                // If stream was previously blocked by a gap, now it skips/fulfills the gap
                // change the status back to normal.
                if (m_status == StreamStatus.BLOCKED) {
                    assert (m_coordinator.isMaster()); // only master stream can resolve the data gap
                    exportLog.info("Export queue gap resolved. Resuming export for " + ExportDataSource.this.toString());
                    clearGap(true);
                }

                // Create a container, forcing the schema as required by the poll() caller,
                // or on first block after transition to Export Master (schema may have
                // changed while we were not Export Master).

                final AckingContainer ackingContainer = AckingContainer.create(
                        this, first_unpolled_block, m_committedBuffers);

                try {
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Posting Export data for " + ackingContainer.toString());
                    }
                    pollTask.setFuture(ackingContainer);
                } catch (RejectedExecutionException reex) {
                    // The {@code GuestProcessor} instance wasn't able to handle the future (e.g. being
                    // shut down by a catalog update): place the polled container in pending
                    // so it is picked up by the new GuestProcessor.
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Pending a rejected " + ackingContainer);
                    }
                    setPendingContainer(ackingContainer);
                } finally {
                    m_pollTask = null;
                }
            }
        } catch (Throwable t) {
            try {
                pollTask.setException(t);
            } catch (RejectedExecutionException reex) {
                /* Ignore */
                exportLog.error("Poll exception rejected");
            } finally {
                m_pollTask = null;
            }
        }
    }

    /**
     * Calling this method will advance the export stream to {@code lastSeqNo}, release underlying
     * PBD file if needed, and also forwarding the ACK message to replica(s) of the export stream.
     *
     * @param lastSeqNo the export sequence number advances to
     * @param commitSeqNo the committed export sequence number
     * @param commitTxnId the committed TxnId
     * @param startTime the time of when the buffer is delivered to export client
     * @throws RejectedExecutionException - if the stream's task executor cannot accept the task
     */
    public void advance(long lastSeqNo, long commitSeqNo, long commitTxnId, long startTime) {
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                if (exportLog.isTraceEnabled()) {
                    exportLog.trace("Advance sequence number to: " + lastSeqNo);
                }
                assert(startTime != 0);
                long elapsedMS = System.currentTimeMillis() - startTime;
                m_blocksSentSinceClear += 1;
                m_totalLatencySinceClearInMS += elapsedMS;
                m_averageLatency = m_totalLatencySinceClearInMS / m_blocksSentSinceClear;
                if (m_averageLatency > m_maxLatency) {
                    m_maxLatency = m_averageLatency;
                }

                try {
                    localAck(commitSeqNo, lastSeqNo);
                    forwardAckToOtherReplicas();
                    if (m_migrateRowsDeleter != null && commitTxnId > 0 && m_coordinator.isMaster()) {
                        m_migrateRowsDeleter.delete(commitTxnId);
                    }
                } catch (Exception e) {
                    exportLog.error("Error acking export buffer", e);
                } catch (Error e) {
                    VoltDB.crashLocalVoltDB("Error acking export buffer", true, e);
                }
            }
        });
    }

    private BinaryPayloadMessage createReleaseBufferMessage() {

        final int msgLen = getAckMessageLength();

        ByteBuffer buf = ByteBuffer.allocate(msgLen);
        buf.put(ExportManager.RELEASE_BUFFER);
        buf.putInt(m_partitionId);
        buf.putInt(m_signatureBytes.length);
        buf.put(m_signatureBytes);
        buf.putLong(m_committedSeqNo);
        buf.putLong(m_committedBuffers.getGenerationIdCreated());

        BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());
        return bpm;
    }

    public void forwardAckToOtherReplicas() {
        // In RunEveryWhere mode, every data source is master, no need to send out acks.
        if (m_runEveryWhere) {
            return;
        }
        Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
        if (p == null) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug(ExportDataSource.this.toString() + ": Skip forwarding ack of seq " + m_committedSeqNo);
            }
            return;
        }
        Mailbox mbx = p.getFirst();
        if (mbx != null && p.getSecond().size() > 0) {

            BinaryPayloadMessage bpm = createReleaseBufferMessage();
            for( Long siteId: p.getSecond()) {
                mbx.send(siteId, bpm);
            }
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Send RELEASE_BUFFER to " + toString()
                        + " with sequence number " + m_committedSeqNo
                        + ", generation ID " + m_committedBuffers.getGenerationIdCreated()
                        + " from " + CoreUtils.hsIdToString(mbx.getHSId())
                        + " to " + CoreUtils.hsIdCollectionToString(p.getSecond()));
            }
        }
    }

    private int getAckMessageLength() {
        // msg type(1) + partition:int(4) + length:int(4) +
        // signaturesBytes.length + ackUSO:long(8) + genIdCreated:long(8).
        final int msgLen = 1 + 4 + 4 + m_signatureBytes.length + 8 + 8;
        return msgLen;
    }

    /**
     * Entry point for receiving acknowledgments from remote entities.
     *
     * @param seq acknowledged sequence number, ALWAYS last row of a transaction, or 0.
     */
    public void remoteAck(final long seq) {

        m_es.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    // Reflect the remote ack in our state
                    localAck(seq, seq);

                    // If we passed a safe point, try satisfying a pending poll request
                    if (m_coordinator.isSafePoint(seq)) {
                        if (exportLog.isDebugEnabled()) {
                            exportLog.debug("Passed safe point " + seq + ", resume polling.");
                        }
                        pollImpl(m_pollTask);
                    }
                } catch (Exception e) {
                    exportLog.error("Error acking export buffer", e);
                } catch (Error e) {
                    VoltDB.crashLocalVoltDB("Error acking export buffer", true, e);
                }
            }
        });
    }

    /**
     * Common acknowledgement method: MUST be invoked from runnable
     *
     * @param commitSeq
     * @param ackSeq
     */
    public void localAck(long commitSeq, long ackSeq) {
        if (m_closed) {
            return;
        }
        setCommittedSeqNo(commitSeq);
        ackImpl(ackSeq);
    }

     private void ackImpl(long seq) {
        //Process the ack if any and add blocks to the delete list or move the released sequence number
        if (seq > 0) {
            try {
                releaseExportBytes(seq);
                if (handleDrainedSource(m_pollTask)) {
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Handled a drained source on ack");
                    }
                    m_pollTask = null;
                }
            } catch (IOException e) {
                VoltDB.crashLocalVoltDB("Error attempting to release export bytes", true, e);
                return;
            }
        }
    }

     /**
      * Notify the generation when source is drained on an unused partition.
      *
      * @param pollTask the current poll request or null
      * @return true if handled a drained source
      *
      * @throws IOException
      */
     private boolean handleDrainedSource(PollTask pollTask) throws IOException {

         // It may be that the drained source was detected and handled
         // in the truncate, and that we may be called again from GuestProcessor.
         // Send an end of stream to GuestProcessor but don't notify the generation.
         if (m_closed) {
             endOfStream(pollTask);
             return true;
         }

         // Send end of stream to GuestProcessor and notify generation.
         if (!inCatalog() && m_committedBuffers.isEmpty()) {
             endOfStream(pollTask);
             m_generation.onSourceDrained(m_partitionId, m_tableName);
             return true;
         }
         return false;
     }

     private void endOfStream(PollTask pollTask) {
         //Returning null indicates end of stream
         try {
             if (pollTask != null) {
                 pollTask.setFuture(null);
             }
         } catch (RejectedExecutionException reex) {
             // Ignore, {@code GuestProcessor} was closed
             if (exportLog.isDebugEnabled()) {
                 exportLog.debug("End of Stream event rejected ");
             }
         }
     }

    /**
     * On processor shutdown, clear pending poll and expect to be reactivated by new
     * {@code GuestProcessor} instance.
     */
    public void onProcessorShutdown() {
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Handling processor shutdown for " + this);
                }
                m_pollTask = null;
                m_readyForPolling = false;
            }
        });
    }

    /**
     * Become partition leader.
     */
    public void becomeLeader() {
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                if (m_coordinator.isPartitionLeader()) {
                    return;
                }
                try {
                    if (!m_es.isShutdown() || !m_closed) {
                        exportLog.debug("Becoming the leader of stream " + m_tableName + ", partition " + getPartitionId());
                        m_coordinator.becomeLeader();
                    }
                } catch (Exception e) {
                    exportLog.error("Error in becoming leader", e);
                }
            }
        });
    }

    public void setRunEveryWhere(boolean runEveryWhere) {
        if (exportLog.isDebugEnabled() && runEveryWhere != m_runEveryWhere) {
            exportLog.debug("Change " + toString() + " to " +
                    (runEveryWhere ? "replicated stream" : " non-replicated stream"));
        }
        m_runEveryWhere = runEveryWhere;
        if (m_runEveryWhere) {
            //export stream for run-everywhere clients doesn't need ack mailbox
            m_ackMailboxRefs.set(null);
        }
    }

    public ListeningExecutorService getExecutorService() {
        return m_es;
    }

    public byte[] getTableSignature() {
        return m_signatureBytes;
    }
    public long getLastReleaseSeqNo() {
        return m_lastReleasedSeqNo;
    }

    // Status is accessed by multiple threads
    public synchronized void setStatus(StreamStatus status) {
        this.m_status = status;
    }

    public StreamStatus getStatus() {
        return m_status;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ExportDataSource for table ")
                .append(getTableName())
                .append(" partition ")
                .append(getPartitionId())
                .append(" (")
                .append(m_status)
                ;
        if (m_coordinator != null) {
            sb.append(", ");
            sb.append((m_coordinator.isMaster() ? "Master":"Replica"));
            if (m_coordinator.isPartitionLeader()) {
                sb.append(", Leader");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    // During rejoin it's possible that the stream is blocked by a gap before the export
    // sequence number carried by rejoin snapshot, so we couldn't trust the sequence number
    // in snapshot to find where to poll next buffer. The right thing to do should be setting
    // the firstUnpolled to a safe point to avoid releasing a gap prematurely, waits for
    // current master to tell us where to poll next buffer.
    private void resetStateInRejoinOrRecover(long initialSequenceNumber, StreamStartAction action) {
        if (action == StreamStartAction.REJOIN) {
            if (!m_gapTracker.isEmpty()) {
                m_lastReleasedSeqNo = Math.max(m_lastReleasedSeqNo, m_gapTracker.getFirstSeqNo() - 1);
            }
        } else {
            m_lastReleasedSeqNo = Math.max(m_lastReleasedSeqNo, initialSequenceNumber);
        }
        // Rejoin or recovery should be on a transaction boundary (except maybe in a gap situation)
        m_committedSeqNo = m_lastReleasedSeqNo;
        m_firstUnpolledSeqNo =  m_lastReleasedSeqNo + 1;
        if (exportLog.isDebugEnabled()) {
            exportLog.debug(toString() + " reset state in " + action
                    + ", initial seqNo " + initialSequenceNumber + ", last released/committed " + m_lastReleasedSeqNo
                    + ", first unpolled " + m_firstUnpolledSeqNo + ", initial generation ID " + m_committedBuffers.getGenerationIdCreated());
        }
    }

    public String getTarget() {
        return m_exportTargetName;
    }

    private void blockOnGap(long start, long end) {
        // Set ourselves as blocked
        m_status = StreamStatus.BLOCKED;
        m_queueGap = end - start;
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Blocked on " + start + ", until " + end);
        }

        // Check whether we can auto-release
        VoltDBInterface voltdb = VoltDB.instance();
        if (voltdb.isClusterComplete()) {
            if (ENABLE_AUTO_GAP_RELEASE) {
                processStreamControl(StreamControlOperation.RELEASE);
            } else {
                // Show warning only in full cluster.
                String warnMsg = "Export is blocked, missing rows [" +
                        start + ", " + end + "] from " +
                        ExportDataSource.this.toString() +
                        ". Please rejoin any nodes missing from the cluster or use " +
                        "the 'voltadmin export release' command to skip the missing data.";
                exportLogLimited.log(warnMsg, EstTime.currentTimeMillis());
                consoleLogLimited.log(warnMsg, EstTime.currentTimeMillis() );
                SnmpTrapSender snmp = VoltDB.instance().getSnmpTrapSender();
                if (snmp != null) {
                    snmp.streamBlocked(warnMsg);
                }
            }
        }
    }

    public synchronized boolean processStreamControl(StreamControlOperation operation) {
        switch (operation) {
        case RELEASE:
            if (m_status == StreamStatus.BLOCKED) {
                // Satisfy a pending poll request
                m_es.execute(() -> {
                    try {
                        // Check again in case of multiple export release tasks are queued
                        // because restarted @ExportControl transaction
                        if (m_status != StreamStatus.BLOCKED || !isMaster() || m_pollTask == null) {
                            return;
                        }

                        Pair<Long, Long> gap = m_gapTracker.getFirstGap(m_firstUnpolledSeqNo);
                        long firstUnpolledSeqNo = gap == null ? m_gapTracker.getFirstSeqNo() : gap.getSecond() + 1;
                        exportLog.info("Skipping over gap [" + m_firstUnpolledSeqNo + '-' + (firstUnpolledSeqNo - 1)
                                + "]  in " + this);
                        m_firstUnpolledSeqNo = firstUnpolledSeqNo;
                        clearGap(true);
                        pollImpl(m_pollTask);
                    } catch (Exception e) {
                        exportLog.error("Exception polling export buffer after RELEASE", e);
                    } catch (Error e) {
                        VoltDB.crashLocalVoltDB("Error polling export bufferafter RELEASE", true, e);
                    }
                });
                return true;
            }
            break;
        default:
            // should not happen since the operation is verified prior to this call
        }
        return false;
    }

    private void clearGap(boolean setActive) {
        m_queueGap = 0;
        if (setActive) {
            setStatus(StreamStatus.ACTIVE);
        }
    }

    private void setCommittedSeqNo(long committedSeqNo) {
        if (committedSeqNo == NULL_COMMITTED_SEQNO) {
            return;
        }
        if (committedSeqNo > m_committedSeqNo) {
            m_committedSeqNo  = committedSeqNo;
        }
    }

    public void setupMigrateRowsDeleter(int partitionId) {
        m_migrateRowsDeleter = new MigrateRowsDeleter(m_tableName, partitionId);
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("MigrateRowsDeleter has been initialized for table: " + m_tableName + ", partition:" + partitionId);
        }
    }

    public void updateCatalog(Table table, long genId) {
        if (m_closed) {
            return;
        }
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    PersistedMetadata metadata = new PersistedMetadata(table, m_partitionId,
                            m_committedBuffers.getGenerationIdCreated(), genId);
                    m_committedBuffers.updateSchema(metadata);
                } catch (IOException e) {
                    VoltDB.crashLocalVoltDB("Unable to write PBD export header.", true, e);
                }
                m_currentGenerationId = genId;
            }
        });
    }

    // This is called when schema update doesn't affect export
    public void updateGenerationId(long genId) {
        // This needs to be serialized through the executor service, so the when updateCatalog() writes
        // genId to the PBD header, the correct generation id for that schema is used.
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                m_currentGenerationId = genId;
            }
        });
    }

    // Called from {@code ExportCoordinator}, returns duplicate of tracker
    public ExportSequenceNumberTracker getTracker() {
        ExportSequenceNumberTracker tracker = m_gapTracker.duplicate();
        return tracker;
    }
}
