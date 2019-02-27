/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.ExportStatsBase.ExportStatsRow;
import org.voltdb.RealVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.common.Constants;
import org.voltdb.export.AdvertisedDataSource.ExportFormat;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.sysprocs.ExportControl.OperationMode;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.PBDSegment;
import org.voltdb.utils.VoltFile;

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
     * Processors also log using this facility.
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");
    private static final int SEVENX_AD_VERSION = 1;     // AD version for export format 7.x

    private final String m_database;
    private final String m_tableName;
    private final String m_signature;
    private final byte [] m_signatureBytes;
    private final int m_partitionId;

    // For stats
    private final int m_siteId;
    private String m_exportTargetName = "";
    private long m_tupleCount = 0;
    private AtomicInteger m_tuplesPending = new AtomicInteger(0);
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

    private final ExportFormat m_format;

    private long m_firstUnpolledSeqNo = 1L; // sequence number starts from 1
    private long m_lastReleasedSeqNo = 0L;

    // The committed sequence number is the sequence number of the last row
    // of the last committed transaction pushed up from the EE, and acknowledged by
    // the export client.
    private long m_committedSeqNo = 0L;

    // Null value for last committed sequence number in EE buffer
    // See {@code ExportStreamBlock} in EE code.
    public static long NULL_COMMITTED_SEQNO = -1L;

    // End sequence number of most recently pushed export buffer
    private long m_lastPushedSeqNo = 0L;
    // Relinquish export master after this sequence number
    private long m_seqNoToDrain = Long.MAX_VALUE;
    //This is released when all mailboxes are set.
    private final Semaphore m_allowAcceptingMastership = new Semaphore(0);
    // This EDS is export master when the flag set to true
    private volatile AtomicBoolean m_mastershipAccepted = new AtomicBoolean(false);
    // This is set when mastership is going to transfer to another node.
    private Integer m_newLeaderHostId = null;
    // Sender HSId to query response map
    private Map<Long, QueryResponse> m_queryResponses = new HashMap<>();

    private volatile boolean m_closed = false;
    private final StreamBlockQueue m_committedBuffers;
    private Runnable m_onMastership;
    // m_pollFuture is used for a common case to improve efficiency, export decoder thread creates
    // future and passes to EDS executor thread, if EDS executor has no new buffer to poll, the future
    // is assigned to m_pollFuture. When site thread pushes buffer to EDS executor thread, m_pollFuture
    // is reused to notify export decoder to stop waiting.
    private SettableFuture<AckingContainer> m_pollFuture;
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
    // It is used to filter stale message responses
    private long m_currentRequestId = 0L;
    // *Generation Id* is actually a timestamp generated during catalog update(UpdateApplicationBase.java)
    // genId in this class represents the genId of the most recent pushed buffer. If a new buffer contains
    // different genId than the previous value, the new buffer needs to be written to new PBD segment.
    //
    private long m_previousGenId;

    private ExportSequenceNumberTracker m_gapTracker = new ExportSequenceNumberTracker();

    public final ArrayList<String> m_columnNames = new ArrayList<>();
    public final ArrayList<Integer> m_columnTypes = new ArrayList<>();
    public final ArrayList<Integer> m_columnLengths = new ArrayList<>();
    private String m_partitionColumnName = "";

    private static final boolean DISABLE_AUTO_GAP_RELEASE = Boolean.getBoolean("DISABLE_AUTO_GAP_RELEASE");

    private static final String VOLT_TRANSACTION_ID = "VOLT_TRANSACTION_ID";
    private static final String VOLT_EXPORT_TIMESTAMP = "VOLT_EXPORT_TIMESTAMP";
    private static final String VOLT_EXPORT_SEQUENCE_NUMBER = "VOLT_EXPORT_SEQUENCE_NUMBER";
    private static final String VOLT_PARTITION_ID = "VOLT_PARTITION_ID";
    private static final String VOLT_SITE_ID = "VOLT_SITE_ID";
    private static final String VOLT_EXPORT_OPERATION = "VOLT_EXPORT_OPERATION";

    static enum StreamStatus {
        ACTIVE,
        DROPPED,
        BLOCKED
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

    /**
     * Create a new data source.
     * @param db
     * @param tableName
     * @param partitionId
     */
    public ExportDataSource(
            Generation generation,
            ExportDataProcessor processor,
            String db,
            String tableName,
            int partitionId,
            int siteId,
            long genId,
            String signature,
            CatalogMap<Column> catalogMap,
            Column partitionColumn,
            String overflowPath
            ) throws IOException
    {
        m_previousGenId = genId;
        m_generation = generation;
        m_format = ExportFormat.SEVENDOTX;
        m_database = db;
        m_tableName = tableName;
        m_signature = signature;
        m_signatureBytes = m_signature.getBytes(StandardCharsets.UTF_8);

        PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(m_signatureBytes);
        String nonce = m_tableName + "_" + crc.getValue() + "_" + partitionId;

        m_committedBuffers = new StreamBlockQueue(overflowPath, nonce, m_tableName);
        m_gapTracker = m_committedBuffers.scanForGap();
        // Pretend it's rejoin so we set first unpolled to a safe place
        resetStateInRejoinOrRecover(0L, true);

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

        // Add the Export meta-data columns to the schema followed by the
        // catalog columns for this table.
        m_columnNames.add("VOLT_TRANSACTION_ID");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));
        m_columnLengths.add(8);

        m_columnNames.add("VOLT_EXPORT_TIMESTAMP");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));
        m_columnLengths.add(8);

        m_columnNames.add("VOLT_EXPORT_SEQUENCE_NUMBER");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));
        m_columnLengths.add(8);

        m_columnNames.add("VOLT_PARTITION_ID");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));
        m_columnLengths.add(8);

        m_columnNames.add("VOLT_SITE_ID");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));
        m_columnLengths.add(8);

        m_columnNames.add("VOLT_EXPORT_OPERATION");
        m_columnTypes.add(((int)VoltType.TINYINT.getValue()));
        m_columnLengths.add(1);

        for (Column c : CatalogUtil.getSortedCatalogItems(catalogMap, "index")) {
            m_columnNames.add(c.getName());
            m_columnTypes.add(c.getType());
            m_columnLengths.add(c.getSize());
        }

        if (partitionColumn != null) {
            m_partitionColumnName = partitionColumn.getName();
        }

        m_adFile = new VoltFile(overflowPath, nonce + ".ad");
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Creating ad for " + nonce);
        }
        byte jsonBytes[] = null;
        try {
            JSONStringer stringer = new JSONStringer();
            stringer.object();
            stringer.keySymbolValuePair("database", m_database);
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
            m_runEveryWhere = m_client.isRunEverywhere();
            if (exportLog.isDebugEnabled() && m_runEveryWhere) {
                exportLog.debug(toString() + " is a replicated export stream");
            }
        }
        m_es = CoreUtils.getListeningExecutorService("ExportDataSource for table " +
                    m_tableName + " partition " + m_partitionId, 1);
    }

    public ExportDataSource(Generation generation, File adFile,
            List<Pair<Integer, Integer>> localPartitionsToSites,
            final ExportDataProcessor processor,
            final long genId) throws IOException {
        m_previousGenId = genId;
        m_generation = generation;
        m_adFile = adFile;
        String overflowPath = adFile.getParent();
        byte data[] = Files.toByteArray(adFile);
        try {
            JSONObject jsObj = new JSONObject(new String(data, StandardCharsets.UTF_8));

            long version = jsObj.getLong("adVersion");
            if (version != SEVENX_AD_VERSION) {
                throw new IOException("Unsupported ad file version " + version);
            }
            m_database = jsObj.getString("database");
            m_partitionId = jsObj.getInt("partitionId");
            // SiteId is outside the valid range if it is no longer local
            int partitionsLocalSite = MpInitiator.MP_INIT_PID + 1;
            if (localPartitionsToSites != null) {
                for (Pair<Integer, Integer> partition : localPartitionsToSites) {
                    if (partition.getFirst() == m_partitionId) {
                        partitionsLocalSite = partition.getSecond();
                        break;
                    }
                }
            }
            m_siteId = partitionsLocalSite;
            m_signature = jsObj.getString("signature");
            m_signatureBytes = m_signature.getBytes(StandardCharsets.UTF_8);
            m_tableName = jsObj.getString("tableName");
            JSONArray columns = jsObj.getJSONArray("columns");
            for (int ii = 0; ii < columns.length(); ii++) {
                JSONObject column = columns.getJSONObject(ii);
                m_columnNames.add(column.getString("name"));
                int columnType = column.getInt("type");
                m_columnTypes.add(columnType);
                m_columnLengths.add(column.getInt("length"));
            }

            if (jsObj.has("format")) {
                m_format = ExportFormat.valueOf(jsObj.getString("format"));
            } else {
                m_format = ExportFormat.SEVENDOTX;
            }

            try {
                m_partitionColumnName = jsObj.getString("partitionColumnName");
            } catch (Exception ex) {
                //Ignore these if we have a OLD ad file these may not exist.
            }
        } catch (JSONException e) {
            throw new IOException(e);
        }

        PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(m_signatureBytes);
        final String nonce = m_tableName + "_" + crc.getValue() + "_" + m_partitionId;
        m_committedBuffers = new StreamBlockQueue(overflowPath, nonce, m_tableName);
        m_gapTracker = m_committedBuffers.scanForGap();
         // Pretend it's rejoin so we set first unpolled to a safe place
        resetStateInRejoinOrRecover(0L, true);
        if (exportLog.isDebugEnabled()) {
            exportLog.debug(toString() + " at AD file reads gap tracker from PBD:" + m_gapTracker.toString());
        }
        //EDS created from adfile is always from disk.
        m_isInCatalog = false;
        m_client = processor.getExportClient(m_tableName);
        if (m_client != null) {
            m_runEveryWhere = m_client.isRunEverywhere();
            if (exportLog.isDebugEnabled() && m_runEveryWhere) {
                exportLog.debug(toString() + " is a replicated export stream");
            }
        }
        m_es = CoreUtils.getListeningExecutorService("ExportDataSource for table " +
                m_tableName + " partition " + m_partitionId, 1);
    }

    public void setReadyForPolling(boolean readyForPolling) {
        m_readyForPolling = readyForPolling;
    }

    public void markInCatalog() {
        m_isInCatalog = true;
    }

    public boolean inCatalog() {
        return m_isInCatalog;
    }

    public synchronized void updateAckMailboxes(final Pair<Mailbox, ImmutableList<Long>> ackMailboxes) {
        //export stream for run-everywhere clients doesn't need ack mailboxes
        if (m_runEveryWhere) {
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
        //TODO precondition?
        m_exportTargetName = client.getTargetName();
        m_client = client;
    }

    public ExportClientBase getClient() {
        return m_client;
    }

    private synchronized void releaseExportBytes(long releaseSeqNo) throws IOException {
        // Released offset is in an already-released past
        if (releaseSeqNo < m_lastReleasedSeqNo) {
            return;
        }
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
        if (m_status == StreamStatus.BLOCKED &&
                m_gapTracker.getFirstGap() != null &&
                releaseSeqNo >= m_gapTracker.getFirstGap().getSecond()) {
            exportLog.info("Export queue gap resolved. Resuming export for " + ExportDataSource.this.toString());
            setStatus(StreamStatus.ACTIVE);
            m_queueGap = 0;
        }
        m_lastReleasedSeqNo = releaseSeqNo;
        int tuplesDeleted = m_gapTracker.truncate(releaseSeqNo);
        m_tuplesPending.addAndGet(-tuplesDeleted);
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

    public String getDatabase() {
        return m_database;
    }

    public String getTableName() {
        return m_tableName;
    }

    public String getSignature() {
        return m_signature;
    }

    public final int getPartitionId() {
        return m_partitionId;
    }

    public String getPartitionColumnName() {
        return m_partitionColumnName;
    }

    public long getGeneration() {
        return 0L;
    }

    public final void writeAdvertisementTo(JSONStringer stringer) throws JSONException {
        stringer.keySymbolValuePair("adVersion", SEVENX_AD_VERSION);
        stringer.keySymbolValuePair("partitionId", getPartitionId());
        stringer.keySymbolValuePair("signature", m_signature);
        stringer.keySymbolValuePair("tableName", getTableName());
        stringer.keySymbolValuePair("startTime", ManagementFactory.getRuntimeMXBean().getStartTime());
        stringer.key("columns").array();
        for (int ii=0; ii < m_columnNames.size(); ++ii) {
            stringer.object();
            stringer.keySymbolValuePair("name", m_columnNames.get(ii));
            stringer.keySymbolValuePair("type", m_columnTypes.get(ii));
            stringer.keySymbolValuePair("length", m_columnLengths.get(ii));
            stringer.endObject();
        }
        stringer.endArray();
        stringer.keySymbolValuePair("format", ExportFormat.SEVENDOTX.toString());
        stringer.keySymbolValuePair("partitionColumnName", m_partitionColumnName);
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

        result = m_database.compareTo(o.m_database);
        if (result != 0) {
            return result;
        }

        result = m_tableName.compareTo(o.m_tableName);
        if (result != 0) {
            return result;
        }

        result = m_signature.compareTo(o.m_signature);
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
        result += m_database.hashCode();
        result += m_tableName.hashCode();
        result += m_signature.hashCode();
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
                    exportingRole = (m_mastershipAccepted.get() ? "TRUE" : "FALSE");
                }
                return new ExportStatsRow(m_partitionId, m_siteId, m_tableName, m_exportTargetName,
                        exportingRole, m_tupleCount, m_tuplesPending.get(),
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
            long genId,
            ByteBuffer buffer,
            boolean poll) throws Exception {
        final java.util.concurrent.atomic.AtomicBoolean deleted = new java.util.concurrent.atomic.AtomicBoolean(false);
        long lastSequenceNumber = calcEndSequenceNumber(startSequenceNumber, tupleCount);
        if (exportLog.isTraceEnabled()) {
            exportLog.trace("pushExportBufferImpl [" + startSequenceNumber + "," +
                    lastSequenceNumber + "], poll=" + poll);
        }
        if (buffer != null) {
            // header space along is 8 bytes
            assert (buffer.capacity() > StreamBlock.HEADER_SIZE);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            // Drop already acked buffer
            final BBContainer cont = DBBPool.wrapBB(buffer);
            if (isAcked(lastSequenceNumber)) {
                m_tupleCount += tupleCount;
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Dropping already acked buffer. " +
                            " Buffer info: [" + startSequenceNumber + "," + lastSequenceNumber + "] Size: " + tupleCount +
                            " last released seq: " + m_lastReleasedSeqNo);
                }
                cont.discard();
                return;
            }

            try {
                StreamBlock sb = new StreamBlock(
                        new BBContainer(buffer) {
                            @Override
                            public void discard() {
                                checkDoubleFree();
                                cont.discard();
                                deleted.set(true);
                            }
                        },
                        null,
                        startSequenceNumber,
                        committedSequenceNumber,
                        tupleCount, uniqueId, false);

                // Mark release sequence number to partially acked buffer.
                if (isAcked(sb.startSequenceNumber())) {
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Setting releaseSeqNo as " + m_lastReleasedSeqNo +
                                " for SB [" + sb.startSequenceNumber() + "," + sb.lastSequenceNumber() +
                                "] for partition " + m_partitionId);
                    }
                    sb.releaseTo(m_lastReleasedSeqNo);
                }
                long newTuples = m_gapTracker.addRange(sb.unreleasedSequenceNumber(), lastSequenceNumber);
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Append [" + sb.unreleasedSequenceNumber() + "," + lastSequenceNumber +"] to gap tracker.");
                }

                m_lastQueuedTimestamp = sb.getTimestamp();
                m_lastPushedSeqNo = lastSequenceNumber;
                m_tupleCount += newTuples;
                m_tuplesPending.addAndGet((int)newTuples);
                assert (genId >= m_previousGenId);
                // This serializer is used to write stream schema to pbd
                StreamTableSchemaSerializer ds = new StreamTableSchemaSerializer(
                        VoltDB.instance().getCatalogContext(), m_tableName);
                // check generation id change at every push to tell when to create new segment
                m_committedBuffers.offer(sb, ds, genId != m_previousGenId);
            } catch (IOException e) {
                VoltDB.crashLocalVoltDB("Unable to write to export overflow.", true, e);
            }
        }
        if (poll) {
            try {
                pollImpl(m_pollFuture);
            } catch (RejectedExecutionException ex) {
                //Its ok.
            }
        }
    }

    public void pushExportBuffer(
            final long startSequenceNumber,
            final long committedSequenceNumber,
            final int tupleCount,
            final long uniqueId,
            final long genId,
            final ByteBuffer buffer,
            final boolean sync) {
        try {
            m_bufferPushPermits.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (m_es.isShutdown()) {
            //If we are shutting down push it to PBD
            try {
                pushExportBufferImpl(startSequenceNumber, committedSequenceNumber,
                        tupleCount, uniqueId, genId, buffer, false);
            } catch (Throwable t) {
                VoltDB.crashLocalVoltDB("Error pushing export  buffer", true, t);
            } finally {
                m_bufferPushPermits.release();
            }
           return;
        }
        try {
            m_es.execute((new Runnable() {
                @Override
                public void run() {
                    try {
                        if (!m_es.isShutdown()) {
                            pushExportBufferImpl(startSequenceNumber, committedSequenceNumber,
                                    tupleCount, uniqueId, genId, buffer, m_readyForPolling);
                        }
                    } catch (Throwable t) {
                        VoltDB.crashLocalVoltDB("Error pushing export  buffer", true, t);
                    } finally {
                        m_bufferPushPermits.release();
                    }
                }
            }));
            if (sync) {
                try {
                    //Don't do a real sync, just write the in memory buffers
                    //to a file. Blocking snapshot will do the fsync
                    ListenableFuture<?> rslt = sync(true);
                    rslt.get();
                } catch (ExecutionException | InterruptedException e) {
                    // swallow the exception since IOException will perform a CrashLocal
                }
            }
        } catch (RejectedExecutionException rej) {
            m_bufferPushPermits.release();
            //We are shutting down very much rolling generation so dont passup for error reporting.
            exportLog.info("Error pushing export  buffer: ", rej);
        }
    }

    public ListenableFuture<?> truncateExportToSeqNo(boolean isRecover, boolean isRejoin, long sequenceNumber) {
        return m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    m_tupleCount = sequenceNumber;
                    if (isRecover) {
                        if (sequenceNumber < 0) {
                            exportLog.error("Snapshot does not include valid truncation point for partition " +
                                    m_partitionId);
                            return;
                        }
                        m_committedBuffers.truncateToSequenceNumber(sequenceNumber);
                        // export data after truncation point will be regenerated by c/l replay
                        m_gapTracker.truncateAfter(sequenceNumber);
                        if (exportLog.isDebugEnabled()) {
                            exportLog.debug("Truncating tracker via snapshot truncation to " + sequenceNumber +
                                    ", tracker map is " + m_gapTracker.toString());
                        }
                    }
                    // Need to update pending tuples in rejoin
                    resetStateInRejoinOrRecover(sequenceNumber, isRejoin);
                } catch (Throwable t) {
                    VoltDB.crashLocalVoltDB("Error while trying to truncate export to seq " +
                            sequenceNumber, true, t);
                }
            }
        });
    }

    private class SyncRunnable implements Runnable {
        private final boolean m_nofsync;
        SyncRunnable(final boolean nofsync) {
            this.m_nofsync = nofsync;
        }

        @Override
        public void run() {
            try {
                m_committedBuffers.sync(m_nofsync);
            } catch (IOException e) {
                VoltDB.crashLocalVoltDB("Unable to write to export overflow.", true, e);
            }
        }
    }

    public ListenableFuture<?> sync(final boolean nofsync) {
        return m_es.submit(new SyncRunnable(nofsync));
    }

    public boolean isClosed() {
        return m_closed;
    }

    public ListenableFuture<?> closeAndDelete() {
        m_closed = true;
        return m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    m_committedBuffers.closeAndDelete();
                    m_adFile.delete();
                    m_ackMailboxRefs.set(null);
                } catch(IOException e) {
                    exportLog.rateLimitedLog(60, Level.WARN, e, "Error closing commit buffers");
                } finally {
                    m_es.shutdown();
                }
            }
        });
    }

    public ListenableFuture<?> close() {
        m_closed = true;
        //If we are waiting at this allow to break out when close comes in.
        m_allowAcceptingMastership.release();
        return m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    m_committedBuffers.close();
                    m_ackMailboxRefs.set(null);
                } catch (IOException e) {
                    exportLog.error(e.getMessage(), e);
                } finally {
                    m_es.shutdown();
                }
            }
        });
    }

    // Needs to be thread-safe, EDS executor, export decoder and site thread both touch m_pendingContainer.
    public void setPendingContainer(AckingContainer container) {
        Preconditions.checkNotNull(m_pendingContainer.get() != null, "Pending container must be null.");
        m_pendingContainer.set(container);
    }

    public ListenableFuture<AckingContainer> poll() {
        final SettableFuture<AckingContainer> fut = SettableFuture.create();
        try {
            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    // ENG-14488, it's possible to have the export master gives up mastership
                    // but still try to poll immediately after that, e.g. from Pico Network
                    // thread the master gives up mastership, from decoder thread it tries to
                    // poll periodically, they won't overlap but poll can happen after giving up
                    // mastership. If it happens m_pollFuture can be mistakingly set, and when
                    // the old master retakes mastership again it refuses to export because
                    // m_pollFuture should be false on a fresh master.
                    //
                    // Add following check to eliminate this window.
                    if (!m_mastershipAccepted.get()) {
                        fut.set(null);
                        m_pollFuture = null;
                        return;
                    }

                    try {
                        //If we have anything pending set that before moving to next block.
                        if (m_pendingContainer.get() != null) {
                            fut.set(m_pendingContainer.getAndSet(null));
                            if (m_pollFuture != null) {
                                if (exportLog.isDebugEnabled()) {
                                    exportLog.debug("Pick up work from pending container, set poll future to null");
                                }
                                m_pollFuture = null;
                            }
                            return;
                        }
                        /*
                         * The poll is blocking through the cached future, shouldn't
                         * call poll a second time until a response has been given
                         * which satisfies the cached future.
                         */
                        if (m_pollFuture != null) {
                            fut.setException(new ReentrantPollException("Reentrant poll detected: InCat = " + m_isInCatalog +
                                    " In ExportDataSource for Table " + getTableName() + ", Partition " + getPartitionId()));
                            return;
                        }
                        if (!m_es.isShutdown()) {
                            pollImpl(fut);
                        }
                    } catch (Exception e) {
                        exportLog.error("Exception polling export buffer", e);
                    } catch (Error e) {
                        VoltDB.crashLocalVoltDB("Error polling export buffer", true, e);
                    }
                }
            });
        } catch (RejectedExecutionException rej) {
            //Don't expect this to happen outside of test, but in test it's harmless
            exportLog.info("Polling from export data source rejected, this should be harmless");
        }
        return fut;
    }

    private synchronized void pollImpl(SettableFuture<AckingContainer> fut) {
        if (fut == null) {
            return;
        }

        try {
            cleanupEmptySource();
            StreamBlock first_unpolled_block = null;
            //Assemble a list of blocks to delete so that they can be deleted
            //outside of the m_committedBuffers critical section
            ArrayList<StreamBlock> blocksToDelete = new ArrayList<>();
            //Inside this critical section do the work to find out
            //what block should be returned by the next poll.
            //Copying and sending the data will take place outside the critical section
            try {
                Iterator<StreamBlock> iter = m_committedBuffers.iterator();
                long firstUnpolledSeq = m_firstUnpolledSeqNo;
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("polling data from seqNo " + firstUnpolledSeq);
                }
                while (iter.hasNext()) {
                    StreamBlock block = iter.next();
                    // find the first block that has unpolled data
                    if (firstUnpolledSeq >= block.startSequenceNumber() &&
                            firstUnpolledSeq <= block.lastSequenceNumber()) {
                        first_unpolled_block = block;
                        m_firstUnpolledSeqNo = block.lastSequenceNumber() + 1;
                        break;
                    } else if (firstUnpolledSeq > block.lastSequenceNumber()) {
                        blocksToDelete.add(block);
                        iter.remove();
                        if (exportLog.isDebugEnabled()) {
                            exportLog.debug("pollImpl delete polled buffer [" + block.startSequenceNumber() + "," +
                                    block.lastSequenceNumber() + "]");
                        }
                    } else {
                        // Gap only exists in the middle of buffers, why is it never be in the head of
                        // queue? Because only master checks the gap, mastership migration waits until
                        // the last pushed buffer at the checkpoint time is acked, it won't leave gap
                        // behind before migrates to another node.
                        Pair<Long, Long> gap = m_gapTracker.getFirstGap();
                        // Hit a gap! Prepare to relinquish master role and broadcast queries for
                        // capable candidate.
                        if (gap != null && firstUnpolledSeq >= gap.getFirst() && firstUnpolledSeq <= gap.getSecond()) {
                            // If another mastership migration in progress and is before the gap,
                            // don't bother to start new one.
                            if (m_seqNoToDrain > firstUnpolledSeq - 1) {
                                exportLog.info("Export data missing from current queue [" + gap.getFirst() + ", " + gap.getSecond() +
                                        "] from " + this.toString() + ". Searching other sites for missing data.");
                                m_seqNoToDrain = firstUnpolledSeq - 1;
                                mastershipCheckpoint(firstUnpolledSeq - 1);
                            }
                            break;
                        }
                    }
                }
            } catch (RuntimeException e) {
                if (e.getCause() instanceof IOException) {
                    VoltDB.crashLocalVoltDB("Error attempting to find unpolled export data", true, e);
                } else {
                    throw e;
                }
            } finally {
                //Try hard not to leak memory
                for (StreamBlock sb : blocksToDelete) {
                    int tuplesDeleted = m_gapTracker.truncate(sb.lastSequenceNumber());
                    m_tuplesPending.addAndGet(-tuplesDeleted);
                    sb.discard();
                }
            }

            //If there are no unpolled blocks return the firstUnpolledUSO with no data
            if (first_unpolled_block == null) {
                m_pollFuture = fut;
            } else {
                // If stream was previously blocked by a gap, now it skips/fulfills the gap
                // change the status back to normal.
                if (m_status == StreamStatus.BLOCKED) {
                    exportLog.info("Export queue gap resolved. Resuming export for " + ExportDataSource.this.toString());
                    setStatus(StreamStatus.ACTIVE);
                    m_queueGap = 0;
                }
                final AckingContainer ackingContainer =
                        new AckingContainer(first_unpolled_block.unreleasedContainer(),
                                first_unpolled_block.getSchemaContainer(),
                                first_unpolled_block.startSequenceNumber() + first_unpolled_block.rowCount() - 1,
                                first_unpolled_block.committedSequenceNumber());
                try {
                    fut.set(ackingContainer);
                } catch (RejectedExecutionException reex) {
                    //We are closing source dont discard next processor will pick it up.
                }
                m_pollFuture = null;
            }
        } catch (Throwable t) {
            fut.setException(t);
        }
    }

    public class AckingContainer extends BBContainer {
        final long m_lastSeqNo;
        final long m_commitSeqNo;
        final BBContainer m_backingCont;
        final BBContainer m_schemaCont;
        long m_startTime = 0;

        public AckingContainer(BBContainer cont, BBContainer schemaCont, long seq, long commitSeq) {
            super(cont.b());
            m_lastSeqNo = seq;
            m_commitSeqNo = commitSeq;
            m_backingCont = cont;
            m_schemaCont = schemaCont;
        }

        public void updateStartTime(long startTime) {
            m_startTime = startTime;
        }

        public ByteBuffer schema() {
            if (m_schemaCont == null) {
                return null;
            }
            return m_schemaCont.b();
        }

        @Override
        public void discard() {
            checkDoubleFree();
            try {
                m_es.execute(new Runnable() {
                    @Override
                    public void run() {
                        if (exportLog.isTraceEnabled()) {
                            exportLog.trace("AckingContainer.discard with sequence number: " + m_lastSeqNo);
                        }
                        assert(m_startTime != 0);
                        long elapsedMS = System.currentTimeMillis() - m_startTime;
                        m_blocksSentSinceClear += 1;
                        m_totalLatencySinceClearInMS += elapsedMS;
                        m_averageLatency = m_totalLatencySinceClearInMS / m_blocksSentSinceClear;
                        if (m_averageLatency > m_maxLatency) {
                            m_maxLatency = m_averageLatency;
                        }

                        try {
                             m_backingCont.discard();
                             if (m_schemaCont != null) {
                                 m_schemaCont.discard();
                             }
                            try {
                                if (!m_es.isShutdown()) {
                                    setCommittedSeqNo(m_commitSeqNo);
                                    ackImpl(m_lastSeqNo);
                                }
                            } finally {
                                forwardAckToOtherReplicas();
                            }
                        } catch (Exception e) {
                            exportLog.error("Error acking export buffer", e);
                        } catch (Error e) {
                            VoltDB.crashLocalVoltDB("Error acking export buffer", true, e);
                        }
                    }
                });
            } catch (RejectedExecutionException rej) {
                  //Don't expect this to happen outside of test, but in test it's harmless
                  exportLog.info("Acking export data task rejected, this should be harmless");
                  m_backingCont.discard();
                  m_schemaCont.discard();
            }
        }
    }

    public void forwardAckToOtherReplicas() {
        // In RunEveryWhere mode, every data source is master, no need to send out acks.
        if (m_runEveryWhere) {
            return;
        }
        Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
        Mailbox mbx = p.getFirst();
        if (mbx != null && p.getSecond().size() > 0) {
            // msgType:byte(1) + partition:int(4) + length:int(4) +
            // signaturesBytes.length + ackUSO:long(8).
            final int msgLen = 1 + 4 + 4 + m_signatureBytes.length + 8;

            ByteBuffer buf = ByteBuffer.allocate(msgLen);
            buf.put(ExportManager.RELEASE_BUFFER);
            buf.putInt(m_partitionId);
            buf.putInt(m_signatureBytes.length);
            buf.put(m_signatureBytes);
            buf.putLong(m_committedSeqNo);

            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());

            for( Long siteId: p.getSecond()) {
                mbx.send(siteId, bpm);
            }
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Send RELEASE_BUFFER to " + toString()
                        + " with sequence number " + m_committedSeqNo
                        + " from " + CoreUtils.hsIdToString(mbx.getHSId())
                        + " to " + CoreUtils.hsIdCollectionToString(p.getSecond()));
            }
        }
    }

    // In case of newly joined or rejoined streams miss any RELEASE_BUFFER event,
    // master stream resend the event when the export mailbox is aware of new streams.
    public void forwardAckToNewJoinedReplicas(Set<Long> newReplicas) {
        // In RunEveryWhere mode, every data source is master, no need to send out acks.
        if (!m_mastershipAccepted.get() || m_runEveryWhere) {
            return;
        }

        m_es.submit(new Runnable() {
            @Override
            public void run() {
                Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
                Mailbox mbx = p.getFirst();
                if (mbx != null && newReplicas.size() > 0) {
                 // msg type(1) + partition:int(4) + length:int(4) +
                    // signaturesBytes.length + ackUSO:long(8).
                    final int msgLen = 1 + 4 + 4 + m_signatureBytes.length + 8;

                    ByteBuffer buf = ByteBuffer.allocate(msgLen);
                    buf.put(ExportManager.RELEASE_BUFFER);
                    buf.putInt(m_partitionId);
                    buf.putInt(m_signatureBytes.length);
                    buf.put(m_signatureBytes);
                    buf.putLong(m_committedSeqNo);

                    BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());

                    for( Long siteId: newReplicas) {
                        mbx.send(siteId, bpm);
                    }
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Send RELEASE_BUFFER to " + toString()
                                + " with sequence number " + m_committedSeqNo
                                + " from " + CoreUtils.hsIdToString(mbx.getHSId())
                                + " to " + CoreUtils.hsIdCollectionToString(p.getSecond()));
                    }
                }
            }
        });
    }

    /**
     * Entry point for receiving acknowledgments from remote entities.
     *
     * @param seq acknowledged sequence number, ALWAYS last row of a transaction, or 0.
     */
    public void remoteAck(final long seq) {

        //In replicated only master will be doing this.
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    // ENG-12282: A race condition between export data source
                    // master promotion and getting acks from the previous
                    // failed master can occur. The failed master could have
                    // sent out an ack with Long.MIN and fails immediately after
                    // that, which causes a new master to be elected. The
                    // election and the receiving of this ack message happens on
                    // two different threads on the new master. If it's promoted
                    // while processing the ack, the ack may call `m_onDrain`
                    // while the other thread is polling buffers, which may
                    // never get discarded.
                    //
                    // Now that we are on the same thread, check to see if we
                    // are already promoted to be the master. If so, ignore the
                    // ack.
                    if (!m_es.isShutdown() && !m_mastershipAccepted.get()) {
                        setCommittedSeqNo(seq);
                        ackImpl(seq);

                        // FIXME: remove
                        if (m_lastReleasedSeqNo != m_committedSeqNo) {
                            exportLog.info("XXX ack mismatch - released " + m_lastReleasedSeqNo
                                    + ", committed " + m_committedSeqNo);
                        } else {
                            exportLog.info("YYY ack match - released " + m_lastReleasedSeqNo
                                    + ", committed " + m_committedSeqNo);
                        }
                    }
                } catch (Exception e) {
                    exportLog.error("Error acking export buffer", e);
                } catch (Error e) {
                    VoltDB.crashLocalVoltDB("Error acking export buffer", true, e);
                }
            }
        });
    }

     private void ackImpl(long seq) {
        //Process the ack if any and add blocks to the delete list or move the released sequence number
        if (seq > 0) {
            try {
                releaseExportBytes(seq);
                cleanupEmptySource();
                mastershipCheckpoint(seq);
            } catch (IOException e) {
                VoltDB.crashLocalVoltDB("Error attempting to release export bytes", true, e);
                return;
            }
        }
    }

    // Discard dangling or dropped export data source when the buffer is empty
    private void cleanupEmptySource() throws IOException {
        if ((!this.m_isInCatalog || m_status == StreamStatus.DROPPED) && m_committedBuffers.isEmpty()) {
            //Returning null indicates end of stream
            try {
                if (m_pollFuture != null) {
                    m_pollFuture.set(null);
                }
            } catch (RejectedExecutionException reex) {
                //We are closing source.
            }
            m_pollFuture = null;
            m_generation.onSourceDone(m_partitionId, m_signature);
            return;
        }
    }

    /**
     * indicate the partition leader has been migrated away
     * prepare to give up the mastership,
     * has to drain existing PBD and then notify new leaders (through ack)
     */
    void prepareTransferMastership(int newLeaderHostId) {
        if (!m_mastershipAccepted.get()) {
            return;
        }
        m_es.submit(new Runnable() {
            @Override
            public void run() {
                // memorize end sequence number of the most recently pushed buffer from EE
                // but if we already wait to switch mastership, don't update the drain-to
                // sequence number to a greater number
                m_seqNoToDrain = Math.min(m_seqNoToDrain, m_lastPushedSeqNo);
                m_newLeaderHostId = newLeaderHostId;
                // if no new buffer to be drained, send the migrate event right away
                mastershipCheckpoint(m_lastReleasedSeqNo);
            }
        });
    }

    private void sendGiveMastershipMessage(int newLeaderHostId) {
        if (m_runEveryWhere) {
            return;
        }
        Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
        Mailbox mbx = p.getFirst();
        if (mbx != null && p.getSecond().size() > 0 ) {
            // msg type(1) + partition:int(4) + length:int(4) +
            // signaturesBytes.length + curSeq:long(8).
            final int msgLen = 1 + 4 + 4 + m_signatureBytes.length + 8;
            ByteBuffer buf = ByteBuffer.allocate(msgLen);
            buf.put(ExportManager.GIVE_MASTERSHIP);
            buf.putInt(m_partitionId);
            buf.putInt(m_signatureBytes.length);
            buf.put(m_signatureBytes);
            buf.putLong(m_committedSeqNo);

            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());

            for(Long siteId: p.getSecond()) {
                // Just send to the ack mailbox on the new master
                if (CoreUtils.getHostIdFromHSId(siteId) == newLeaderHostId) {
                    mbx.send(siteId, bpm);
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug(toString() + " send GIVE_MASTERSHIP message to " +
                                CoreUtils.hsIdToString(siteId)
                                + " with sequence number " + m_committedSeqNo);
                    }
                    break;
                }
            }
        }
        unacceptMastership();
    }

    private void sendTakeMastershipMessage() {
        m_queryResponses.clear();
        Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
        Mailbox mbx = p.getFirst();
        m_currentRequestId = System.nanoTime();
        if (mbx != null && p.getSecond().size() > 0) {
            // msg type(1) + partition:int(4) + length:int(4) + signaturesBytes.length
            // requestId(8)
            final int msgLen = 1 + 4 + 4 + m_signatureBytes.length + 8;
            ByteBuffer buf = ByteBuffer.allocate(msgLen);
            buf.put(ExportManager.TAKE_MASTERSHIP);
            buf.putInt(m_partitionId);
            buf.putInt(m_signatureBytes.length);
            buf.put(m_signatureBytes);
            buf.putLong(m_currentRequestId);
            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());
            for( Long siteId: p.getSecond()) {
                mbx.send(siteId, bpm);
            }
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Send TAKE_MASTERSHIP message(" + m_currentRequestId +
                        ") for partition " + m_partitionId + " source signature " + m_tableName +
                        " from " + CoreUtils.hsIdToString(mbx.getHSId()) +
                        " to " + CoreUtils.hsIdCollectionToString(p.getSecond()));
            }
        } else {
            // There is no other replica, promote myself.
            acceptMastership();
        }
    }

    private void sendQueryResponse(long senderHSId, long requestId, long lastSeq) {
        Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
        Mailbox mbx = p.getFirst();
        if (mbx != null) {
            // msg type(1) + partition:int(4) + length:int(4) + signaturesBytes.length
            // requestId(8) + lastSeq(8)
            int msgLen = 1 + 4 + 4 + m_signatureBytes.length + 8 + 8;
            ByteBuffer buf = ByteBuffer.allocate(msgLen);
            buf.put(ExportManager.QUERY_RESPONSE);
            buf.putInt(m_partitionId);
            buf.putInt(m_signatureBytes.length);
            buf.put(m_signatureBytes);
            buf.putLong(requestId);
            buf.putLong(lastSeq);
            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());
            mbx.send(senderHSId, bpm);
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Partition " + m_partitionId + " mailbox hsid (" +
                        CoreUtils.hsIdToString(mbx.getHSId()) + ") send QUERY_RESPONSE message(" +
                        requestId + "," + lastSeq + ") to " + CoreUtils.hsIdToString(senderHSId));
            }
        }
    }

    public synchronized void unacceptMastership() {
        if (exportLog.isDebugEnabled()) {
            exportLog.debug(toString() + " is no longer the export stream master.");
        }
        m_mastershipAccepted.set(false);
        m_pollFuture = null;
        m_readyForPolling = false;
        m_seqNoToDrain = Long.MAX_VALUE;
        m_newLeaderHostId = null;
    }

    /**
     * Trigger an execution of the mastership runnable by the associated
     * executor service
     */
    public synchronized void acceptMastership() {
        if (m_onMastership == null) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Mastership Runnable not yet set for table " + getTableName() + " partition " + getPartitionId());
            }
            return;
        }
        if (m_mastershipAccepted.get()) {
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Export table " + getTableName() + " mastership already accepted for partition " + getPartitionId());
            }
            return;
        }
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!m_es.isShutdown() || !m_closed) {
                        if (exportLog.isDebugEnabled()) {
                            exportLog.debug("Export table " + getTableName() + " accepting mastership for partition " + getPartitionId());
                        }
                        if (m_mastershipAccepted.compareAndSet(false, true)) {
                            // Either get enough responses or have received TRANSFER_MASTER event, clear the response sender HSids.
                            m_queryResponses.clear();
                            m_onMastership.run();
                        }
                    }
                } catch (Exception e) {
                    exportLog.error("Error in accepting mastership", e);
                }
            }
        });
    }

    /**
     * set the runnable task that is to be executed on mastership designation
     *
     * @param toBeRunOnMastership a {@link @Runnable} task
     * @param runEveryWhere       Set if connector "replicated" property is set to true Like replicated table, every
     *                            replicated export stream is its own master.
     */
    public void setOnMastership(Runnable toBeRunOnMastership) {
        Preconditions.checkNotNull(toBeRunOnMastership, "mastership runnable is null");
        m_onMastership = toBeRunOnMastership;
        // If connector "replicated" property is set to true then every
        // replicated export stream is its own master
        if (m_runEveryWhere) {
            //export stream for run-everywhere clients doesn't need ack mailbox
            m_ackMailboxRefs.set(null);
            acceptMastership();
        }
    }

    public void setRunEveryWhere(boolean runEveryWhere) {
        if (exportLog.isDebugEnabled() && runEveryWhere != m_runEveryWhere) {
            exportLog.debug("Change " + toString() + " to " +
                    (runEveryWhere ? "replicated stream" : " non-replicated stream"));
        }
        m_runEveryWhere = runEveryWhere;
    }

    public ExportFormat getExportFormat() {
        return m_format;
    }

    public ListeningExecutorService getExecutorService() {
        return m_es;
    }

    private void sendGapQuery() {

        // jump over a gap for run everywhere
        if (m_runEveryWhere) {
            // It's unlikely but thinking switch regular stream to replicated stream on the fly.
            if (m_gapTracker.getFirstGap() != null) {
                m_firstUnpolledSeqNo = m_gapTracker.getFirstGap().getSecond() + 1;
                exportLog.info(toString() + " skipped stream gap because it's a replicated stream, " +
                        "setting next poll sequence number to " + m_firstUnpolledSeqNo);
            }
            m_queueGap = 0;
            return;
        }

        if (m_mastershipAccepted.get() &&  /* active stream */
                !m_gapTracker.isEmpty() &&  /* finish initialization */
                m_firstUnpolledSeqNo > m_gapTracker.getSafePoint()) { /* may hit a gap */
            m_queryResponses.clear();
            Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
            Mailbox mbx = p.getFirst();
            m_currentRequestId = System.nanoTime();
            if (mbx != null && p.getSecond().size() > 0) {
                // msg type(1) + partition:int(4) + length:int(4) + signaturesBytes.length
                // requestId(8) + gapStart(8)
                final int msgLen = 1 + 4 + 4 + m_signatureBytes.length + 8 + 8;
                ByteBuffer buf = ByteBuffer.allocate(msgLen);
                buf.put(ExportManager.GAP_QUERY);
                buf.putInt(m_partitionId);
                buf.putInt(m_signatureBytes.length);
                buf.put(m_signatureBytes);
                buf.putLong(m_currentRequestId);
                buf.putLong(m_gapTracker.getSafePoint() + 1);
                BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());
                for( Long siteId: p.getSecond()) {
                    mbx.send(siteId, bpm);
                }
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Send GAP_QUERY message(" + m_currentRequestId + "," + (m_gapTracker.getSafePoint() + 1) +
                            ") from " + CoreUtils.hsIdToString(mbx.getHSId()) +
                            " to " + CoreUtils.hsIdCollectionToString(p.getSecond()));
                }
            } else {
                setStatus(StreamStatus.BLOCKED);
                Pair<Long, Long> gap = m_gapTracker.getFirstGap();
                m_queueGap = gap.getSecond() - gap.getFirst() + 1;
                exportLog.warn("Export is blocked, missing [" + gap.getFirst() + ", " + gap.getSecond() + "] from " +
                        this.toString() + ". Please rejoin a node with the missing export queue data. ");
            }
        }
    }

    public void queryForBestCandidate() {
        if (m_runEveryWhere) {
            return;
        }
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                sendGapQuery();
            }
        });
    }

    void takeMastership() {
        // Skip current master or in run everywhere mode
        if (m_mastershipAccepted.get() || m_runEveryWhere) {
            return;
        }
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                if (m_mastershipAccepted.get() || m_runEveryWhere) {
                    return;
                }
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug(ExportDataSource.this.toString() + " is going to export data because partition leader is on current node.");
                }
                // Query export membership if current stream is not the master
                sendTakeMastershipMessage();
            }
        });
    }

    // Query whether a master exists for the given partition, if not try to promote the local data source.
    public void handleQueryMessage(final long senderHSId, long requestId, long gapStart) {
            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    long lastSeq = Long.MIN_VALUE;
                    Pair<Long, Long> range = m_gapTracker.getRangeContaining(gapStart);
                    if (range != null) {
                        lastSeq = range.getSecond();
                    }
                    sendQueryResponse(senderHSId, requestId, lastSeq);
                }
            });
    }

    public void handleQueryResponse(long sendHsId, long requestId, long lastSeq) {
        if (m_currentRequestId == requestId && m_mastershipAccepted.get()) {
            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    m_queryResponses.put(sendHsId, new QueryResponse(lastSeq));
                    Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
                    if (p.getSecond().stream().allMatch(hsid -> m_queryResponses.containsKey(hsid))) {
                        List<Entry<Long, QueryResponse>> candidates =
                                m_queryResponses.entrySet().stream()
                                       .filter(s -> s.getValue().canCoverGap())
                                       .collect(Collectors.toList());
                        Entry<Long, QueryResponse> bestCandidate = null;
                        for (Entry<Long, QueryResponse> candidate : candidates) {
                            if (bestCandidate == null) {
                                bestCandidate = candidate;
                            } else if (candidate.getValue().lastSeq > bestCandidate.getValue().lastSeq) {
                                bestCandidate = candidate;
                            }
                        }
                        if (bestCandidate == null) {
                            // if current stream doesn't hit gap, just leave it as is.
                            Pair<Long, Long> gap = m_gapTracker.getFirstGap();
                            if (gap == null || m_firstUnpolledSeqNo < gap.getFirst()) {
                                return;
                            }
                            setStatus(StreamStatus.BLOCKED);
                            m_queueGap = gap.getSecond() - gap.getFirst() + 1;
                            RealVoltDB voltdb = (RealVoltDB)VoltDB.instance();
                            if (voltdb.isClusterComplete()) {
                                if (DISABLE_AUTO_GAP_RELEASE) {
                                    // Show warning only in full cluster.
                                    exportLog.warn("Export is blocked, missing [" + gap.getFirst() + ", " + gap.getSecond() + "] from " +
                                            ExportDataSource.this.toString() + ". Please rejoin a node with the missing export queue data or " +
                                            "use 'voltadmin export release' command to skip the missing data.");
                                } else {
                                    processStreamControl(OperationMode.RELEASE);
                                }
                            }
                        } else {
                            // time to give up master and give it to the best candidate
                            m_newLeaderHostId = CoreUtils.getHostIdFromHSId(bestCandidate.getKey());
                            exportLog.info("Export queue gap resolved. Resuming export for " + ExportDataSource.this.toString() + " on host " + m_newLeaderHostId);
                            // drainedTo sequence number should haven't been changed.
                            mastershipCheckpoint(m_lastReleasedSeqNo);
                        }
                    }
                }
            });
        }
    }

    public void handleTakeMastershipMessage(long senderHsId, long requestId) {
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                if (m_mastershipAccepted.get()) {
                    m_newLeaderHostId = CoreUtils.getHostIdFromHSId(senderHsId);
                    // mark the trigger
                    m_seqNoToDrain = Math.min(m_seqNoToDrain, m_lastPushedSeqNo);
                    mastershipCheckpoint(m_lastReleasedSeqNo);
                } else {
                    sendTakeMastershipResponse(senderHsId, requestId);
                }
            }
        });
    }

    public void sendTakeMastershipResponse(long senderHsId, long requestId) {
        Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
        Mailbox mbx = p.getFirst();
        if (mbx != null) {
            // msg type(1) + partition:int(4) + length:int(4) + signaturesBytes.length
            // requestId(8)
            int msgLen = 1 + 4 + 4 + m_signatureBytes.length + 8;
            ByteBuffer buf = ByteBuffer.allocate(msgLen);
            buf.put(ExportManager.TAKE_MASTERSHIP_RESPONSE);
            buf.putInt(m_partitionId);
            buf.putInt(m_signatureBytes.length);
            buf.put(m_signatureBytes);
            buf.putLong(requestId);
            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());
            mbx.send(senderHsId, bpm);
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Partition " + m_partitionId + " mailbox hsid (" +
                        CoreUtils.hsIdToString(mbx.getHSId()) +
                        ") send TAKE_MASTERSHIP_RESPONSE message(" +
                        requestId + ") to " + CoreUtils.hsIdToString(senderHsId));
            }
        }
    }

    public void handleTakeMastershipResponse(long sendHsId, long requestId) {
        if (m_currentRequestId == requestId && !m_mastershipAccepted.get()) {
            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    m_queryResponses.put(sendHsId, null);
                    Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
                    if (p.getSecond().stream().allMatch(hsid -> m_queryResponses.containsKey(hsid))) {
                        acceptMastership();
                    }
                }
            });
        }
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
        return "ExportDataSource for table " + getTableName() + " partition " + getPartitionId()
           + " (" + m_status + ", " + (m_mastershipAccepted.get() ? "Master":"Replica") + ")";
    }

    private void mastershipCheckpoint(long seq) {
        if (m_runEveryWhere) {
            return;
        }
        if (exportLog.isTraceEnabled()) {
            exportLog.trace("Export table " + getTableName() + " mastership checkpoint "  +
                    " m_newLeaderHostId " + m_newLeaderHostId + " m_seqNoToDrain " + m_seqNoToDrain +
                    " m_lastReleasedSeqNo " + m_lastReleasedSeqNo + " m_committedSeqNo " + m_committedSeqNo +
                    " m_lastPushedSeqNo " + m_lastPushedSeqNo);
        }
        // time to give away leadership
        if (seq >= m_seqNoToDrain) {
            if (m_newLeaderHostId != null) {
                sendGiveMastershipMessage(m_newLeaderHostId);
            } else {
                sendGapQuery();
            }
        }
    }

    // During rejoin it's possible that the stream is blocked by a gap before the export
    // sequence number carried by rejoin snapshot, so we couldn't trust the sequence number
    // in snapshot to find where to poll next buffer. The right thing to do should be setting
    // the firstUnpolled to a safe point in case of releasing a gap prematurely, waits for
    // current master to tell us where to poll next buffer.
    private void resetStateInRejoinOrRecover(long initialSequenceNumber, boolean isRejoin) {
        if (isRejoin) {
            if (!m_gapTracker.isEmpty()) {
                m_lastReleasedSeqNo = Math.max(m_lastReleasedSeqNo, m_gapTracker.getFirstSeqNo() - 1);
            }
        } else {
            m_lastReleasedSeqNo = Math.max(m_lastReleasedSeqNo, initialSequenceNumber);
        }
        m_firstUnpolledSeqNo =  m_lastReleasedSeqNo + 1;
        m_tuplesPending.set(m_gapTracker.sizeInSequence());
    }

    public String getTarget() {
        return m_exportTargetName;
    }

    public synchronized boolean processStreamControl(OperationMode operation) {
        switch (operation) {
        case RELEASE:
            if (m_status == StreamStatus.BLOCKED && m_mastershipAccepted.get() && m_gapTracker.getFirstGap() != null) {
                long firstUnpolledSeqNo = m_gapTracker.getFirstGap().getSecond() + 1;
                exportLog.warn("Export data is missing [" + m_gapTracker.getFirstGap().getFirst() + ", " + m_gapTracker.getFirstGap().getSecond() +
                        "] and cluster is complete. Skipping to next available transaction for " + this.toString());
                m_firstUnpolledSeqNo = firstUnpolledSeqNo;
                setStatus(StreamStatus.ACTIVE);
                m_queueGap = 0;
                return true;
            }
            break;
        default:
            // should not happen since the operation is verified prior to this call
        }
        return false;
    }


    private void setCommittedSeqNo(long committedSeqNo) {
        if (committedSeqNo == NULL_COMMITTED_SEQNO) return;
        if (committedSeqNo > m_committedSeqNo) {
            m_committedSeqNo  = committedSeqNo;
        }
    }

    public static class StreamTableSchemaSerializer implements DeferredSerialization {
        private final CatalogContext m_catalogContext;
        private final String m_streamName;
        public StreamTableSchemaSerializer(CatalogContext catalogContext, String streamName) {
            m_catalogContext = catalogContext;
            m_streamName = streamName;
        }

        public static void writeMetaColumns(ByteBuffer buf) {
            // VOLT_TRANSACTION_ID, VoltType.BIGINT
            buf.putInt(VOLT_TRANSACTION_ID.length());
            buf.put(VOLT_TRANSACTION_ID.getBytes(Constants.UTF8ENCODING));
            buf.put(VoltType.BIGINT.getValue());
            buf.putInt(Long.BYTES);

            // VOLT_EXPORT_TIMESTAMP, VoltType.BIGINT
            buf.putInt(VOLT_EXPORT_TIMESTAMP.length());
            buf.put(VOLT_EXPORT_TIMESTAMP.getBytes(Constants.UTF8ENCODING));
            buf.put(VoltType.BIGINT.getValue());
            buf.putInt(Long.BYTES);

            // VOLT_EXPORT_SEQUENCE_NUMBER, VoltType.BIGINT
            buf.putInt(VOLT_EXPORT_SEQUENCE_NUMBER.length());
            buf.put(VOLT_EXPORT_SEQUENCE_NUMBER.getBytes(Constants.UTF8ENCODING));
            buf.put(VoltType.BIGINT.getValue());
            buf.putInt(Long.BYTES);

            // VOLT_PARTITION_ID, VoltType.BIGINT
            buf.putInt(VOLT_PARTITION_ID.length());
            buf.put(VOLT_PARTITION_ID.getBytes(Constants.UTF8ENCODING));
            buf.put(VoltType.BIGINT.getValue());
            buf.putInt(Long.BYTES);

            // VOLT_SITE_ID, VoltType.BIGINT
            buf.putInt(VOLT_SITE_ID.length());
            buf.put(VOLT_SITE_ID.getBytes(Constants.UTF8ENCODING));
            buf.put(VoltType.BIGINT.getValue());
            buf.putInt(Long.BYTES);

            // VOLT_EXPORT_OPERATION, VoltType.TINYINT
            buf.putInt(VOLT_EXPORT_OPERATION.length());
            buf.put(VOLT_EXPORT_OPERATION.getBytes(Constants.UTF8ENCODING));
            buf.put(VoltType.TINYINT.getValue());
            buf.putInt(Byte.BYTES);
        }
        /*
         * Export PBD segment schema layout:
         *
         * export buffer version(1)
         * generation ID(8)
         * schema length(4)
         * stream name length(4)
         * stream name
         * (meta columns)
         * column name length(4)
         * column name(VOLT_TRANSACTION_ID)
         * column type(1, VoltType.BIGINT)
         * column length(4)
         * column name length(4)
         * column name(VOLT_EXPORT_TIMESTAMP)
         * column type(1, VoltType.BIGINT)
         * column length(4)
         * column name length(4)
         * column name(VOLT_EXPORT_SEQUENCE_NUMBER)
         * column type(1, VoltType.BIGINT)
         * column length(4)
         * column name length(4)
         * column name(VOLT_PARTITION_ID)
         * column type(1, VoltType.BIGINT)
         * column length(4)
         * column name length(4)
         * column name(VOLT_SITE_ID)
         * column type(1, VoltType.BIGINT)
         * column length(4)
         * column name length(4)
         * column name(VOLT_EXPORT_OPERATION)
         * column type(1, VoltType.TINYINT)
         * column length(4)
         * (every column)
         * column name length(4)
         * column name
         * column type(1)
         * column length(4)
         *
         */
        @Override
        public void serialize(ByteBuffer buf) throws IOException {
            buf.put((byte)StreamBlockQueue.EXPORT_BUFFER_VERSION);
            buf.putLong(m_catalogContext.m_genId);
            buf.putInt(buf.limit() - PBDSegment.EXPORT_SCHEMA_HEADER_BYTES); // size of schema
            buf.putInt(m_streamName.length());
            buf.put(m_streamName.getBytes(Constants.UTF8ENCODING));

            // write export meta columns
            writeMetaColumns(buf);
            // column name length, name, type, length
            Table streamTable = m_catalogContext.database.getTables().get(m_streamName);
            assert (streamTable != null);
            for (Column c : CatalogUtil.getSortedCatalogItems(streamTable.getColumns(), "index")) {
                buf.putInt(c.getName().length());
                buf.put(c.getName().getBytes(Constants.UTF8ENCODING));
                buf.put((byte)c.getType());
                buf.putInt(c.getSize());
            }
        }

        @Override
        public void cancel() {}

        @Override
        public int getSerializedSize() throws IOException {
            int size = 0;
            // column name length, name, type, length
            Table streamTable = m_catalogContext.database.getTables().get(m_streamName);
            assert (streamTable != null);
            for (Column c : CatalogUtil.getSortedCatalogItems(streamTable.getColumns(), "index")) {
                size += 4 + c.getName().length() + 1 + 4;
            }
            return  PBDSegment.EXPORT_SCHEMA_HEADER_BYTES + /*schema size*/
                    4 /*name length*/ + m_streamName.length() +
                    4 /*name length*/ + VOLT_TRANSACTION_ID.length() + 1 /*column type*/ + 4 /*column length*/ +
                    4 + VOLT_EXPORT_TIMESTAMP.length() + 1 + 4 +
                    4 + VOLT_EXPORT_SEQUENCE_NUMBER.length() + 1 + 4 +
                    4 + VOLT_PARTITION_ID.length() + 1 + 4 +
                    4 + VOLT_SITE_ID.length() + 1 + 4 +
                    4 + VOLT_EXPORT_OPERATION.length() + 1 + 4 +
                    size;
        }
    }
}
