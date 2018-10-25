/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
import org.voltcore.utils.Pair;
import org.voltdb.ExportStatsBase.ExportRole;
import org.voltdb.ExportStatsBase.ExportStatsRow;
import org.voltdb.RealVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.export.AdvertisedDataSource.ExportFormat;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.utils.CatalogUtil;
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
    private long m_gapCount = 0;
    private AtomicInteger m_tuplesPending = new AtomicInteger(0);
    private long m_averageLatency = 0; // for current counting-session
    private long m_maxLatency = 0; // for current counting-session
    private long m_blocksSentSinceClear = 0;
    private long m_totalLatencySinceClearInMS = 0;
    private long m_overallBlocksSent = 0;
    private long m_overallLatencyInMS = 0;
    private long m_overallMaxLatency = 0;

    private final ExportFormat m_format;

    private long m_firstUnpolledSeqNo = 1L; // sequence number starts from 1
    private long m_lastReleasedSeqNo = 0L;
    // End uso of most recently pushed export buffer
    private long m_lastPushedSeqNo = 0L;
    // Relinquish export master after this uso
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
    private volatile boolean m_eos;
    private final Generation m_generation;
    private final File m_adFile;
    private ExportClientBase m_client;
    private boolean m_readyForPolling;
    // This flag is specifically added for XDCR conflicts stream, which export conflict logs
    // on every host. Every data source with this flag set to true is an export master.
    private boolean m_runEveryWhere = false;
    // It is used to filter stale message responses
    private long m_currentRequestId = 0L;

    private ExportSequenceNumberTracker m_gapTracker = new ExportSequenceNumberTracker();

    public final ArrayList<String> m_columnNames = new ArrayList<>();
    public final ArrayList<Integer> m_columnTypes = new ArrayList<>();
    public final ArrayList<Integer> m_columnLengths = new ArrayList<>();
    private String m_partitionColumnName = "";

    static enum StreamStatus {
        ACTIVE,
        DROPPED,
        BLOCKED
    }
    private StreamStatus m_status = StreamStatus.ACTIVE;

    static class QueryResponse implements Comparable<QueryResponse>{
        boolean canCover;
        long lastSeq;
        public QueryResponse(boolean canCover, long lastSeq) {
            this.canCover = canCover;
            this.lastSeq = lastSeq;
        }

        @Override
        public int compareTo(QueryResponse o) {
            return (int)(this.lastSeq - o.lastSeq);
        }

    }

    /**
     * Create a new data source.
     * @param db
     * @param tableName
     * @param partitionId
     */
    public ExportDataSource(
            Generation generation,
            String db,
            String tableName,
            int partitionId,
            int siteId,
            String signature,
            CatalogMap<Column> catalogMap,
            Column partitionColumn,
            String overflowPath
            ) throws IOException
    {
        m_generation = generation;
        m_format = ExportFormat.SEVENDOTX;
        m_database = db;
        m_tableName = tableName;
        m_signature = signature;
        m_signatureBytes = m_signature.getBytes(StandardCharsets.UTF_8);

        PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(m_signatureBytes);
        String nonce = m_tableName + "_" + crc.getValue() + "_" + partitionId;

        m_committedBuffers = new StreamBlockQueue(overflowPath, nonce);
        m_gapTracker = m_committedBuffers.scanPersistentLog();
        m_firstUnpolledSeqNo = m_gapTracker.isEmpty() ? 1L : m_gapTracker.getFirstSeqNo();
        if (exportLog.isDebugEnabled()) {
            exportLog.debug(toString() + " reads gap tracker from PBD:" + m_gapTracker.toString());
        }

        /*
         * This is not the catalog relativeIndex(). This ID incorporates
         * a catalog version and a table id so that it is constant across
         * catalog updates that add or drop tables.
         */
        m_partitionId = partitionId;

        m_siteId = siteId;

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
        m_eos = false;
        m_client = null;
        m_es = CoreUtils.getListeningExecutorService("ExportDataSource for table " +
                    m_tableName + " partition " + m_partitionId, 1);
    }

    public ExportDataSource(Generation generation, File adFile,
            List<Pair<Integer, Integer>> localPartitionsToSites) throws IOException {
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
        m_committedBuffers = new StreamBlockQueue(overflowPath, nonce);
        m_gapTracker = m_committedBuffers.scanPersistentLog();
        m_firstUnpolledSeqNo = m_gapTracker.isEmpty() ? 1L : m_gapTracker.getFirstSeqNo();
        if (exportLog.isDebugEnabled()) {
            exportLog.debug(toString() + " reads gap tracker from PBD:" + m_gapTracker.toString());
        }
        //EDS created from adfile is always from disk.
        m_isInCatalog = false;
        m_eos = false;
        m_client = null;
        m_es = CoreUtils.getListeningExecutorService("ExportDataSource for table " +
                m_tableName + " partition " + m_partitionId, 1);
    }

    public void setReadyForPolling(boolean readyForPolling) {
        m_readyForPolling = readyForPolling;
    }

    public void markInCatalog() {
        m_isInCatalog = true;
    }

    public synchronized void updateAckMailboxes(final Pair<Mailbox, ImmutableList<Long>> ackMailboxes) {
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Mailbox " + CoreUtils.hsIdToString(ackMailboxes.getFirst().getHSId()) + " is registered for " + this.toString() +
                    " : replicas " + CoreUtils.hsIdCollectionToString(ackMailboxes.getSecond()) );
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

    private synchronized void releaseExportBytes(long releaseSeqNo, int tuplesSent) throws IOException {
        // Released offset is in an already-released past
        if (!m_committedBuffers.isEmpty() && releaseSeqNo < m_committedBuffers.peek().startSequenceNumber()) {
            tuplesSent = 0;
        }
        if (m_lastReleasedSeqNo == releaseSeqNo) {
            tuplesSent = 0;
        }

        long lastSeqNo = 0;
        while (!m_committedBuffers.isEmpty() && releaseSeqNo >= m_committedBuffers.peek().startSequenceNumber()) {
            StreamBlock sb = m_committedBuffers.peek();
            if (releaseSeqNo >= sb.startSequenceNumber() + sb.rowCount() - 1) {
                m_committedBuffers.pop();
                try {
                    lastSeqNo = sb.startSequenceNumber() + sb.rowCount() - 1;
                } finally {
                    sb.discard();
                }
            } else if (releaseSeqNo >= sb.startSequenceNumber()) {
                sb.releaseTo(releaseSeqNo);
                lastSeqNo = releaseSeqNo;
                break;
            }
        }
        m_lastReleasedSeqNo = releaseSeqNo;
        // If persistent log contains gap, mostly due to node failures and rejoins, acks from leader might
        // fill the gap gradually.
        m_gapTracker.truncate(releaseSeqNo);
        if (exportLog.isDebugEnabled()) {
            exportLog.debug("Truncate tracker via ack to:" + releaseSeqNo + " now is " + m_gapTracker.toString());
        }
        // If releaseSeqNo falls in between stream buffers,
        if (m_committedBuffers.isEmpty()) {
            lastSeqNo = releaseSeqNo;
        }
        m_firstUnpolledSeqNo = Math.max(m_firstUnpolledSeqNo, lastSeqNo + 1);
        int pendingCount = m_tuplesPending.get();
        m_tuplesPending.set(pendingCount - tuplesSent);

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
                ExportRole role = m_mastershipAccepted.get() ? ExportRole.MASTER : ExportRole.REPLICA;

                return new ExportStatsRow(m_partitionId, m_siteId, m_tableName, role.toString(), m_exportTargetName,
                        m_tupleCount, m_tuplesPending.get(), avgLatency, maxLatency, m_gapCount, m_status.toString());
            }
        });
    }

    private void pushExportBufferImpl(
            long startSequenceNumber,
            int tupleCount,
            ByteBuffer buffer,
            boolean sync,
            boolean poll) throws Exception {
        final java.util.concurrent.atomic.AtomicBoolean deleted = new java.util.concurrent.atomic.AtomicBoolean(false);
        if (exportLog.isTraceEnabled()) {
            exportLog.trace("pushExportBufferImpl with seq=" + startSequenceNumber + ", sync=" + sync + ", poll=" + poll);
        }
        if (buffer != null) {
            // header space along is 8 bytes
            assert (buffer.capacity() > StreamBlock.HEADER_SIZE);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            // Drop already acked buffer
            final BBContainer cont = DBBPool.wrapBB(buffer);
            long lastSequenceNumber = startSequenceNumber + tupleCount - 1;
            if (m_lastReleasedSeqNo > 0 && m_lastReleasedSeqNo >= lastSequenceNumber) {
                m_tupleCount += tupleCount;
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug("Dropping already acked buffer. " +
                            " Buffer info: " + startSequenceNumber + " Size: " + tupleCount +
                            " last released seq: " + m_lastReleasedSeqNo);
                }
                cont.discard();
                return;
            }
            m_gapTracker.append(startSequenceNumber, lastSequenceNumber);
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Append [" + startSequenceNumber + "," + lastSequenceNumber +"] to gap tracker.");
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
                        }, startSequenceNumber, tupleCount, false);

                // Mark release sequence number to partially acked buffer.
                if (m_lastReleasedSeqNo > 0 && m_lastReleasedSeqNo >= sb.startSequenceNumber()) {
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Setting releaseSeqNo as " + m_lastReleasedSeqNo +
                                " for SB with sequence number " + sb.startSequenceNumber() +
                                " for partition " + m_partitionId);
                    }
                    sb.releaseTo(m_lastReleasedSeqNo);
                }

                m_lastPushedSeqNo = lastSequenceNumber;
                m_tupleCount += tupleCount;
                m_tuplesPending.addAndGet(tupleCount);
                m_committedBuffers.offer(sb);
            } catch (IOException e) {
                VoltDB.crashLocalVoltDB("Unable to write to export overflow.", true, e);
            }
        }
        if (sync) {
            try {
                //Don't do a real sync, just write the in memory buffers
                //to a file. @Quiesce or blocking snapshot will do the sync
                m_committedBuffers.sync(true);
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


    public void pushEndOfStream() {
        exportLog.info("End of stream for table: " + getTableName() +
                " partition: " + getPartitionId() + " signature: " + getSignature());
        m_eos = true;
    }

    public void pushExportBuffer(
            final long startSequenceNumber,
            final int tupleCount,
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
                pushExportBufferImpl(startSequenceNumber, tupleCount, buffer, sync, false);
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
                            pushExportBufferImpl(startSequenceNumber, tupleCount, buffer, sync, m_readyForPolling);
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
            exportLog.info("Error pushing export  buffer: ", rej);
        }
    }

    public ListenableFuture<?> truncateExportToSeqNo(boolean isRecover, long sequenceNumber) {
        return m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    m_tupleCount = sequenceNumber;
                    if (isRecover) {
                        if (sequenceNumber < 0) {
                            exportLog.error("Snapshot does not include valid truncation point for partition " +
                                    m_partitionId);
                        }
                        else {
                            m_committedBuffers.truncateToSequenceNumber(sequenceNumber);
                            if (exportLog.isDebugEnabled()) {
                                exportLog.debug("Truncating export pdb files to sequence number " + sequenceNumber);
                            }
                        }
                    }
                    m_gapTracker.truncate(sequenceNumber);
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Truncate tracker via snapshot truncation to " + sequenceNumber + " now is " + m_gapTracker.toString());
                    }
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
                exportLog.error("failed to sync export overflow", e);
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
                        return;
                    }

                    try {
                        //If we have anything pending set that before moving to next block.
                        if (m_pendingContainer.get() != null) {
                            fut.set(m_pendingContainer.getAndSet(null));
                            if (m_pollFuture != null) {
                                if (exportLog.isDebugEnabled()) {
                                    exportLog.debug("picked up work from pending container, set poll future to null");
                                }
                                m_pollFuture = null;
                            }
                            return;
                        }
                        /*
                         * The poll is blocking through the future, shouldn't
                         * call poll a second time until a response has been given
                         * which nulls out the field
                         */
                        if (m_pollFuture != null) {
                            fut.setException(new RuntimeException("Should not poll more than once: InCat = " + m_isInCatalog +
                                    " ExportDataSource for Table " + getTableName() + " at Partition " + getPartitionId()));
                            // Since it's not fatal exception, gives it second chance to poll again.
                            m_pollFuture = null;
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
            StreamBlock first_unpolled_block = null;

            if (!this.m_isInCatalog && m_eos && m_committedBuffers.isEmpty()) {
                //Returning null indicates end of stream
                m_pollFuture = null;
                try {
                    fut.set(null);
                } catch (RejectedExecutionException reex) {
                    //We are closing source.
                }
                //Let generation know to cleanup. Processor needs to do its own cleanup.
                forwardAckToOtherReplicas(Long.MIN_VALUE, 0);
                m_generation.onSourceDone(m_partitionId, m_signature);
                return;
            }
            //Assemble a list of blocks to delete so that they can be deleted
            //outside of the m_committedBuffers critical section
            ArrayList<StreamBlock> blocksToDelete = new ArrayList<>();
            //Inside this critical section do the work to find out
            //what block should be returned by the next poll.
            //Copying and sending the data will take place outside the critical section
            try {
                Iterator<StreamBlock> iter = m_committedBuffers.iterator();
                long firstUnpolledSeq = m_firstUnpolledSeqNo;
                while (iter.hasNext()) {
                    StreamBlock block = iter.next();
                    // find the first block that has unpolled data
                    if (firstUnpolledSeq >= block.startSequenceNumber() &&
                            firstUnpolledSeq < block.startSequenceNumber() + block.rowCount()) {
                        first_unpolled_block = block;
                        m_firstUnpolledSeqNo = block.startSequenceNumber() + block.rowCount();
                        break;
                    } else if (firstUnpolledSeq >= block.startSequenceNumber() + block.rowCount()) {
                        blocksToDelete.add(block);
                        iter.remove();
                    } else {
                        // Gap only exists in the middle of buffers, why is it never be in the head of
                        // queue? Because only master checks the gap, mastership migration waits until
                        // the last pushed buffer at the checkpoint time is acked, it won't leave gap
                        // behind before migrates to another node.
                        Pair<Long, Long> gap = m_gapTracker.getFirstGap();
                        if (gap != null && firstUnpolledSeq >= gap.getFirst() && firstUnpolledSeq <= gap.getSecond()) {
                            // Hit a gap! Prepare to relinquish master role and broadcast queries for
                            // capable candidate.
                            exportLog.info(toString() + " hit a gap [" +
                                    gap.getFirst() + ", " + gap.getSecond() +
                                    "], start looking for other nodes that has the data.");
                            // If another mastership migration in progress and is before the gap,
                            // don't bother to start another
                            if (m_seqNoToDrain >= firstUnpolledSeq -1) {
                                m_seqNoToDrain = Math.min(m_seqNoToDrain, firstUnpolledSeq - 1);
                                mastershipCheckpoint(firstUnpolledSeq - 1);
                            }
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
                    sb.discard();
                }
            }

            //If there are no unpolled blocks return the firstUnpolledUSO with no data
            if (first_unpolled_block == null) {
                m_pollFuture = fut;
            } else {
                if (m_status == StreamStatus.BLOCKED) {
                    setStatus(StreamStatus.ACTIVE);
                }
                final AckingContainer ackingContainer =
                        new AckingContainer(first_unpolled_block.unreleasedContainer(),
                                first_unpolled_block.startSequenceNumber() + first_unpolled_block.rowCount() - 1,
                                first_unpolled_block.rowCount());
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
        final long m_seqNo;
        final int m_tuplesSent;
        final BBContainer m_backingCont;
        long m_startTime = 0;

        public AckingContainer(BBContainer cont, long seq, int tuplesSent) {
            super(cont.b());
            m_seqNo = seq;
            m_tuplesSent = tuplesSent;
            m_backingCont = cont;
        }

        public void updateStartTime(long startTime) {
            m_startTime = startTime;
        }

        @Override
        public void discard() {
            checkDoubleFree();
            try {
                m_es.execute(new Runnable() {
                    @Override
                    public void run() {
                        if (exportLog.isTraceEnabled()) {
                            exportLog.trace("AckingContainer.discard with sequence number: " + m_seqNo + " tuples sent: " + m_tuplesSent);
                        }
                        assert(m_tuplesSent == 0 || m_startTime != 0);
                        long elapsedMS = System.currentTimeMillis() - m_startTime;
                        m_blocksSentSinceClear += 1;
                        m_totalLatencySinceClearInMS += elapsedMS;
                        m_averageLatency = m_totalLatencySinceClearInMS / m_blocksSentSinceClear;
                        if (m_averageLatency > m_maxLatency) {
                            m_maxLatency = m_averageLatency;
                        }

                        try {
                             m_backingCont.discard();
                            try {
                                if (!m_es.isShutdown()) {
                                    ackImpl(m_seqNo, m_tuplesSent);
                                }
                            } finally {
                                forwardAckToOtherReplicas(m_seqNo, m_tuplesSent);
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
            }
        }
    }

    private void forwardAckToOtherReplicas(long seq, int tuplesSent) {
        // In RunEveryWhere mode, every data source is master, no need to send out acks.
        if (m_runEveryWhere) {
            return;
        }
        Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
        Mailbox mbx = p.getFirst();
        if (mbx != null && p.getSecond().size() > 0) {
            // msgType:byte(1) + partition:int(4) + length:int(4) +
            // signaturesBytes.length + ackUSO:long(8) + tuplesSent:int(4).
            final int msgLen = 1 + 4 + 4 + m_signatureBytes.length + 8 + 4;

            ByteBuffer buf = ByteBuffer.allocate(msgLen);
            buf.put(ExportManager.RELEASE_BUFFER);
            buf.putInt(m_partitionId);
            buf.putInt(m_signatureBytes.length);
            buf.put(m_signatureBytes);
            buf.putLong(seq);
            buf.putInt(tuplesSent);

            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());

            for( Long siteId: p.getSecond()) {
                mbx.send(siteId, bpm);
            }
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Send RELEASE_BUFFER to " + toString() + " with sequence number " + seq
                        + " tuples sent " + tuplesSent
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
                    // signaturesBytes.length + ackUSO:long(8) + tuplesSent:int(4).
                    final int msgLen = 1 + 4 + 4 + m_signatureBytes.length + 8 + 4;

                    ByteBuffer buf = ByteBuffer.allocate(msgLen);
                    buf.put(ExportManager.RELEASE_BUFFER);
                    buf.putInt(m_partitionId);
                    buf.putInt(m_signatureBytes.length);
                    buf.put(m_signatureBytes);
                    buf.putLong(m_lastReleasedSeqNo);
                    buf.putInt(0);

                    BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());

                    for( Long siteId: newReplicas) {
                        mbx.send(siteId, bpm);
                    }
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Send RELEASE_BUFFER to " + toString() + " with sequence number " + m_lastReleasedSeqNo
                                + " from " + CoreUtils.hsIdToString(mbx.getHSId())
                                + " to " + CoreUtils.hsIdCollectionToString(p.getSecond()));
                    }
                }
            }
        });
    }

    public void ack(final long seq, int tuplesSent) {

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
                        ackImpl(seq, tuplesSent);
                    }
                } catch (Exception e) {
                    exportLog.error("Error acking export buffer", e);
                } catch (Error e) {
                    VoltDB.crashLocalVoltDB("Error acking export buffer", true, e);
                }
            }
        });
    }

     private void ackImpl(long seq, int tuplesSent) {
        //Process the ack if any and add blocks to the delete list or move the released sequence number
        if (seq > 0) {
            try {
                releaseExportBytes(seq, tuplesSent);
                mastershipCheckpoint(seq);
            } catch (IOException e) {
                VoltDB.crashLocalVoltDB("Error attempting to release export bytes", true, e);
                return;
            }
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
                // but if we already wait to switch mastership, don't update the sequence to drain to
                // a greater number
                m_seqNoToDrain = Math.min(m_seqNoToDrain, m_lastPushedSeqNo);
                m_newLeaderHostId = newLeaderHostId;
                // if no new buffer to be drained, send the migrate event right away
                mastershipCheckpoint(m_lastReleasedSeqNo);
            }
        });
    }

    private void sendGiveMastershipMessage(int newLeaderHostId, long curSeq) {
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
            buf.putLong(curSeq);

            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());

            for(Long siteId: p.getSecond()) {
                // Just send to the ack mailbox on the new master
                if (CoreUtils.getHostIdFromHSId(siteId) == newLeaderHostId) {
                    mbx.send(siteId, bpm);
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug(toString() + " send GIVE_MASTERSHIP message to " +
                                CoreUtils.hsIdToString(siteId) + " curruent sequence number " + curSeq);
                    }
                    break;
                }
            }
        }
        unacceptMastership();
    }

    private void sendTaskMastershipMessage() {
        m_queryResponses.clear();
        Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
        Mailbox mbx = p.getFirst();
        m_currentRequestId = System.nanoTime();
        if (mbx != null && p.getSecond().size() > 0) {
            // msg type(1) + partition:int(4) + length:int(4) + signaturesBytes.length
            // requestId(8)
            final int msgLen = 1 + 4 + 4 + m_signatureBytes.length + 8;
            ByteBuffer buf = ByteBuffer.allocate(msgLen);
            buf.put(ExportManager.TASK_MASTERSHIP);
            buf.putInt(m_partitionId);
            buf.putInt(m_signatureBytes.length);
            buf.put(m_signatureBytes);
            buf.putLong(m_currentRequestId);
            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());
            for( Long siteId: p.getSecond()) {
                mbx.send(siteId, bpm);
            }
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Send TASK_MASTERSHIP message(" + m_currentRequestId +
                        ") for partition " + m_partitionId + "source signature " + m_tableName +
                        " from " + CoreUtils.hsIdToString(mbx.getHSId()) +
                        " to " + CoreUtils.hsIdCollectionToString(p.getSecond()));
            }
        } else {
            // There is no other replica, promote myself.
            acceptMastership();
        }
    }

    private void sendQueryResponse(long senderHSId, long requestId, boolean canCover, long lastSeq) {
        Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
        Mailbox mbx = p.getFirst();
        if (mbx != null && p.getSecond().size() > 0 && p.getSecond().contains(senderHSId)) {
            // msg type(1) + partition:int(4) + length:int(4) + signaturesBytes.length
            // requestId(8) + canCover(1) + lastSeq(8, optional)
            int msgLen = 1 + 4 + 4 + m_signatureBytes.length + 8 + 1;
            if (canCover) {
                msgLen += 8;
            }
            ByteBuffer buf = ByteBuffer.allocate(msgLen);
            buf.put(ExportManager.QUERY_RESPONSE);
            buf.putInt(m_partitionId);
            buf.putInt(m_signatureBytes.length);
            buf.put(m_signatureBytes);
            buf.putLong(requestId);
            buf.put(canCover? (byte)1 : (byte)0);
            if (canCover) {
                buf.putLong(lastSeq);
            }
            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());
            mbx.send(senderHSId, bpm);
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Partition " + m_partitionId + " mailbox hsid (" +
                        CoreUtils.hsIdToString(mbx.getHSId()) + ") send QUERY_RESPONSE message(" +
                        requestId + "," + m_mastershipAccepted.get() + "," + canCover +
                        ") to " + CoreUtils.hsIdToString(senderHSId));
            }
        }
    }

    public synchronized void unacceptMastership() {
        if (exportLog.isDebugEnabled()) {
            exportLog.debug(toString() + " is no longer the export stream master.");
        }
        m_mastershipAccepted.set(false);
        m_isInCatalog = false;
        m_eos = false;
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
    public void setOnMastership(Runnable toBeRunOnMastership, boolean runEveryWhere) {
        Preconditions.checkNotNull(toBeRunOnMastership, "mastership runnable is null");
        m_onMastership = toBeRunOnMastership;
        runEveryWhere(runEveryWhere);
    }

    public ExportFormat getExportFormat() {
        return m_format;
    }

    /**
     * @param runEveryWhere Set if connector "replicated" property is set to true Like replicated table, every
     *                      replicated export stream is its own master.
     */
    public void runEveryWhere(boolean runEveryWhere) {
        m_runEveryWhere = runEveryWhere;
        if (runEveryWhere) {
            acceptMastership();
        }
    }

    public ListeningExecutorService getExecutorService() {
        return m_es;
    }

    private void sendGapQuery() {
        // Should be the master and the master was stuck on a gap
        if (m_mastershipAccepted.get() && m_gapTracker.getFirstGap() != null) {
            if (m_gapTracker.isEmpty()) {
                return;
            }
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
                            ") for partition " + m_partitionId + "source signature " + m_tableName +
                            " from " + CoreUtils.hsIdToString(mbx.getHSId()) +
                            " to " + CoreUtils.hsIdCollectionToString(p.getSecond()));
                }
            } else {
                setStatus(StreamStatus.BLOCKED);
                Pair<Long, Long> gap = m_gapTracker.getFirstGap();
                exportLog.warn(toString() + " is blocked because of data from sequence number " + gap.getFirst() +
                        " to " + gap.getSecond() + " is missing.");
            }
        }
    }

    public void queryForBestCandidate() {
        m_es.execute(new Runnable() {
            public void run() {
                sendGapQuery();
            }
        });
    }

    void takeMastership() {
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                // Skip current master
                if (m_mastershipAccepted.get()) {
                    return;
                }
                if (exportLog.isDebugEnabled()) {
                    exportLog.debug(toString() + " decides to claw mastership back from other node.");
                }
                // Query export membership if current stream is not the master
                sendTaskMastershipMessage();
            }
        });
    }

    // Query whether a master exists for the given partition, if not try to promote the local data source.
    public void handleQueryMessage(final long senderHSId, long requestId, long gapStart) {
            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    boolean canCover = canCoverNextSequenceNumber(gapStart);
                    long lastSeq = m_gapTracker.getRangeContaining(gapStart);
                    sendQueryResponse(senderHSId, requestId, canCover, lastSeq);
                }
            });
    }

    public void handleQueryResponse(long sendHsId, long requestId, boolean canCover, long lastSeq) {
        if (m_currentRequestId == requestId && m_mastershipAccepted.get()) {
            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    m_queryResponses.put(sendHsId, new QueryResponse(canCover, lastSeq));
                    Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
                    if (p.getSecond().stream().allMatch(hsid -> m_queryResponses.containsKey(hsid))) {
                        Entry<Long, QueryResponse> bestCandidate = null;
                        try {
                            bestCandidate = m_queryResponses.entrySet().stream()
                                           .filter(s -> s.getValue().canCover)
                                           .sorted()
                                           .findFirst()
                                           .get();
                        } catch (NoSuchElementException e) {
                            setStatus(StreamStatus.BLOCKED);
                            // Show warning only in full cluster.
                            RealVoltDB voltdb = (RealVoltDB)VoltDB.instance();
                            if (voltdb.isClusterComplete()) {
                                Pair<Long, Long> gap = m_gapTracker.getFirstGap();
                                exportLog.warn(ExportDataSource.this.toString() + " is blocked because stream hits a gap from sequence number " +
                                        gap.getFirst() + " to " + gap.getSecond());
                            }
                        }
                        // time to give up master and give it to the best candidate
                        if (bestCandidate != null) {
                            m_newLeaderHostId = CoreUtils.getHostIdFromHSId(bestCandidate.getKey());
                            exportLog.info("Stream master is going to switch to host " + m_newLeaderHostId + " to jump the gap.");
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
        if (mbx != null && p.getSecond().size() > 0 && p.getSecond().contains(senderHsId)) {
            // msg type(1) + partition:int(4) + length:int(4) + signaturesBytes.length
            // requestId(8)
            int msgLen = 1 + 4 + 4 + m_signatureBytes.length + 8;
            ByteBuffer buf = ByteBuffer.allocate(msgLen);
            buf.put(ExportManager.TASK_MASTERSHIP_RESPONSE);
            buf.putInt(m_partitionId);
            buf.putInt(m_signatureBytes.length);
            buf.put(m_signatureBytes);
            buf.putLong(requestId);
            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());
            mbx.send(senderHsId, bpm);
            if (exportLog.isDebugEnabled()) {
                exportLog.debug("Partition " + m_partitionId + " mailbox hsid (" +
                        CoreUtils.hsIdToString(mbx.getHSId()) +
                        ") send TASK_MASTERSHIP_RESPONSE message(" +
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

    public void setStatus(StreamStatus status) {
        this.m_status = status;
    }

    public void setGapCount(long gapCount) {
        m_gapCount = gapCount;
    }

    @Override
    public String toString() {
        return "ExportDataSource for Table " + getTableName() + " at Partition " + getPartitionId();
    }

    private boolean canCoverNextSequenceNumber(long nextSeq) {
        if (m_gapTracker.size() == 0) {
            return true;
        }
        return m_gapTracker.contains(nextSeq, nextSeq);
    }

    private void mastershipCheckpoint(long seq) {
        if (exportLog.isTraceEnabled()) {
            exportLog.trace("Export table " + getTableName() + " mastership checkpoint "  +
                    " m_newLeaderHostId " + m_newLeaderHostId + " m_seqNoToDrain " + m_seqNoToDrain +
                    " m_lastReleasedSeqNo " + m_lastReleasedSeqNo + " m_lastPushedSeqNo " + m_lastPushedSeqNo);
        }
        // time to give away leadership
        if (seq >= m_seqNoToDrain) {
            if (m_newLeaderHostId != null) {
                sendGiveMastershipMessage(m_newLeaderHostId, seq);
            } else {
                sendGapQuery();
            }
        }
    }
}
