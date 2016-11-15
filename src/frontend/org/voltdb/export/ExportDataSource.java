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

package org.voltdb.export;

import static com.google_voltpatches.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

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
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.common.Constants;
import org.voltdb.export.AdvertisedDataSource.ExportFormat;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.io.Files;
import com.google_voltpatches.common.util.concurrent.Futures;
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

    private final String m_database;
    private final String m_tableName;
    private String m_partitionColumnName = "";
    private final String m_signature;
    private final byte [] m_signatureBytes;
    private final long m_generation;
    private final int m_partitionId;
    private final ExportFormat m_format;
    public final ArrayList<String> m_columnNames = new ArrayList<String>();
    public final ArrayList<Integer> m_columnTypes = new ArrayList<Integer>();
    public final ArrayList<Integer> m_columnLengths = new ArrayList<Integer>();
    private long m_firstUnpolledUso = 0;
    private final StreamBlockQueue m_committedBuffers;
    private boolean m_endOfStream = false;
    private Runnable m_onDrain;
    private Runnable m_onMastership;
    private SettableFuture<BBContainer> m_pollFuture;
    private final AtomicReference<Pair<Mailbox, ImmutableList<Long>>> m_ackMailboxRefs =
            new AtomicReference<Pair<Mailbox,ImmutableList<Long>>>(Pair.of((Mailbox)null, ImmutableList.<Long>builder().build()));
    private final Semaphore m_bufferPushPermits = new Semaphore(16);

    private final int m_nullArrayLength;
    private long m_lastReleaseOffset = 0;
    private long m_lastAckUSO = 0;
    //This is for testing only.
    public static boolean m_dontActivateForTest = false;
    //Set if connector "replicated" property is set to true
    private boolean m_runEveryWhere = false;
    private boolean m_isMaster = false;
    private boolean m_replicaRunning = false;
    //This is released when all mailboxes are set.
    private final Semaphore m_allowAcceptingMastership = new Semaphore(0);
    private volatile boolean m_closed = false;
    private volatile boolean m_mastershipAccepted = false;
    private volatile ListeningExecutorService m_executor;
    private final Integer m_executorLock = new Integer(0);
    private final LinkedTransferQueue<RunnableWithES> m_queuedActions = new LinkedTransferQueue<>();
    private RunnableWithES m_firstAction = null;

    /**
     * Create a new data source.
     * @param db
     * @param tableName
     * @param isReplicated
     * @param partitionId
     * @param HSId
     * @param tableId
     * @param catalogMap
     */
    public ExportDataSource(
            final Runnable onDrain,
            String db, String tableName,
            int partitionId, String signature, long generation,
            CatalogMap<Column> catalogMap,
            Column partitionColumn,
            String overflowPath
            ) throws IOException
            {
        checkNotNull( onDrain, "onDrain runnable is null");
        m_format = ExportFormat.FOURDOTFOUR;
        m_generation = generation;
        m_onDrain = new Runnable() {
            @Override
            public void run() {
                try {
                    onDrain.run();
                } finally {
                    m_onDrain = null;
                    forwardAckToOtherReplicas(Long.MIN_VALUE);
                }
            }
        };
        m_database = db;
        m_tableName = tableName;

        String nonce = signature + "_" + partitionId;

        m_committedBuffers = new StreamBlockQueue(overflowPath, nonce);

        /*
         * This is not the catalog relativeIndex(). This ID incorporates
         * a catalog version and a table id so that it is constant across
         * catalog updates that add or drop tables.
         */
        m_signature = signature;
        m_signatureBytes = m_signature.getBytes(Constants.UTF8ENCODING);
        m_partitionId = partitionId;

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
        File adFile = new VoltFile(overflowPath, nonce + ".ad");
        exportLog.info("Creating ad for " + nonce);
        byte jsonBytes[] = null;
        try {
            JSONStringer stringer = new JSONStringer();
            stringer.object();
            stringer.keySymbolValuePair("database", m_database);
            writeAdvertisementTo(stringer);
            stringer.endObject();
            JSONObject jsObj = new JSONObject(stringer.toString());
            jsonBytes = jsObj.toString(4).getBytes(Charsets.UTF_8);
        } catch (JSONException e) {
            exportLog.error("Failed to Write ad file for " + nonce);
            Throwables.propagate(e);
        }

        try (FileOutputStream fos = new FileOutputStream(adFile)) {
            fos.write(jsonBytes);
            fos.getFD().sync();
        }

        // compute the number of bytes necessary to hold one bit per
        // schema column
        m_nullArrayLength = ((m_columnTypes.size() + 7) & -8) >> 3;

        // This is not being loaded from file, so activate immediately
        if (!m_dontActivateForTest) {
            activate();
        }
    }

    public ExportDataSource(final Runnable onDrain, File adFile, boolean isContinueingGeneration) throws IOException {
        /*
         * Certainly no more data coming if this is coming off of disk
         */
        m_onDrain = new Runnable() {
            @Override
            public void run() {
                try {
                    onDrain.run();
                } finally {
                    m_onDrain = null;
                    forwardAckToOtherReplicas(Long.MIN_VALUE);
                }
            }
        };

        String overflowPath = adFile.getParent();
        byte data[] = Files.toByteArray(adFile);
        long hsid = -1;
        try {
            JSONObject jsObj = new JSONObject(new String(data, Charsets.UTF_8));

            long version = jsObj.getLong("adVersion");
            if (version != 0) {
                throw new IOException("Unsupported ad file version " + version);
            }
            try {
                hsid = jsObj.getLong("hsId");
                exportLog.info("Found old for export data source file ignoring m_HSId");
            } catch (JSONException jex) {
                hsid = -1;
            }
            m_database = jsObj.getString("database");
            m_generation = jsObj.getLong("generation");
            m_partitionId = jsObj.getInt("partitionId");
            m_signature = jsObj.getString("signature");
            m_signatureBytes = m_signature.getBytes(Constants.UTF8ENCODING);
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
                m_format = ExportFormat.FOURDOTFOUR;
            }

            try {
                m_partitionColumnName = jsObj.getString("partitionColumnName");
            } catch (Exception ex) {
                //Ignore these if we have a OLD ad file these may not exist.
            }
        } catch (JSONException e) {
            throw new IOException(e);
        }

        String nonce;
        if (hsid == -1) {
            nonce = m_signature + "_" + m_partitionId;
        } else {
            nonce = m_signature + "_" + hsid + "_" + m_partitionId;
        }
        //If on disk generation matches catalog generation we dont do end of stream as it will be appended to.
        m_endOfStream = !isContinueingGeneration;

        m_committedBuffers = new StreamBlockQueue(overflowPath, nonce);

        // compute the number of bytes necessary to hold one bit per
        // schema column
        m_nullArrayLength = ((m_columnTypes.size() + 7) & -8) >> 3;
    }

    public void activate() {
        setupExecutor();
    }

    public synchronized void updateAckMailboxes(final Pair<Mailbox, ImmutableList<Long>> ackMailboxes) {
        m_ackMailboxRefs.set( ackMailboxes);
    }

    private void releaseExportBytes(long releaseOffset) throws IOException {
        // if released offset is in an already-released past, just return success
        if (!m_committedBuffers.isEmpty() && releaseOffset < m_committedBuffers.peek().uso()) {
            return;
        }

        long lastUso = m_firstUnpolledUso;
        while (!m_committedBuffers.isEmpty()
                && releaseOffset >= m_committedBuffers.peek().uso()) {
            StreamBlock sb = m_committedBuffers.peek();
            if (releaseOffset >= sb.uso() + sb.totalUso()) {
                m_committedBuffers.pop();
                try {
                    lastUso = sb.uso() + sb.totalUso();
                } finally {
                    sb.discard();
                }
            } else if (releaseOffset >= sb.uso()) {
                sb.releaseUso(releaseOffset);
                lastUso = releaseOffset;
                break;
            }
        }
        m_lastReleaseOffset = releaseOffset;
        m_firstUnpolledUso = Math.max(m_firstUnpolledUso, lastUso);
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

    public final void writeAdvertisementTo(JSONStringer stringer) throws JSONException {
        stringer.keySymbolValuePair("adVersion", 0);
        stringer.keySymbolValuePair("generation", m_generation);
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
        stringer.keySymbolValuePair("format", ExportFormat.FOURDOTFOUR.toString());
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

        result = m_database.compareTo(o.m_database);
        if (result != 0) {
            return result;
        }

        result = m_tableName.compareTo(o.m_tableName);
        if (result != 0) {
            return result;
        }

        result = (m_partitionId - o.m_partitionId);
        if (result != 0) {
            return result;
        }

        // does not verify replicated / unreplicated.
        // does not verify column names / schema
        return 0;
    }

    /**
     * Make sure equal objects compareTo as 0.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ExportDataSource))
            return false;

        return compareTo((ExportDataSource)o) == 0;
    }

    @Override
    public int hashCode() {
        // based on implementation of compareTo
        int result = 0;
        result += m_database.hashCode();
        result += m_tableName.hashCode();
        result += m_partitionId;
        // does not factor in replicated / unreplicated.
        // does not factor in column names / schema
        return result;
    }


    public long sizeInBytes() {
        try {
            ListeningExecutorService es = getExecutorService();
            if (es==null) {
                return m_committedBuffers.sizeInBytes();
            }
            else {
                return es.submit(new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        return m_committedBuffers.sizeInBytes();
                    }
                }).get();
            }
        } catch (RejectedExecutionException e) {
            return 0;
        } catch (Throwable t) {
            Throwables.propagate(t);
            return 0;
        }
    }

    private void pushExportBufferImpl(
            long uso,
            ByteBuffer buffer,
            boolean sync,
            boolean endOfStream, boolean poll) throws Exception {
        final java.util.concurrent.atomic.AtomicBoolean deleted = new java.util.concurrent.atomic.AtomicBoolean(false);
        if (endOfStream) {
            assert(!m_endOfStream);
            assert(buffer == null);
            assert(!sync);

            m_endOfStream = endOfStream;

            if (m_committedBuffers.isEmpty()) {
                exportLog.info("Pushed EOS buffer with 0 bytes remaining");
                if (m_pollFuture != null) {
                    m_pollFuture.set(null);
                    m_pollFuture = null;
                }
                if (m_onDrain != null) {
                    m_onDrain.run();
                }
            } else {
                exportLog.info("EOS for " + m_tableName + " partition " + m_partitionId +
                        " with first unpolled uso " + m_firstUnpolledUso + " and remaining bytes " +
                        m_committedBuffers.sizeInBytes());
            }
            return;
        }
        assert(!m_endOfStream);
        if (buffer != null) {
            //There will be 8 bytes of no data that we can ignore, it is header space for storing
            //the USO in stream block
            if (buffer.capacity() > 8) {
                final BBContainer cont = DBBPool.wrapBB(buffer);
                if (m_lastReleaseOffset > 0 && m_lastReleaseOffset >= (uso + (buffer.capacity() - 8))) {
                    //What ack from future is known?
                    if (exportLog.isDebugEnabled()) {
                        exportLog.debug("Dropping already acked USO: " + m_lastReleaseOffset
                                + " Buffer info: " + uso + " Size: " + buffer.capacity());
                    }
                    cont.discard();
                    return;
                }
                try {
                    m_committedBuffers.offer(new StreamBlock(
                            new BBContainer(buffer) {
                                @Override
                                public void discard() {
                                    checkDoubleFree();
                                    cont.discard();
                                    deleted.set(true);
                                }
                            }, uso, false));
                } catch (IOException e) {
                    VoltDB.crashLocalVoltDB("Unable to write to export overflow.", true, e);
                }
            } else {
                /*
                 * TupleStreamWrapper::setBytesUsed propagates the USO by sending
                 * over an empty stream block. The block will be deleted
                 * on the native side when this method returns
                 */
                exportLog.info("Syncing first unpolled USO to " + uso + " for table "
                        + m_tableName + " partition " + m_partitionId);
                m_firstUnpolledUso = uso;
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
            pollImpl(m_pollFuture);
        }
    }

    public void pushExportBuffer(
            final long uso,
            final ByteBuffer buffer,
            final boolean sync,
            final boolean endOfStream) {
        try {
            m_bufferPushPermits.acquire();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
        ListeningExecutorService es = getExecutorService();
        if (es == null) {
            //If we have not activated lets get the buffer in overflow and dont poll
            try {
                pushExportBufferImpl(uso, buffer, sync, endOfStream, false);
            } catch (Throwable t) {
                VoltDB.crashLocalVoltDB("Error pushing export  buffer", true, t);
            } finally {
                m_bufferPushPermits.release();
            }
            return;
        }

        if (es.isShutdown()) {
           m_bufferPushPermits.release();
           return;
        }
        try {
            es.execute((new Runnable() {
                @Override
                public void run() {
                    try {
                        if (!es.isShutdown()) {
                            //Since we are part of active generation we poll too
                            pushExportBufferImpl(uso, buffer, sync, endOfStream, true /* poll */);
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

    public long getGeneration() {
        return m_generation;
    }

    public ListenableFuture<?> truncateExportToTxnId(final long txnId) {
        RunnableWithES runnable = new RunnableWithES("truncateExportToTxnId") {
            @Override
            public void run() {
                try {
                    m_committedBuffers.truncateToTxnId(txnId, m_nullArrayLength);
                    if (m_committedBuffers.isEmpty() && m_endOfStream) {
                        if (m_pollFuture != null) {
                            m_pollFuture.set(null);
                            m_pollFuture = null;
                        }
                        if (m_onDrain != null) {
                            m_onDrain.run();
                        }
                    }
                } catch (Throwable t) {
                    VoltDB.crashLocalVoltDB("Error while trying to truncate export to txnid " + txnId, true, t);
                }
            }
        };
        //This is a setup task when stashed tasks are run this is run first.
        return stashOrSubmitTask(runnable, false, true);
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
        RunnableWithES runnable = new RunnableWithES("sync") {
            @Override
            public void run() {
                new SyncRunnable(nofsync).run();
            }
        };

        return stashOrSubmitTask(runnable, false, false);
    }

    public boolean isClosed() {
        return m_closed;
    }

    public ListenableFuture<?> closeAndDelete() {
        m_closed = true;
        RunnableWithES runnable = new RunnableWithES("closeAndDelete") {
            @Override
            public void run() {
                try {
                    m_committedBuffers.closeAndDelete();
                } catch(IOException e) {
                    exportLog.rateLimitedLog(60, Level.WARN, e, "Error closing commit buffers");
                } finally {
                    getLocalExecutorService().shutdown();
                }
            }
        };
        return stashOrSubmitTask(runnable, false, false);
    }

    public ListenableFuture<?> close() {
        m_closed = true;
        //If we are waiting at this allow to break out when close comes in.
        m_allowAcceptingMastership.release();
        RunnableWithES runnable = new RunnableWithES("close") {
            @Override
            public void run() {
                try {
                    m_committedBuffers.close();
                } catch (IOException e) {
                    exportLog.error(e);
                } finally {
                    getLocalExecutorService().shutdown();
                }
            }
        };

        return stashOrSubmitTask(runnable, false, false);
    }

    public ListenableFuture<BBContainer> poll() {
        final SettableFuture<BBContainer> fut = SettableFuture.create();
        RunnableWithES runnable = new RunnableWithES("poll") {
            @Override
            public void run() {
                try {
                    /*
                     * The poll is blocking through the future, shouldn't
                     * call poll a second time until a response has been given
                     * which nulls out the field
                     */
                    if (m_pollFuture != null) {
                        fut.setException(new RuntimeException("Should not poll more than once"));
                        return;
                    }
                    if (!getLocalExecutorService().isShutdown()) {
                        pollImpl(fut);
                    }
                } catch (Exception e) {
                    exportLog.error("Exception polling export buffer", e);
                } catch (Error e) {
                    VoltDB.crashLocalVoltDB("Error polling export buffer", true, e);
                }
            }
        };
        stashOrSubmitTask(runnable, true, false);
        return fut;
    }

    //If replica we poll from lowest of ack rcvd or last poll point.
    private long getFirstUnpolledUso() {
        if (m_isMaster) {
            return m_firstUnpolledUso;
        }
        return Math.min(m_lastAckUSO, m_firstUnpolledUso);
    }

    private void pollImpl(SettableFuture<BBContainer> fut) {
        if (fut == null) {
            return;
        }

        try {
            StreamBlock first_unpolled_block = null;

            if (m_endOfStream && m_committedBuffers.isEmpty()) {
                //Returning null indicates end of stream
                try {
                    fut.set(null);
                } catch (RejectedExecutionException reex) {
                    //We are closing source.
                }
                if (m_onDrain != null) {
                    m_onDrain.run();
                }
                return;
            }
            //Assemble a list of blocks to delete so that they can be deleted
            //outside of the m_committedBuffers critical section
            ArrayList<StreamBlock> blocksToDelete = new ArrayList<StreamBlock>();
            //Inside this critical section do the work to find out
            //what block should be returned by the next poll.
            //Copying and sending the data will take place outside the critical section
            try {
                Iterator<StreamBlock> iter = m_committedBuffers.iterator();
                long fuso = getFirstUnpolledUso();
                while (iter.hasNext()) {
                    StreamBlock block = iter.next();
                    // find the first block that has unpolled data
                    if (fuso < block.uso() + block.totalUso()) {
                        first_unpolled_block = block;
                        m_firstUnpolledUso = (block.uso() + block.totalUso());
                        break;
                    } else {
                        blocksToDelete.add(block);
                        iter.remove();
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
                try {
                    fut.set(
                            new AckingContainer(first_unpolled_block.unreleasedContainer(),
                                    first_unpolled_block.uso() + first_unpolled_block.totalUso()));
                } catch (RejectedExecutionException reex) {
                    //We are closing source.
                }
                m_pollFuture = null;
            }
        } catch (Throwable t) {
            fut.setException(t);
        }
    }

    class AckingContainer extends BBContainer {
        final long m_uso;
        final BBContainer m_backingCont;
        public AckingContainer(BBContainer cont, long uso) {
            super(cont.b());
            m_uso = uso;
            m_backingCont = cont;
        }

        @Override
        public void discard() {
            checkDoubleFree();
            RunnableWithES runnable = new RunnableWithES("discard") {
                @Override
                public void run() {
                    try {
                        m_backingCont.discard();
                        try {
                            if (!getLocalExecutorService().isShutdown()) {
                                ackImpl(m_uso);
                            }
                        } finally {
                            forwardAckToOtherReplicas(m_uso);
                        }
                    } catch (Exception e) {
                        exportLog.error("Error acking export buffer", e);
                    } catch (Error e) {
                        VoltDB.crashLocalVoltDB("Error acking export buffer", true, e);
                    }
                }
            };
            stashOrSubmitTask(runnable, true, false);
        }
    }

    private void forwardAckToOtherReplicas(long uso) {
        if (m_runEveryWhere && m_replicaRunning) {
           //we dont forward if we are running as replica in replicated export
           return;
        }
        Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
        Mailbox mbx = p.getFirst();
        if (mbx != null && p.getSecond().size() > 0) {
            // partition:int(4) + length:int(4) +
            // signaturesBytes.length + ackUSO:long(8) + 2 bytes for runEverywhere or not.
            final int msgLen = 4 + 4 + m_signatureBytes.length + 8 + 2;

            ByteBuffer buf = ByteBuffer.allocate(msgLen);
            buf.putInt(m_partitionId);
            buf.putInt(m_signatureBytes.length);
            buf.put(m_signatureBytes);
            buf.putLong(uso);
            buf.putShort((m_runEveryWhere ? (short )1 : (short )0));


            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());

            for( Long siteId: p.getSecond()) {
                mbx.send(siteId, bpm);
            }
        }
    }

    public void ack(final long uso, boolean runEveryWhere) {
        // If I am not master and run everywhere connector and I get ack to start replicating....do so and become a exporting replica.
        if (m_runEveryWhere && !m_isMaster && runEveryWhere) {
            //These are single threaded so no need to lock.
            m_lastAckUSO = uso;
            if (!m_replicaRunning) {
                exportLog.info("Export generation " + getGeneration() + " accepting mastership for " + getTableName() + " partition " + getPartitionId() + " as replica");
                m_replicaRunning = true;
                m_isMaster = false;
                acceptMastership();
            }
            return;
        }

        //In replicated only master will be doing this.
        RunnableWithES runnable = new RunnableWithES("ack") {
            @Override
            public void run() {
                try {
                    if (!getLocalExecutorService().isShutdown()) {
                       ackImpl(uso);
                    }
                } catch (Exception e) {
                    exportLog.error("Error acking export buffer", e);
                } catch (Error e) {
                    VoltDB.crashLocalVoltDB("Error acking export buffer", true, e);
                }
            }
        };

        stashOrSubmitTask(runnable, true, false);
    }

     private void ackImpl(long uso) {

        if (uso == Long.MIN_VALUE && m_onDrain != null) {
            m_onDrain.run();
            return;
        }

        //Process the ack if any and add blocks to the delete list or move the released USO pointer
        if (uso > 0) {
            try {
                releaseExportBytes(uso);
            } catch (IOException e) {
                VoltDB.crashLocalVoltDB("Error attempting to release export bytes", true, e);
                return;
            }
        }
    }

    /**
     * Returns if replica was running.
     * @return
     */
    public boolean setMaster() {
        exportLog.info("Setting master for partition: " + getPartitionId() + " Table " + getTableName() + " Replica running " + m_replicaRunning);
        m_isMaster = true;
        boolean rval = m_replicaRunning;
        m_replicaRunning = false;
        return rval;
    }

    //Is this a run everywhere source
    public boolean isRunEveryWhere() {
        return m_runEveryWhere;
    }

    /**
     * Trigger an execution of the mastership runnable by the associated
     * executor service
     */
    public synchronized void acceptMastership() {
        Preconditions.checkNotNull(m_onMastership, "mastership runnable is not yet set");
        if (m_mastershipAccepted) {
            exportLog.info("Export generation " + getGeneration() + " Table " + getTableName() + " mastership already accepted for partition " + getPartitionId());
            return;
        }
        exportLog.info("Accepting mastership for export generation " + getGeneration() + " Table " + getTableName() + " partition " + getPartitionId());
        m_mastershipAccepted = true;
        RunnableWithES runnable = new RunnableWithES("acceptMastership") {
            @Override
            public void run() {
                try {
                    if (!getLocalExecutorService().isShutdown() || !m_closed) {
                        exportLog.info("Export generation " + getGeneration() + " Table " + getTableName() + " accepting mastership for partition " + getPartitionId());
                        m_onMastership.run();
                    }
                } catch (Exception e) {
                    exportLog.error("Error in accepting mastership", e);
                }
            }
        };
        stashOrSubmitTask(runnable, true, false);
    }

    /**
     * set the runnable task that is to be executed on mastership designation
     * @param toBeRunOnMastership a {@link @Runnable} task
     */
    public void setOnMastership(Runnable toBeRunOnMastership) {
        Preconditions.checkNotNull(toBeRunOnMastership, "mastership runnable is null");
        m_onMastership = toBeRunOnMastership;
    }

    public ExportFormat getExportFormat() {
        return m_format;
    }

    //Set it from client.
    public void setRunEveryWhere(boolean runEveryWhere) {
        m_runEveryWhere = runEveryWhere;
    }

    private ListenableFuture<?> stashOrSubmitTask(RunnableWithES runnable, final boolean callExecute, final boolean setupTask) {
        if (m_executor==null) {
            synchronized (m_executorLock) {
                if (m_executor==null) {
                    // Bound the queue. It shouldn't get to this high value.
                    // Log an error so that we know if it does get to the high value.
                    if (m_queuedActions.size() > 50) {

                        StringBuilder builder = new StringBuilder();
                        builder.append("Export task queue is filled up to: " + m_queuedActions.size());
                        builder.append(". Not queueing anymore events beyond 50 for generation " + m_generation);
                        builder.append(" and table " + m_tableName + ". The queue contains the following tasks:\n");
                        for (RunnableWithES queuedR : m_queuedActions) {
                            builder.append(queuedR.getTaskName() + "\t");
                         }

                        exportLog.warn(builder.toString());

                        return Futures.immediateFuture(null);
                    }
                    if (setupTask) {
                        m_firstAction = runnable;
                    } else {
                        m_queuedActions.add(runnable);
                    }
                    return Futures.immediateFuture(null);
                }
            }
        }

        // If we got here executor is not null and this generation is active
        runnable.setExecutorService(m_executor);

        if (m_executor.isShutdown()) {
            return Futures.immediateFuture(null);
        }
        if (callExecute) {
            m_executor.execute(runnable);
            return Futures.immediateFuture(null);
        } else {
            return m_executor.submit(runnable);
        }
    }

    public void setupExecutor() {
        if (m_executor!=null) {
            return;
        }

        synchronized(m_executorLock) {
            if (m_executor==null) {
                ListeningExecutorService es = CoreUtils.getListeningExecutorService(
                            "ExportDataSource gen " + m_generation
                            + " table " + m_tableName + " partition " + m_partitionId, 1);
                //If we have a truncate task do that first.
                if (m_firstAction != null) {
                    exportLog.info("Submitting truncate task for ExportDataSource gen " + m_generation
                            + " table " + m_tableName + " partition " + m_partitionId);
                    es.submit(m_firstAction);
                }
                if (m_queuedActions.size()>0) {
                    for (RunnableWithES queuedR : m_queuedActions) {
                        queuedR.setExecutorService(es);
                        es.submit(queuedR);
                    }
                    m_queuedActions.clear();
                }
                m_executor = es;
            }
        }
    }

    public ListeningExecutorService getExecutorService() {
        return m_executor;
    }

    private abstract class RunnableWithES implements Runnable {

        private final String m_taskName;

        private ListeningExecutorService m_executorService;

        public RunnableWithES(String taskName){
            m_taskName = taskName;
        }
        public void setExecutorService(ListeningExecutorService executorService) {
            m_executorService = executorService;
        }

        public ListeningExecutorService getLocalExecutorService() {
            return m_executorService;
        }

        public String getTaskName() {
            return m_taskName;
        }
    }
}
