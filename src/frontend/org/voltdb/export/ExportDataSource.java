/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.export.AdvertisedDataSource.ExportFormat;
import org.voltdb.exportclient.ExportClientBase;
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
    private final ExportFormat m_format;
    private long m_firstUnpolledUso = 0;
    private final StreamBlockQueue m_committedBuffers;
    private Runnable m_onMastership;
    private SettableFuture<BBContainer> m_pollFuture;
    private final AtomicReference<Pair<Mailbox, ImmutableList<Long>>> m_ackMailboxRefs =
            new AtomicReference<>(Pair.of((Mailbox)null, ImmutableList.<Long>builder().build()));
    private final Semaphore m_bufferPushPermits = new Semaphore(16);

    private long m_lastReleaseOffset = 0;
    //Set if connector "replicated" property is set to true
    private boolean m_runEveryWhere = false;
    private boolean m_replicaRunning = false;
    //This is released when all mailboxes are set.
    private final Semaphore m_allowAcceptingMastership = new Semaphore(0);
    private volatile boolean m_closed = false;
    private volatile AtomicBoolean m_mastershipAccepted = new AtomicBoolean(false);
    private volatile ListeningExecutorService m_es;
    private final AtomicReference<BBContainer> m_pendingContainer = new AtomicReference<>();
    private volatile boolean m_isInCatalog;
    private volatile boolean m_eos;
    private final Generation m_generation;
    private final File m_adFile;
    private ExportClientBase m_client;

    public final ArrayList<String> m_columnNames = new ArrayList<>();
    public final ArrayList<Integer> m_columnTypes = new ArrayList<>();
    public final ArrayList<Integer> m_columnLengths = new ArrayList<>();
    private String m_partitionColumnName = "";

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

        /*
         * This is not the catalog relativeIndex(). This ID incorporates
         * a catalog version and a table id so that it is constant across
         * catalog updates that add or drop tables.
         */
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

        m_adFile = new VoltFile(overflowPath, nonce + ".ad");
        exportLog.info("Creating ad for " + nonce);
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
            Throwables.propagate(e);
        }

        try (FileOutputStream fos = new FileOutputStream(m_adFile)) {
            fos.write(jsonBytes);
            fos.getFD().sync();
        }
        m_isInCatalog = true;
        m_eos = false;
        m_client = null;
        m_es = CoreUtils.getListeningExecutorService("ExportDataSource for table " + m_tableName + " partition " + m_partitionId, 1);
    }

    public ExportDataSource(Generation generation, File adFile) throws IOException {
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
        //EDS created from adfile is always from disk.
        m_isInCatalog = false;
        m_eos = false;
        m_client = null;
        m_es = CoreUtils.getListeningExecutorService("ExportDataSource for table " + m_tableName + " partition " + m_partitionId, 1);
    }

    public void markInCatalog() {
        m_isInCatalog = true;
    }

    public synchronized void updateAckMailboxes(final Pair<Mailbox, ImmutableList<Long>> ackMailboxes) {
        m_ackMailboxRefs.set( ackMailboxes);
    }

    public void setClient(ExportClientBase client) {
        //TODO prcondition?
        m_client = client;
    }

    public ExportClientBase getClient() {
        return m_client;
    }

    private synchronized void releaseExportBytes(long releaseOffset) throws IOException {
        // if released offset is in an already-released past, just return success
        if (!m_committedBuffers.isEmpty() && releaseOffset < m_committedBuffers.peek().uso()) {
            return;
        }

        long lastUso = m_firstUnpolledUso;
        while (!m_committedBuffers.isEmpty() && releaseOffset >= m_committedBuffers.peek().uso()) {
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

    private void pushExportBufferImpl(
            long uso,
            ByteBuffer buffer,
            boolean sync,
            boolean poll) throws Exception {
        final java.util.concurrent.atomic.AtomicBoolean deleted = new java.util.concurrent.atomic.AtomicBoolean(false);

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
        //If not in catalog means we are doing UAC and push came
        if (poll) {
            try {
                pollImpl(m_pollFuture);
            } catch (RejectedExecutionException ex) {
                //Its ok.
            }
        }
    }


    public void pushEndOfStream() {
        exportLog.info("End of stream for table: " + getTableName() + " partition: " + getPartitionId() + " signature: " + getSignature());
        m_eos = true;
    }

    public void pushExportBuffer(
            final long uso,
            final ByteBuffer buffer,
            final boolean sync) {
        try {
            m_bufferPushPermits.acquire();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }

        if (m_es.isShutdown()) {
            //If we are shutting down push it to PBD
            try {
                pushExportBufferImpl(uso, buffer, sync, false);
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
                            pushExportBufferImpl(uso, buffer, sync, true);
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

    public ListenableFuture<?> truncateExportToTxnId(final long txnId) {
        return m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    m_committedBuffers.truncateToTxnId(txnId);
                } catch (Throwable t) {
                    VoltDB.crashLocalVoltDB("Error while trying to truncate export to txnid " + txnId, true, t);
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

    public void setPendingContainer(BBContainer container) {
        Preconditions.checkNotNull(m_pendingContainer.get() != null, "Pending container must be null.");
        m_pendingContainer.set(container);
    }

    public ListenableFuture<BBContainer> poll() {
        final SettableFuture<BBContainer> fut = SettableFuture.create();
        try {
            m_es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        //If we have anything pending set that before moving to next block.
                        if (m_pendingContainer.get() != null) {
                            fut.set(m_pendingContainer.getAndSet(null));
                            if (m_pollFuture != null) {
                                exportLog.info("picked up work from pending container, set poll future to null");
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

    //If replica we poll from lowest of ack rcvd or last poll point.
    private long getFirstUnpolledUso() {
        return m_firstUnpolledUso;
    }

    private synchronized void pollImpl(SettableFuture<BBContainer> fut) {
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
                forwardAckToOtherReplicas(Long.MIN_VALUE);
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
                final AckingContainer ackingContainer = new AckingContainer(first_unpolled_block.unreleasedContainer(),
                                                                            first_unpolled_block.uso() + first_unpolled_block.totalUso());
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
            try {
                m_es.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            m_backingCont.discard();
                            try {
                                if (!m_es.isShutdown()) {
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
                });
            } catch (RejectedExecutionException rej) {
                  //Don't expect this to happen outside of test, but in test it's harmless
                  exportLog.info("Acking export data task rejected, this should be harmless");
                  m_backingCont.discard();
            }
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
            // signaturesBytes.length + ackUSO:long(8) + 2 bytes for runEverywhere or not + 8 bytes for generation ID.
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
                       ackImpl(uso);
                    }
                } catch (Exception e) {
                    exportLog.error("Error acking export buffer", e);
                } catch (Error e) {
                    VoltDB.crashLocalVoltDB("Error acking export buffer", true, e);
                }
            }
        });
    }

     private void ackImpl(long uso) {

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

    //Is this a run everywhere source
    public boolean isRunEveryWhere() {
        return m_runEveryWhere;
    }

    public synchronized void unacceptMastership() {
        m_onMastership = null;
        m_mastershipAccepted.set(false);
        m_isInCatalog = false;
        m_eos = false;
        // For case where the previous export processor had only row of the first block to process
        // and it completed processing it, poll future is not set to null still. Set it to null to
        // prepare for the new processor polling
        if ((m_pollFuture != null) && (m_pendingContainer.get() == null)) {
            m_pollFuture = null;
        }
    }

    /**
     * Trigger an execution of the mastership runnable by the associated
     * executor service
     */
    public synchronized void acceptMastership() {
        if (m_onMastership == null) {
            exportLog.info("Mastership Runnable not yet set for table " + getTableName() + " partition " + getPartitionId());
            return;
        }
        if (m_mastershipAccepted.get()) {
            exportLog.info("Export table " + getTableName() + " mastership already accepted for partition " + getPartitionId());
            return;
        }
        exportLog.info("Accepting mastership for export table " + getTableName() + " partition " + getPartitionId());
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!m_es.isShutdown() || !m_closed) {
                        exportLog.info("Export table " + getTableName() + " accepting mastership for partition " + getPartitionId());
                        if (m_onMastership != null) {
                            if (m_mastershipAccepted.compareAndSet(false, true)) {
                                m_onMastership.run();
                            }
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
     * @param toBeRunOnMastership a {@link @Runnable} task
     */
    public void setOnMastership(Runnable toBeRunOnMastership, boolean isRunEveryWhere) {
        Preconditions.checkNotNull(toBeRunOnMastership, "mastership runnable is null");
        m_onMastership = toBeRunOnMastership;
        if (isRunEveryWhere) {
            acceptMastership();
        }
    }

    public ExportFormat getExportFormat() {
        return m_format;
    }

    //Set it from client.
    public void setRunEveryWhere(boolean runEveryWhere) {
        m_runEveryWhere = runEveryWhere;
        if (m_runEveryWhere) {
            acceptMastership();
        }
    }

    public ListeningExecutorService getExecutorService() {
        return m_es;
    }

    @Override
    public String toString() {
        return "ExportDataSource for Table " + getTableName() + " at Partition " + getPartitionId();
    }
}
