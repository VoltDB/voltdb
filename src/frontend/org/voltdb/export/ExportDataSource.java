/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
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
import org.voltdb.export.processors.RawProcessor;
import org.voltdb.export.processors.RawProcessor.ExportInternalMessage;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltFile;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;

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
    private final String m_signature;
    private final byte [] m_signatureBytes;
    private final long m_HSId;
    private final long m_generation;
    private final int m_partitionId;
    public final ArrayList<String> m_columnNames = new ArrayList<String>();
    public final ArrayList<Integer> m_columnTypes = new ArrayList<Integer>();
    public final ArrayList<Integer> m_columnLengths = new ArrayList<Integer>();
    private long m_firstUnpolledUso = 0;
    private final StreamBlockQueue m_committedBuffers;
    private boolean m_endOfStream = false;
    private Runnable m_onDrain;
    private Runnable m_onMastership;
    private final ListeningExecutorService m_es;
    private SettableFuture<BBContainer> m_pollFuture;
    private final AtomicReference<Pair<Mailbox, ImmutableList<Long>>> m_ackMailboxRefs =
            new AtomicReference<Pair<Mailbox,ImmutableList<Long>>>(Pair.of((Mailbox)null, ImmutableList.<Long>builder().build()));
    private final Semaphore m_bufferPushPermits = new Semaphore(16);

    private final int m_nullArrayLength;

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
            int partitionId, long HSId, String signature, long generation,
            CatalogMap<Column> catalogMap,
            String overflowPath
            ) throws IOException
            {
        checkNotNull( onDrain, "onDrain runnable is null");

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
        m_es =
                CoreUtils.getListeningExecutorService(
                        "ExportDataSource gen " + m_generation
                        + " table " + m_tableName + " partition " + partitionId, 1);

        String nonce = signature + "_" + HSId + "_" + partitionId;

        m_committedBuffers = new StreamBlockQueue(overflowPath, nonce);

        /*
         * This is not the catalog relativeIndex(). This ID incorporates
         * a catalog version and a table id so that it is constant across
         * catalog updates that add or drop tables.
         */
        m_signature = signature;
        m_signatureBytes = m_signature.getBytes(Constants.UTF8ENCODING);
        m_partitionId = partitionId;
        m_HSId = HSId;

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


        File adFile = new VoltFile(overflowPath, nonce + ".ad");
        exportLog.info("Creating ad for " + nonce);
        assert(!adFile.exists());
        byte jsonBytes[] = null;
        try {
            JSONStringer stringer = new JSONStringer();
            stringer.object();
            stringer.key("hsId").value(m_HSId);
            stringer.key("database").value(m_database);
            writeAdvertisementTo(stringer);
            stringer.endObject();
            JSONObject jsObj = new JSONObject(stringer.toString());
            jsonBytes = jsObj.toString(4).getBytes(Charsets.UTF_8);
        } catch (JSONException e) {
            Throwables.propagate(e);
        }
        FileOutputStream fos = new FileOutputStream(adFile);
        fos.write(jsonBytes);
        fos.getFD().sync();
        fos.close();

        // compute the number of bytes necessary to hold one bit per
        // schema column
        m_nullArrayLength = ((m_columnTypes.size() + 7) & -8) >> 3;
    }

    public ExportDataSource(final Runnable onDrain,
            File adFile
            ) throws IOException {

        /*
         * Certainly no more data coming if this is coming off of disk
         */
        m_endOfStream = true;
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
        try {
            JSONObject jsObj = new JSONObject(new String(data, Charsets.UTF_8));

            long version = jsObj.getLong("adVersion");
            if (version != 0) {
                throw new IOException("Unsupported ad file version " + version);
            }
            m_HSId = jsObj.getLong("hsId");
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
        } catch (JSONException e) {
            throw new IOException(e);
        }

        String nonce = m_signature + "_" + m_HSId + "_" + m_partitionId;
        m_committedBuffers = new StreamBlockQueue(overflowPath, nonce);

        // compute the number of bytes necessary to hold one bit per
        // schema column
        m_nullArrayLength = ((m_columnTypes.size() + 7) & -8) >> 3;
        m_es = CoreUtils.getListeningExecutorService("ExportDataSource gen " + m_generation + " table " + m_tableName + " partition " + m_partitionId, 1);
    }

    public void updateAckMailboxes( final Pair<Mailbox, ImmutableList<Long>> ackMailboxes) {
        m_ackMailboxRefs.set( ackMailboxes);
    }

    private void resetPollMarker() throws IOException {
        if (!m_committedBuffers.isEmpty()) {
            StreamBlock oldestBlock = m_committedBuffers.peek();
            m_firstUnpolledUso = oldestBlock.unreleasedUso();
        }
    }

    private void releaseExportBytes(long releaseOffset) throws IOException {
        // if released offset is in an already-released past, just return success
        if (!m_committedBuffers.isEmpty() && releaseOffset < m_committedBuffers.peek().uso())
        {
            return;
        }

        long lastUso = m_firstUnpolledUso;
        while (!m_committedBuffers.isEmpty() &&
                releaseOffset >= m_committedBuffers.peek().uso()) {
            StreamBlock sb = m_committedBuffers.peek();
            if (releaseOffset >= sb.uso() + sb.totalUso()) {
                m_committedBuffers.pop();
                try {
                    lastUso = sb.uso() + sb.totalUso();
                } finally {
                    sb.deleteContent();
                }
            } else if (releaseOffset >= sb.uso()) {
                sb.releaseUso(releaseOffset);
                lastUso = releaseOffset;
                break;
            }
        }
        m_firstUnpolledUso = Math.max(m_firstUnpolledUso, lastUso);
    }

    private void exportActionImpl(RawProcessor.ExportInternalMessage m) {
        assert(m.m_m.getGeneration() == m_generation);
        ExportProtoMessage message = m.m_m;
        ExportProtoMessage result =
            new ExportProtoMessage(
                    message.getGeneration(), message.m_partitionId, message.m_signature);
        ExportInternalMessage mbp = new ExportInternalMessage(m.m_sb, result);
        StreamBlock first_unpolled_block = null;

        //Assemble a list of blocks to delete so that they can be deleted
        //outside of the m_committedBuffers critical section
        ArrayList<StreamBlock> blocksToDelete = new ArrayList<StreamBlock>();

        boolean hitEndOfStreamWithNoRunnable = false;
        try {
            //Process the ack if any and add blocks to the delete list or move the released USO pointer
            if (message.isAck() && message.getAckOffset() > 0) {
                try {
                    releaseExportBytes(message.getAckOffset());
                } catch (IOException e) {
                    VoltDB.crashLocalVoltDB("Error attempting to release export bytes", true, e);
                    return;
                }
            }

            if (m_endOfStream && m_committedBuffers.sizeInBytes() == 0) {
                if (m_onDrain != null) {
                    m_onDrain.run();
                } else {
                    hitEndOfStreamWithNoRunnable = true;
                }
                return;
            }

            //Reset the first unpolled uso so that blocks that have already been polled will
            //be served up to the next connection
            if (message.isClose()) {
                try {
                    resetPollMarker();
                } catch (IOException e) {
                    exportLog.error(e);
                }
            }

            //Inside this critical section do the work to find out
            //what block should be returned by the next poll.
            //Copying and sending the data will take place outside the critical section
            try {
                if (message.isPoll()) {
                    Iterator<StreamBlock> iter = m_committedBuffers.iterator();
                    while (iter.hasNext()) {
                        StreamBlock block = iter.next();
                        // find the first block that has unpolled data
                        if (m_firstUnpolledUso < block.uso() + block.totalUso()) {
                            first_unpolled_block = block;
                            m_firstUnpolledUso = block.uso() + block.totalUso();
                            break;
                        } else {
                            blocksToDelete.add(block);
                            iter.remove();
                        }
                    }
                }
            } catch (RuntimeException e) {
                if (e.getCause() instanceof IOException) {
                    VoltDB.crashLocalVoltDB("Error attempting to find unpolled export data", true, e);
                } else {
                    throw e;
                }
            }
        } finally {
            //Try hard not to leak memory
            for (StreamBlock sb : blocksToDelete) {
                sb.deleteContent();
            }
            //Cheesy hack for now where we serve info about old
            //data sources from previous generations. In reality accessing
            //this generation is something of an error
            if (hitEndOfStreamWithNoRunnable) {
                ByteBuffer buf = ByteBuffer.allocate(4);
                buf.putInt(0).flip();
                result.pollResponse(m_firstUnpolledUso, buf);
                mbp.m_sb.event(result);
            }
        }

        if (message.isPoll()) {
            //If there are no unpolled blocks return the firstUnpolledUSO with no data
            if (first_unpolled_block == null) {
                ByteBuffer buf = ByteBuffer.allocate(4);
                buf.putInt(0).flip();
                result.pollResponse(m_firstUnpolledUso, buf);
            } else {
                //Otherwise return the block with the USO for the end of the block
                //since the entire remainder of the block is being sent.
                result.pollResponse(
                        first_unpolled_block.uso() + first_unpolled_block.totalUso(),
                        first_unpolled_block.unreleasedBuffer());
            }
            mbp.m_sb.event(result);
        }
    }

    /**
     * Obtain next block of data from source
     */
    public ListenableFuture<?> exportAction(final RawProcessor.ExportInternalMessage m) {
        return m_es.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        exportActionImpl(m);
                    } catch (Exception e) {
                        exportLog.error("Error processing export action", e);
                    } catch (Error e) {
                        VoltDB.crashLocalVoltDB("Error processing export action", true, e);
                    }
                }
        });
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

    public long getHSId() {
        return m_HSId;
    }

    public int getPartitionId() {
        return m_partitionId;
    }

    public void writeAdvertisementTo(JSONStringer stringer) throws JSONException {
        stringer.key("adVersion").value(0);
        stringer.key("generation").value(m_generation);
        stringer.key("partitionId").value(getPartitionId());
        stringer.key("signature").value(m_signature);
        stringer.key("tableName").value(getTableName());
        stringer.key("startTime").value(ManagementFactory.getRuntimeMXBean().getStartTime());
        stringer.key("columns").array();
        for (int ii=0; ii < m_columnNames.size(); ++ii) {
            stringer.object();
            stringer.key("name").value(m_columnNames.get(ii));
            stringer.key("type").value(m_columnTypes.get(ii));
            stringer.key("length").value(m_columnLengths.get(ii));
            stringer.endObject();
        }
        stringer.endArray();
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

        result = Long.signum(m_HSId - o.m_HSId);
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
        result += m_HSId;
        result += m_partitionId;
        // does not factor in replicated / unreplicated.
        // does not factor in column names / schema
        return result;
    }


    public long sizeInBytes() {
        try {
            return m_es.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    return m_committedBuffers.sizeInBytes();
                }
            }).get();
        } catch (Throwable t) {
            Throwables.propagate(t);
            return 0;
        }
    }

    private void pushExportBufferImpl(
            long uso,
            final long bufferPtr,
            ByteBuffer buffer,
            boolean sync,
            boolean endOfStream) {
        final java.util.concurrent.atomic.AtomicBoolean deleted = new java.util.concurrent.atomic.AtomicBoolean(false);
        if (endOfStream) {
            assert(!m_endOfStream);
            assert(bufferPtr == 0);
            assert(buffer == null);
            assert(!sync);

            m_endOfStream = endOfStream;

            if (m_committedBuffers.sizeInBytes() == 0) {
                exportLog.info("Pushed EOS buffer with 0 bytes remaining");
                if (m_pollFuture != null) {
                    m_pollFuture.set(null);
                    m_pollFuture = null;
                }
                if (m_onDrain != null) {
                    m_onDrain.run();
                }
            } else {
                exportLog.info("EOS for " + m_tableName + " partition " + m_partitionId);
            }
            return;
        }
        assert(!m_endOfStream);
        if (buffer != null) {
            if (buffer.capacity() > 0) {
                try {
                    m_committedBuffers.offer(new StreamBlock(
                            new BBContainer(buffer, bufferPtr) {
                                @Override
                                public void discard() {
                                    DBBPool.deleteCharArrayMemory(address);
                                    deleted.set(true);
                                }
                            }, uso, false));
                } catch (IOException e) {
                    exportLog.error(e);
                    if (!deleted.get()) {
                        DBBPool.deleteCharArrayMemory(bufferPtr);
                    }
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
                exportLog.error(e);
            }
        }
        pollImpl(m_pollFuture);
    }

    public void pushExportBuffer(
            final long uso,
            final long bufferPtr,
            final ByteBuffer buffer,
            final boolean sync,
            final boolean endOfStream) {
        try {
            m_bufferPushPermits.acquire();
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    pushExportBufferImpl(uso, bufferPtr, buffer, sync, endOfStream);
                } catch (Exception e) {
                    exportLog.error("Error pushing export buffer", e);
                } catch (Error e) {
                    VoltDB.crashLocalVoltDB("Error pushing export  buffer", true, e);
                } finally {
                    m_bufferPushPermits.release();
                }
            }
        });
    }

    public ListenableFuture<?> closeAndDelete() {
        return m_es.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    m_committedBuffers.closeAndDelete();
                    return null;
                } finally {
                    m_es.shutdown();
                }
            }
        });
    }

    public long getGeneration() {
        return m_generation;
    }

    public ListenableFuture<?> truncateExportToTxnId(final long txnId) {
        return m_es.submit(new Runnable() {
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
                } catch (IOException e) {
                    VoltDB.crashLocalVoltDB("Error while trying to truncate export to txnid " + txnId, true, e);
                }
            }
        });
    }

    public ListenableFuture<?> close() {
        return m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    m_committedBuffers.close();
                } catch (IOException e) {
                    exportLog.error(e);
                } finally {
                    m_es.shutdown();
                }
            }
        });
    }

    public ListenableFuture<BBContainer> poll() {
        final SettableFuture<BBContainer> fut = SettableFuture.create();
        m_es.submit(new Runnable() {
            @Override
            public void run() {
                /*
                 * The poll is blocking through the future, shouldn't
                 * call poll a second time until a response has been given
                 * which nulls out the field
                 */
                if (m_pollFuture != null) {
                    fut.setException(new RuntimeException("Should not poll more than once"));
                    return;
                }
                pollImpl(fut);
            }
        });
        return fut;
    }

    private void pollImpl(SettableFuture<BBContainer> fut) {
        if (fut == null) return;

        try {
            StreamBlock first_unpolled_block = null;

            if (m_endOfStream && m_committedBuffers.sizeInBytes() == 0) {
                //Returning null indicates end of stream
                fut.set(null);
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
                while (iter.hasNext()) {
                    StreamBlock block = iter.next();
                    // find the first block that has unpolled data
                    if (m_firstUnpolledUso < block.uso() + block.totalUso()) {
                        first_unpolled_block = block;
                        m_firstUnpolledUso = block.uso() + block.totalUso();
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
                    sb.deleteContent();
                }
            }

            //If there are no unpolled blocks return the firstUnpolledUSO with no data
            if (first_unpolled_block == null) {
                m_pollFuture = fut;
            } else {
                //Otherwise return the block with the USO for the end of the block
                //since the entire remainder of the block is being sent.
                fut.set(
                        new AckingContainer(first_unpolled_block.unreleasedBufferV2(),
                                first_unpolled_block.uso() + first_unpolled_block.totalUso()));
                m_pollFuture = null;
            }
        } catch (Throwable t) {
            fut.setException(t);
        }
    }

    class AckingContainer extends BBContainer {
        final long m_uso;
        public AckingContainer(ByteBuffer buf, long uso) {
            super(buf, 0L);
            m_uso = uso;
        }

        @Override
        public void discard() {
            try {
                ack(m_uso);
            } finally {
                forwardAckToOtherReplicas(m_uso);
            }
        }

    }

    private void forwardAckToOtherReplicas(long uso) {
        Pair<Mailbox, ImmutableList<Long>> p = m_ackMailboxRefs.get();
        Mailbox mbx = p.getFirst();

        if (mbx != null) {
            // partition:int(4) + length:int(4) +
            // signaturesBytes.length + ackUSO:long(8)
            final int msgLen = 4 + 4 + m_signatureBytes.length + 8;

            ByteBuffer buf = ByteBuffer.allocate(msgLen);
            buf.putInt(m_partitionId);
            buf.putInt(m_signatureBytes.length);
            buf.put(m_signatureBytes);
            buf.putLong(uso);

            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[0], buf.array());

            for( Long siteId: p.getSecond()) {
                mbx.send(siteId, bpm);
            }
        }
    }

    public void ack(final long uso) {
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    ackImpl(uso);
                } catch (Exception e) {
                    exportLog.error("Error acking export buffer", e);
                } catch (Error e) {
                    VoltDB.crashLocalVoltDB("Error acking export buffer", true, e);
                }
            }
        });
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
     * Trigger an execution of the mastership runnable by the associated
     * executor service
     */
    public void acceptMastership() {
        Preconditions.checkNotNull(m_onMastership, "mastership runnable is not yet set");

        m_es.execute(m_onMastership);
    }

    /**
     * set the runnable task that is to be executed on mastership designation
     * @param toBeRunOnMastership a {@link @Runnable} task
     */
    public void setOnMastership(Runnable toBeRunOnMastership) {
        Preconditions.checkNotNull(toBeRunOnMastership, "mastership runnable is null");

        m_onMastership = toBeRunOnMastership;
    }
}
