/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.export;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;

import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.export.processors.RawProcessor;
import org.voltdb.export.processors.RawProcessor.ExportInternalMessage;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.MessagingException;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.VoltFile;

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
    private final long m_tableId;
    private final int m_siteId;
    private final int m_partitionId;
    public final ArrayList<String> m_columnNames = new ArrayList<String>();
    public final ArrayList<Integer> m_columnTypes = new ArrayList<Integer>();
    private long m_firstUnpolledUso = 0;
    private final StreamBlockQueue m_committedBuffers;

    /**
     * Create a new data source.
     * @param db
     * @param tableName
     * @param isReplicated
     * @param partitionId
     * @param siteId
     * @param tableId
     * @param catalogMap
     */
    public ExportDataSource(String db, String tableName,
                         int partitionId, int siteId, long tableId,
                         CatalogMap<Column> catalogMap,
                         String overflowPath) throws IOException
    {
        m_database = db;
        m_tableName = tableName;

        String nonce = tableName + "_" + tableId + "_" + siteId + "_" + partitionId;

        m_committedBuffers = new StreamBlockQueue(overflowPath, nonce);

        /*
         * This is not the catalog relativeIndex(). This ID incorporates
         * a catalog version and a table id so that it is constant across
         * catalog updates that add or drop tables.
         */
        m_tableId = tableId;
        m_partitionId = partitionId;
        m_siteId = siteId;

        // Add the Export meta-data columns to the schema followed by the
        // catalog columns for this table.
        m_columnNames.add("VOLT_TRANSACTION_ID");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));

        m_columnNames.add("VOLT_EXPORT_TIMESTAMP");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));

        m_columnNames.add("VOLT_EXPORT_SEQUENCE_NUMBER");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));

        m_columnNames.add("VOLT_PARTITION_ID");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));

        m_columnNames.add("VOLT_SITE_ID");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));

        m_columnNames.add("VOLT_EXPORT_OPERATION");
        m_columnTypes.add(((int)VoltType.TINYINT.getValue()));

        for (Column c : CatalogUtil.getSortedCatalogItems(catalogMap, "index")) {
            m_columnNames.add(c.getName());
            m_columnTypes.add(c.getType());
        }


        File adFile = new VoltFile(overflowPath, nonce + ".ad");
        assert(!adFile.exists());
        FastSerializer fs = new FastSerializer();
        fs.writeInt(m_siteId);
        fs.writeString(m_database);
        writeAdvertisementTo(fs);
        FileOutputStream fos = new FileOutputStream(adFile);
        fos.write(fs.getBytes());
        fos.getFD().sync();
        fos.close();
    }

    public ExportDataSource(String overflowPath, File adFile) throws IOException {
        FileInputStream fis = new FileInputStream(adFile);
        byte data[] = new byte[(int)adFile.length()];
        int read = fis.read(data);
        if (read != data.length) {
            throw new IOException("Failed to read ad file " + adFile);
        }
        FastDeserializer fds = new FastDeserializer(data);

        m_siteId = fds.readInt();
        m_database = fds.readString();

        m_partitionId = fds.readInt();
        m_tableId = fds.readLong();
        m_tableName = fds.readString();
        int numColumns = fds.readInt();
        for (int ii=0; ii < numColumns; ++ii) {
            m_columnNames.add(fds.readString());
            m_columnTypes.add(fds.readInt());
        }

        String nonce = m_tableName + "_" + m_tableId + "_" + m_siteId + "_" + m_partitionId;
        m_committedBuffers = new StreamBlockQueue(overflowPath, nonce);
    }

    private void resetPollMarker() throws IOException {
        if (!m_committedBuffers.isEmpty()) {
            StreamBlock oldestBlock = m_committedBuffers.peek();
            m_firstUnpolledUso = oldestBlock.unreleasedUso();
        }
    }

    private void releaseExportBytes(long releaseOffset, ArrayList<StreamBlock> blocksToDelete) throws IOException {
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
                blocksToDelete.add(sb);
                lastUso = sb.uso() + sb.totalUso();
            } else if (releaseOffset >= sb.uso()) {
                sb.releaseUso(releaseOffset);
                lastUso = releaseOffset;
                break;
            }
        }
        m_firstUnpolledUso = Math.max(m_firstUnpolledUso, lastUso);
    }

    /**
     * Obtain next block of data from source
     * @throws MessagingException
     */
    public void exportAction(RawProcessor.ExportInternalMessage m) throws MessagingException {
        ExportProtoMessage message = m.m_m;
        ExportProtoMessage result = new ExportProtoMessage( message.m_partitionId, message.m_tableId);
        ExportInternalMessage mbp = new ExportInternalMessage(m.m_sb, result);
        StreamBlock first_unpolled_block = null;

        //Assemble a list of blocks to delete so that they can be deleted
        //outside of the m_committedBuffers critical section
        ArrayList<StreamBlock> blocksToDelete = new ArrayList<StreamBlock>();

        try {
            //Perform all interaction with m_committedBuffers under lock
            //because pushExportBuffer may be called from an ExecutionSite at any time
            synchronized (m_committedBuffers) {
                //Process the ack if any and add blocks to the delete list or move the released USO pointer
                if (message.isAck() && message.getAckOffset() > 0) {
                    try {
                        releaseExportBytes(message.getAckOffset(), blocksToDelete);
                    } catch (IOException e) {
                        exportLog.error(e);
                        result.error();
                        ExportManager.instance().queueMessage(mbp);
                        return;
                    }
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
                        exportLog.error(e);
                        result.error();
                        ExportManager.instance().queueMessage(mbp);
                    } else {
                        throw e;
                    }
                }
            }
        } finally {
            //Try hard not to leak memory
            for (StreamBlock sb : blocksToDelete) {
                sb.deleteContent();
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
            ExportManager.instance().queueMessage(mbp);
        }
    }

    public String getDatabase() {
        return m_database;
    }

    public String getTableName() {
        return m_tableName;
    }

    public long getTableId() {
        return m_tableId;
    }

    public int getSiteId() {
        return m_siteId;
    }

    public int getPartitionId() {
        return m_partitionId;
    }

    public void writeAdvertisementTo(FastSerializer fs) throws IOException {
        fs.writeInt(getPartitionId());
        fs.writeLong(getTableId());
        fs.writeString(getTableName());
        fs.writeInt(m_columnNames.size());
        for (int ii=0; ii < m_columnNames.size(); ++ii) {
            fs.writeString(m_columnNames.get(ii));
            fs.writeInt(m_columnTypes.get(ii));
        }
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

        result = (m_siteId - o.m_siteId);
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

    public long sizeInBytes() {
        return m_committedBuffers.sizeInBytes();
    }

    public void pushExportBuffer(long uso, final long bufferPtr, ByteBuffer buffer, boolean sync) {
        final java.util.concurrent.atomic.AtomicBoolean deleted = new java.util.concurrent.atomic.AtomicBoolean(false);
        synchronized (m_committedBuffers) {
            if (buffer != null) {
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
        }
    }

}
