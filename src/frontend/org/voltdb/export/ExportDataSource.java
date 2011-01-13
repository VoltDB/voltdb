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
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.nio.ByteOrder;

import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.export.processors.RawProcessor;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.MessagingException;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.DBBPool;
import org.voltdb.export.processors.RawProcessor.ExportInternalMessage;

/**
 *  Allows an ExportDataProcessor to access underlying table queues
 */
public class ExportDataSource implements Comparable<ExportDataSource> {

    private final class StreamBlock {

        private StreamBlock(ByteBuffer buffer, long uso, long bufferPtr) {
            m_buffer = buffer;
            m_uso = uso;
            m_bufferPtr = bufferPtr;
            m_totalUso = m_buffer.capacity() - 4;
        }

        private void deleteContent() {
            DBBPool.deleteCharArrayMemory(m_bufferPtr);
            m_bufferPtr = 0;
            m_buffer = null;
        }

        /**
         * Returns the USO of the first unreleased octet in this block
         */
        private long unreleasedUso()
        {
            return m_uso + m_releaseOffset;
        }

        /**
         * Returns the total amount of data in the USO stream, this excludes the length prefix in the buffer
         * @return
         */
        private long totalUso() {
            return m_totalUso;
        }

        /**
         * Returns the size of the unreleased data in this block.
         * -4 due to the length prefix that isn't part of the USO
         */
        private long unreleasedSize()
        {
            return totalUso() - m_releaseOffset;
        }

        // The USO for octets up to which are being released
        private void releaseUso(long releaseUso)
        {
            assert(releaseUso >= m_uso);
            m_releaseOffset = releaseUso - m_uso;
            assert(m_releaseOffset <= totalUso());
        }

        private final long m_uso;
        private final long m_totalUso;
        private ByteBuffer m_buffer;
        private long m_releaseOffset;
        private long m_bufferPtr;

        public ByteBuffer unreleasedBuffer() {
            ByteBuffer responseBuffer = ByteBuffer.allocate((int)(4 + unreleasedSize()));
            responseBuffer.order(ByteOrder.LITTLE_ENDIAN);
            responseBuffer.putInt((int)unreleasedSize());
            responseBuffer.order(ByteOrder.BIG_ENDIAN);
            m_buffer.position(4 + (int)m_releaseOffset);
            responseBuffer.put(m_buffer);
            responseBuffer.flip();
            return responseBuffer;
        }
    }

    private final String m_database;
    private final String m_tableName;
    private final byte m_isReplicated;
    private final long m_tableId;
    private final int m_siteId;
    private final int m_partitionId;
    public final ArrayList<String> m_columnNames = new ArrayList<String>();
    public final ArrayList<Integer> m_columnTypes = new ArrayList<Integer>();
    private long m_firstUnpolledUso = 0;
    private final ArrayDeque<StreamBlock> m_committedBuffers = new ArrayDeque<StreamBlock>();

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
                         boolean isReplicated,
                         int partitionId, int siteId, long tableId,
                         CatalogMap<Column> catalogMap)
    {
        m_database = db;
        m_tableName = tableName;

        /*
         * coerce true == 1, false == 0 for wire format
         */
        m_isReplicated = (byte)(isReplicated ? 1 : 0);

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
    }

    private void resetPollMarker() {
        if (!m_committedBuffers.isEmpty()) {
            StreamBlock oldestBlock = m_committedBuffers.peek();
            m_firstUnpolledUso = oldestBlock.unreleasedUso();
        }
    }

    private boolean releaseExportBytes(long releaseOffset, ArrayList<StreamBlock> blocksToDelete) {
        // if released offset is in an already-released past, just return success
        if (!m_committedBuffers.isEmpty() && releaseOffset < m_committedBuffers.peek().m_uso)
        {
            return true;
        }

        // if released offset is in the uncommitted bytes, then set up
        // to release everything that is committed
        long committedUso = 0;
        if (m_committedBuffers.isEmpty()) {
            if (m_firstUnpolledUso < releaseOffset)
            {
                m_firstUnpolledUso = releaseOffset;
            }
            return true;
        } else {
            committedUso = m_committedBuffers.peekLast().m_uso + m_committedBuffers.peekLast().totalUso();
        }

        if (releaseOffset > committedUso)
        {
            releaseOffset = committedUso;
        }

        boolean retval = false;
        StreamBlock lastBlock = m_committedBuffers.peekLast();
        if (releaseOffset >= lastBlock.m_uso)
        {
            while (m_committedBuffers.size() > 1) {
                StreamBlock sb = m_committedBuffers.poll();
                blocksToDelete.add(sb);
            }
            assert(lastBlock.unreleasedSize() > 0);
            lastBlock.releaseUso(releaseOffset);
            if (lastBlock.unreleasedSize() == 0) {
                blocksToDelete.add(lastBlock);
                m_committedBuffers.poll();
            }
            retval = true;
        }
        else
        {
            StreamBlock sb = m_committedBuffers.peek();
            while (!m_committedBuffers.isEmpty() && !retval)
            {
                if (releaseOffset >= sb.m_uso + sb.totalUso())
                {
                    m_committedBuffers.pop();
                    blocksToDelete.add(sb);
                    sb = m_committedBuffers.peek();
                }
                else
                {
                    sb.releaseUso(releaseOffset);
                    retval = true;
                }
            }
        }

        if (retval)
        {
            if (m_firstUnpolledUso < releaseOffset)
            {
                m_firstUnpolledUso = releaseOffset;
            }
        }

        return retval;
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
                    if (!releaseExportBytes(message.getAckOffset(), blocksToDelete)) {
                        result.error();
                        ExportManager.instance().queueMessage(mbp);
                        return;
                    }
                }

                //Reset the first unpolled uso so that blocks that have already been polled will
                //be served up to the next connection
                if (message.isClose()) {
                    resetPollMarker();
                }

                //Inside this critical section do the work to find out
                //what block should be returned by the next poll.
                //Copying and sending the data will take place outside the critical section
                if (message.isPoll()) {
                    Iterator<StreamBlock> iter = m_committedBuffers.iterator();
                    while (iter.hasNext()) {
                        StreamBlock block = iter.next();
                        // find the first block that has unpolled data
                        if (m_firstUnpolledUso < block.m_uso + block.totalUso()) {
                            first_unpolled_block = block;
                            m_firstUnpolledUso = block.m_uso + block.totalUso();
                            break;
                        } else {
                            blocksToDelete.add(block);
                            iter.remove();
                        }
                    }
                }
            }
        } finally {
            //Try hard not to accidentally leak memory
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
                        first_unpolled_block.m_uso + first_unpolled_block.totalUso(),
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

    public byte getIsReplicated() {
        return m_isReplicated;
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
        fs.writeByte(getIsReplicated());
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

    public void pushExportBuffer(long uso, long bufferPtr, ByteBuffer buffer) {
        synchronized (m_committedBuffers) {
            m_committedBuffers.offer(new StreamBlock(buffer, uso, bufferPtr));
        }
    }

}
