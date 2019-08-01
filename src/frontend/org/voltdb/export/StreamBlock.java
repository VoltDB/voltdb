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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.VoltDB;
import org.voltdb.exportclient.ExportRowSchema;
import org.voltdb.iv2.UniqueIdGenerator;
import org.voltdb.utils.BinaryDequeReader;

/*
 * The stream block has a default reference count of 1 for being in the queue.
 * It acquires another if it is handed of for processing in another thread.
 *
 * This prevents the memory from being deallocated when the data is pushed to disk
 * as part of a sync. In those scenarios we let the processing thread deallocate
 * when it is finished, and the ack will handle deleting the persisted version of the block.
 *
 * For buffers that are disk backed they won't go through that
 * transition without the coop of the processor so there is no race.
 *
 * There are basically two schemes for tracking data, reference counting for managing allocations
 * and acking for the logical stream data and they serve different purposes. Acking determines
 * when the data needs to go away. Reference counting is just there to gracefully handle data being
 * pushed to disk while it is being processed.
 *
 * Interactions with disk based data are serialized through the ExportDataSource thread while
 * freeing the memory can be done safely from either thread because the allocator is concurrent.
 */
public class StreamBlock {

    // start seq number(8) + committed seq number (8) + row count(4) + uniqueId (8)
    public static final int HEADER_SIZE = 28;
    public static final int SEQUENCE_NUMBER_OFFSET = 0;
    public static final int COMMIT_SEQUENCE_NUMBER_OFFSET = 8;
    public static final int ROW_NUMBER_OFFSET = 16;
    public static final int UNIQUE_ID_OFFSET = 20;

    StreamBlock(BinaryDequeReader.Entry<ExportRowSchema> entry, long startSequenceNumber, long committedSequenceNumber,
            int rowCount, long uniqueId, boolean isPersisted) {
        assert(entry != null);
        m_entry  = entry;
        m_startSequenceNumber = startSequenceNumber;
        m_committedSequenceNumber = committedSequenceNumber;
        m_rowCount = rowCount;
        m_uniqueId = uniqueId;
        // The first 20 bytes are space for us to store the sequence number, row count and uniqueId
        // if we end up persisting
        m_entry.getData().position(HEADER_SIZE);
        m_totalSize = m_entry.getData().remaining();
        //The first 8 bytes are space for us to store the sequence number if we end up persisting
        m_isPersisted = isPersisted;
    }

    private final AtomicInteger m_refCount = new AtomicInteger(1);

    /*
     * Call discard on the underlying buffer used for storage
     */
    void discard() {
        final int count = m_refCount.decrementAndGet();
        if (count == 0) {
            m_entry.release();
        } else if (count < 0) {
            VoltDB.crashLocalVoltDB("Broken refcounting in export", true, null);
        }
    }

    ExportRowSchema getSchema() {
        return m_entry.getExtraHeader();
    }

    long startSequenceNumber() {
        return m_startSequenceNumber;
    }

    long lastSequenceNumber() {
        return m_startSequenceNumber + m_rowCount - 1;
    }


    long committedSequenceNumber() {
        return m_committedSequenceNumber;
    }

    /**
     * Returns the sequence number of the first unreleased export row in this block
     */
    long unreleasedSequenceNumber()
    {
        return m_startSequenceNumber + m_releaseOffset + 1;
    }

    int rowCount() {
        return m_rowCount;
    }

    long uniqueId() {
        return m_uniqueId;
    }

    long getTimestamp() {
        return UniqueIdGenerator.getTimestampFromUniqueId(m_uniqueId) * 1000;
    }

    /**
     * Returns the total amount of bytes in the stream
     * @return
     */
    long totalSize() {
        return m_totalSize;
    }

    /**
     * Returns the number of the unreleased rows in this block.
     */
    long unreleasedRowCount()
    {
        return m_rowCount - (m_releaseOffset + 1);
    }

    // The sequence number for export rows up to which are being released
    void releaseTo(long releaseSequenceNumber)
    {
        assert(releaseSequenceNumber >= m_startSequenceNumber);
        m_releaseOffset = (int)(releaseSequenceNumber - m_startSequenceNumber);
        // if it is fully released, we will discard the block
        assert(m_releaseOffset < (m_rowCount - 1));
    }

    boolean isPersisted() {
        return m_isPersisted;
    }

    private final long m_startSequenceNumber;
    private long m_committedSequenceNumber;
    private final int m_rowCount;
    private final long m_uniqueId;
    private final long m_totalSize;
    private BinaryDequeReader.Entry<ExportRowSchema> m_entry;
    // index of the last row that has been released.
    private int m_releaseOffset = -1;

    /*
     * True if this block is still backed by a file and false
     * if the buffer is only stored in memory. No guarantees about fsync though
     */
    private final boolean m_isPersisted;

    BBContainer unreleasedContainer() {
        m_refCount.incrementAndGet();
        return getRefCountingContainer(m_entry.getData().slice().asReadOnlyBuffer());
    }

    private BBContainer getRefCountingContainer(ByteBuffer buf) {
        return new BBContainer(buf) {
            @Override
            public void discard() {
                checkDoubleFree();
                StreamBlock.this.discard();
            }
        };
    }

    /**
     * Put header data at the start of the ByteBuffer and return a container wrapping it for read-only access.
     *
     * Does not increment the refcount, uses the implicit 1 count
     * and should only be called once to get a container for pushing the data to disk.
     */
    BBContainer asBBContainer() {
        ByteBuffer b = m_entry.getData();
        b.order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(SEQUENCE_NUMBER_OFFSET, startSequenceNumber());
        b.putLong(COMMIT_SEQUENCE_NUMBER_OFFSET, committedSequenceNumber());
        b.putInt(ROW_NUMBER_OFFSET, rowCount());
        b.putLong(UNIQUE_ID_OFFSET, uniqueId());
        b.position(SEQUENCE_NUMBER_OFFSET);
        b.order(ByteOrder.BIG_ENDIAN);
        return getRefCountingContainer(b.asReadOnlyBuffer());
    }
}
