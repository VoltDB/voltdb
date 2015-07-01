/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.VoltDB;

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

    public static final int HEADER_SIZE = 8;

    StreamBlock(BBContainer cont, long uso, boolean isPersisted) {
        m_buffer = cont;
        m_uso = uso;
        //The first 8 bytes are space for us to store the USO if we end up persisting
        m_buffer.b().position(HEADER_SIZE);
        m_totalUso = m_buffer.b().remaining();
        m_isPersisted = isPersisted;
    }

    private final AtomicInteger m_refCount = new AtomicInteger(1);

    /*
     * Call discard on the underlying buffer used for storage
     */
    void discard() {
        final int count = m_refCount.decrementAndGet();
        if (count == 0) {
            m_buffer.discard();
            m_buffer = null;
        } else if (count < 0) {
            VoltDB.crashLocalVoltDB("Broken refcounting in export", true, null);
        }
    }

    long uso() {
        return m_uso;
    }

    /**
     * Returns the USO of the first unreleased octet in this block
     */
    long unreleasedUso()
    {
        return m_uso + m_releaseOffset;
    }

    /**
     * Returns the total amount of data in the USO stream
     * @return
     */
    long totalUso() {
        return m_totalUso;
    }

    /**
     * Returns the size of the unreleased data in this block.
     * -4 due to the length prefix that isn't part of the USO
     */
    long unreleasedSize()
    {
        return totalUso() - m_releaseOffset;
    }

    // The USO for octets up to which are being released
    void releaseUso(long releaseUso)
    {
        assert(releaseUso >= m_uso);
        m_releaseOffset = releaseUso - m_uso;
        assert(m_releaseOffset <= totalUso());
    }

    boolean isPersisted() {
        return m_isPersisted;
    }

    private final long m_uso;
    private final long m_totalUso;
    private BBContainer m_buffer;
    private long m_releaseOffset;

    /*
     * True if this block is still backed by a file and false
     * if the buffer is only stored in memory. No guarantees about fsync though
     */
    private final boolean m_isPersisted;

    BBContainer unreleasedContainer() {
        m_refCount.incrementAndGet();
        return getRefCountingContainer(m_buffer.b().slice().asReadOnlyBuffer());
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

    /*
     * Does not increment the refcount, uses the implicit 1 count
     * and should only be called once to get a container for pushing the data to disk
     */
    BBContainer asBBContainer() {
        m_buffer.b().putLong(0, uso());
        m_buffer.b().position(0);
        return getRefCountingContainer(m_buffer.b().asReadOnlyBuffer());
    }
}
