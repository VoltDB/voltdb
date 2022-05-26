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

package org.voltdb.rejoin;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.messaging.VoltDbMessageFactory;
import org.voltdb.utils.AllocationStrategy;

import com.google_voltpatches.common.base.Preconditions;

/**
 * Stores a binary serialization of an arbitrary number of procedure invocations,
 * all from a single partition in a single time window.
 *
 */
public class RejoinTaskBuffer {
    /**
     *  Default Invocation buffer size
     */
    public static int DEFAULT_BUFFER_SIZE = 1024 * 256;

    private static final VoltLogger log = new VoltLogger("DR");

    public final int partitionId;
    private int compiledSize = 0;
    BBContainer m_container;
    AllocationStrategy m_allocator = null;

    /**
     * The size of the metadata, also the size of an empty buffer
     * @return
     */
    public static int metadataSize() {
        return 4; /* partitionId */
    }

    /**
     * The size of task header
     */
    public static int taskHeaderSize() {
        return 4 /* int task size */
             + 8 /* long source HSID */
             ;
    }

    /**
     * Constructor used for creating a mutable (append-only) buffer of
     * invocations for a partition.
     *
     * @param partitionId
     *            The partition id this buffer belongs to.
     * @param sizeHint
     *            Initial Allocation size Hint
     */
    public RejoinTaskBuffer(int partitionId, int sizeHint) {
        this.partitionId = partitionId;
        this.m_allocator = AllocationStrategy.LR;
        this.m_container = m_allocator.allocate(sizeHint);

        ByteBuffer bb = m_container.b();
        bb.putInt(partitionId);
    }

    /**
     * Constructor used for creating an immutable buffer of invocations from
     * a network message.
     * @param cont Container for byte buffer containing the message data.
     */
    public RejoinTaskBuffer(BBContainer cont) {
        m_container = cont;

        ByteBuffer bb = m_container.b();

        compiledSize = bb.limit();
        partitionId = bb.getInt();

        m_container = cont;

        if (log.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("Constructing buffer:");
            ByteBuffer dup = m_container.bDR();
            dup.position(0);
            while (dup.hasRemaining()) {
                sb.append(" ").append(dup.get());
            }
            log.trace(sb.toString());
        }
    }

    public int size() {
        if (compiledSize == 0) {
            return m_container.b().position();
        } else {
            return compiledSize;
        }
    }

    public boolean isReadOnly() {
        return compiledSize > 0;
    }

    /**
     * Expand the underlying byte buffer to contain the specified delta
     * @param delta amount needed to accommodate the next buffer append
     */
    private void ensureCapacity(int delta) {
        ByteBuffer bb = m_container.b();
        int bufferDelta = (bb.position() + delta) - bb.capacity();
        if (bufferDelta > 0) {
            BBContainer ncntnr = DBBPool.allocateUnsafeByteBuffer(bb.capacity() + bufferDelta);

            ByteBuffer newbb = ncntnr.b();
            bb.flip();
            newbb.put(bb);

            m_container.discard();
            m_container = ncntnr;
        }
    }

    /**
     * Appends a task message to the buffer.
     * @param sourceHSId
     * @param task
     * @throws IOException If the buffer is not of the type TASK
     * @return how many bytes are left in this buffer for adding a new task
     */
    public int appendTask(long sourceHSId, TransactionInfoBaseMessage task) throws IOException {
        Preconditions.checkState(compiledSize == 0, "buffer is already compiled");

        final int msgSerializedSize = task.getSerializedSize();
        ensureCapacity(taskHeaderSize() + msgSerializedSize);

        ByteBuffer bb = m_container.b();
        bb.putInt(msgSerializedSize);
        bb.putLong(sourceHSId);

        int limit = bb.limit();
        bb.limit(bb.position() + msgSerializedSize);
        task.flattenToBuffer(bb.slice());
        bb.limit(limit);
        bb.position(bb.position() + msgSerializedSize);

        // Don't allow any further expansion to the underlying buffer
        if (bb.position() + taskHeaderSize() > DEFAULT_BUFFER_SIZE) {
            compile();
            return 0;
        } else {
            return DEFAULT_BUFFER_SIZE - (bb.position() + taskHeaderSize());
        }
    }

    /**
     * Get the next task message in this buffer.
     *
     * @return null if there is no more messages
     * @throws IOException
     *             if message deserialization fails.
     */
    public TransactionInfoBaseMessage nextTask() throws IOException {
        if (!hasMoreEntries()) {
            return null;
        }

        ByteBuffer bb = m_container.b();

        int position = bb.position();
        int length = bb.getInt();
        long sourceHSId = bb.getLong();
        VoltDbMessageFactory factory = new VoltDbMessageFactory();
        /*
         * create a new buffer that just contains the message, deserialization
         * of the messsage may assert on the capacity of the buffer
         */
        final int oldLimit = bb.limit();
        bb.limit(bb.position() + length);
        ByteBuffer slice = bb.slice();
        bb.limit(oldLimit);
        VoltMessage msg = factory.createMessageFromBuffer(slice, sourceHSId);
        // createMessageFromBuffer() doesn't move the position pointer, set it here
        bb.position(position + length + 8 + 4); // sourceHSId + buf len
        return (TransactionInfoBaseMessage) msg;
    }

    /**
     * @return true if has more entries to deserialize, false if the buffer is
     *         not compiled yet or if there is no more entries.
     * @throws IOException
     */
    private boolean hasMoreEntries() throws IOException {
        if (compiledSize == 0) {
            throw new IOException("Close the buffer by calling compile before reading it");
        }
        ByteBuffer bb = m_container.b();
        if (bb.position() < metadataSize()) {
            bb.position(metadataSize());
        }
        return bb.hasRemaining();
    }

    /**
     * Generate the byte array in preparation of moving over a message bus.
     * Idempotent, but not thread-safe. Also changes state to immutable.
     */
    public void compile() {
        if (compiledSize == 0) {
            ByteBuffer bb = m_container.b();
            compiledSize = bb.position();
            bb.flip();
            m_allocator.track(compiledSize);
        }

        if (log.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("Compiling buffer:   ");
            ByteBuffer dup = m_container.bDR();
            while (dup.hasRemaining()) {
                sb.append(" ").append(dup.get());
            }
            log.trace(sb.toString());
        }
    }

    /**
     *
     * @return the {@link BBContainer} containing the compiled
     */
    public BBContainer getContainer() {
        Preconditions.checkState(compiledSize > 0, "invocation buffer in not compiled");
        return m_container;
    }

    @Override
    public String toString() {
        return String.format("TASK BUFFER p=%d", partitionId);
    }

    public void discard() {
        if (m_container != null) {
            m_container.discard();
        }
    }
}
