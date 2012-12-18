/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltcore.utils;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop_voltpatches.hbase.utils.DirectMemoryUtils;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;

/**
 * A pool of {@link java.nio.ByteBuffer ByteBuffers} that are
 * allocated with
 * {@link java.nio.ByteBuffer#allocateDirect(int) * ByteBuffer.allocateDirect}.
 * Buffers are stored in Arenas that are powers of 2. The smallest arena is 16 bytes.
 * Arenas will shrink every 60 seconds if some of the memory isn't being used.
 */
public final class DBBPool {

    /**
     * Abstract base class for a ByteBuffer container. A container serves to hold a reference
     * to the pool/arena/whatever the ByteBuffer was allocated from and possibly the address
     * of the ByteBuffer if it is a DirectByteBuffer. The container also provides the interface
     * for discarding the ByteBuffer and returning it back to the pool. It is a good practice
     * to discard a container even if it is wrapper for HeapByteBuffer that isn't pooled.
     *
     */
    public static abstract class BBContainer {
        /**
         * Pointer to the location in memory where this buffer is located. Useful if you
         * want to pass it to the native side so it doesn't have to call GetDirectBufferAddress.
         */
        final public long address;

        /**
         * The buffer
         */
        final public ByteBuffer b;

        public BBContainer(ByteBuffer b, long address) {
            this.b = b;
            this.address = address;
        }

        abstract public void discard();
    }

    /**
     * Wrapper for HeapByteBuffers that allows them to pose as ByteBuffers from a pool.
     * @author aweisberg
     *
     */
    private static final class BBWrapperContainer extends BBContainer {
        protected BBWrapperContainer(ByteBuffer b) {
            super( b, 0);
        }

        @Override
        public final void discard() {

        }
    }

    /**
     * Number of bytes allocated globally by DBBPools
     */
    private static AtomicLong bytesAllocatedGlobally = new AtomicLong(0);

    static long getBytesAllocatedGlobally()
    {
        return bytesAllocatedGlobally.get();
    }

    private static final VoltLogger m_logger = new VoltLogger(DBBPool.class.getName());

    /**
     * Retrieve the native address of a DirectByteBuffer as a long
     * @param b Buffer you want to retrieve the address of
     * @return Native address of the buffer as a long.
     */
    public static native long getBufferAddress( ByteBuffer b );

    /**
     * Retrieve the CRC32C value of a DirectByteBuffer as a long
     * The polynomial is different from java.util.zip.CRC32C,
     * and matches the one used by SSE 4.2. hardware CRC instructions.
     * The implementation will use the SSE 4.2. instruction if the native library
     * was compiled with -msse4.2 and there is hardware support, otherwise it falls
     * back to Intel's slicing by 8 algorithm
     * @param b Buffer you want to retrieve the CRC32 of
     * @param offset Offset into buffer to start calculations
     * @param length Length of the buffer to calculate
     * @return CRC32C of the buffer as an int.
     */
    public static native int getBufferCRC32C( ByteBuffer b, int offset, int length);

    /**
     * Retrieve the CRC32C value of a DirectByteBuffer as a long
     * The polynomial is different from java.util.zip.CRC32C,
     * and matches the one used by SSE 4.2. hardware CRC instructions.
     * The implementation will use the SSE 4.2. instruction if the native library
     * was compiled with -msse4.2 and there is hardware support, otherwise it falls
     * back to Intel's slicing by 8 algorithm
     * @param ptr Address of buffer you want to retrieve the CRC32C of
     * @param offset Offset into buffer to start calculations
     * @param length Length of the buffer to calculate
     * @return CRC32C of the buffer as an int.
     */
    public static native int getCRC32C( long ptr, int offset, int length);

    /**
     * Retrieve the CRC32 value of a DirectByteBuffer as a long
     * @param b Buffer you want to retrieve the CRC32 of
     * @param offset Offset into buffer to start calculations
     * @param length Length of the buffer to calculate
     * @return CRC32 of the buffer as an int.
     */
    public static native int getBufferCRC32( ByteBuffer b, int offset, int length);

    /**
     * Retrieve the CRC32 value of a DirectByteBuffer as a long
     * @param ptr Address of buffer you want to retrieve the CRC32 of
     * @param offset Offset into buffer to start calculations
     * @param length Length of the buffer to calculate
     * @return CRC32 of the buffer as an int.
     */
    public static native int getCRC32( long ptr, int offset, int length);

    /**
     * Static factory method to wrap a ByteBuffer in a BBContainer that is not
     * associated with any pool
     * @param b
     */
    public static final BBWrapperContainer wrapBB(ByteBuffer b) {
        return new BBWrapperContainer(b);
    }

    private final long bytesAllocatedLocally = 0;
    private final long bytesLoanedLocally = 0;

    private static final COWMap<Integer, ConcurrentLinkedQueue<BBContainer>> m_pooledBuffers =
            new COWMap<Integer, ConcurrentLinkedQueue<BBContainer>>();

    /*
     * Allocate a DirectByteBuffer from a global lock free pool
     */
    public static BBContainer allocateDirectAndPool(final Integer capacity) {
        ConcurrentLinkedQueue<BBContainer> pooledBuffers = m_pooledBuffers.get(capacity);
        if (pooledBuffers == null) {
            pooledBuffers = new ConcurrentLinkedQueue<BBContainer>();
            if (m_pooledBuffers.putIfAbsent(capacity, pooledBuffers) == null) {
                pooledBuffers = m_pooledBuffers.get(capacity);
            }
        }

        BBContainer cont = pooledBuffers.poll();
        if (cont == null) {
            //Create an origin container
            ByteBuffer b = ByteBuffer.allocateDirect(capacity);
            bytesAllocatedGlobally.getAndAdd(capacity);
            cont = new BBContainer( b, DBBPool.getBufferAddress(b)) {
                @Override
                public void discard() {
                    try {
                        DirectMemoryUtils.destroyDirectByteBuffer(b);
                        bytesAllocatedGlobally.addAndGet(-capacity);
                    } catch (Throwable e) {
                        VoltDB.crashLocalVoltDB("Failed to deallocate direct byte buffer", false, e);
                    }
                }
            };
        }
        final BBContainer origin = cont;
        cont = new BBContainer(origin.b, origin.address) {
            @Override
            public void discard() {
                m_pooledBuffers.get(b.capacity()).offer(origin);
            }
        };
        cont.b.clear();
        return cont;
    }

    /*
     * The only reason to not retrieve the address is that network code shared
     * with the java client shouldn't have a dependency on the native library
     */
    public static BBContainer allocateDirect(final int capacity) {
        final ByteBuffer retval = ByteBuffer.allocateDirect(capacity);
        bytesAllocatedGlobally.getAndAdd(capacity);

        return new BBContainer(retval, 0) {

            @Override
            public void discard() {
                try {
                    DirectMemoryUtils.destroyDirectByteBuffer(retval);
                    bytesAllocatedGlobally.getAndAdd(-capacity);
                } catch (Throwable e) {
                    VoltDB.crashLocalVoltDB("Failed to deallocate direct byte buffer", false, e);
                }
            }

        };
    }

    public static BBContainer allocateDirectWithAddress(final int capacity) {
        final ByteBuffer retval = ByteBuffer.allocateDirect(capacity);
        bytesAllocatedGlobally.getAndAdd(capacity);

        return new BBContainer(retval, DBBPool.getBufferAddress(retval)) {

            @Override
            public void discard() {
                try {
                    DirectMemoryUtils.destroyDirectByteBuffer(retval);
                    bytesAllocatedGlobally.getAndAdd(-capacity);
                } catch (Throwable e) {
                    VoltDB.crashLocalVoltDB("Failed to deallocate direct byte buffer", false, e);
                }
            }

        };
    }

    /*
     * Delete a char array that was allocated on the native heap
     */
    public static native void deleteCharArrayMemory(long pointer);

}
