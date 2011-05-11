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

package org.voltdb.utils;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.VoltDB;
import org.voltdb.logging.VoltLogger;

/**
 * A pool of {@link java.nio.ByteBuffer ByteBuffers} that are
 * allocated with
 * {@link java.nio.ByteBuffer#allocateDirect(int) * ByteBuffer.allocateDirect}.
 * Buffers are stored in Arenas that are powers of 2. The smallest arena is 16 bytes.
 * Arenas will shrink every 60 seconds if some of the memory isn't being used.
 */
public final class DBBPool {

    /**
     * An Arena that maintains allocated memory for a specific size of buffer.
     *
     */
    private static final class Arena {

        /**
         * A DicedBB wraps a larger byte buffer and divides the larger buffer into many smaller buffers
         * based on the allocation size.
         *
         */
        private static final class DicedBB {

            /**
             * A container to hold the reference to the DicedBB/Arena/Pool
             * that this buffer should be released to. The
             * buffer is released into the pool when the discard method is called.
             * A DicedBB will loan out consecutive slices until all slices are loaned out.
             * Once all slices are loaned out the DicedBB is removed from the Arena's
             * available list and is only added back once all slices have been returned.
             */
            private final class DBBContainer extends BBContainer {

                /*
                 * Potential storage for an exception with stack trace
                 * showing where this container was acquired from
                 */
//                public Throwable allocatedForException = null;

                /**
                 * Construct the container with the buffer and address
                 * @param buffer
                 * @param address
                 */
                protected DBBContainer(final ByteBuffer buffer, final long address) {
                    super(buffer, address);
                }

                /**
                 * Return the buffer back to the DicedBB it was allocated from.
                 */
                @Override
                public void discard() {
                    if (b != null) {
                        release(this);
                    }
                }

//                /**
//                 * It is possible to detect buffers not discarded by checking
//                 * to see of the buffer is not null as dicarded would have nulled
//                 * out the field.
//                 */
//                @Override
//                public void finalize() {
//                    if (traceAllocations && b != null) {
//                        System.err.println("DBBContainer was finalized without being released. Probable resource leak");
////                        if (allocatedForException != null) {
////                            System.err.println("Allocated at");
////                            allocatedForException.printStackTrace();
////                        }
//                        System.err.println("From pool " + m_arena.m_pool);
//                        m_arena.m_pool.poolLocation.printStackTrace();
//                        System.err.flush();
//                        VoltDB.crashVoltDB();
//                    }
//                }
            }

            /**
             * Indicates whether this DirectByteBuffer has been used recently
             * where recently is since the last time the Shrinker has walked this pool.
             */
            private boolean lastUsed = true;

            /**
             * Count and index of the next available slice.
             */
            private int m_availableSlices = 0;

            /**
             * The larger ByteBuffer that is the source of the views of the smaller ByteBuffers
             */
            private final BBContainer m_b;

            /**
             * The number of slices this DicedBB contains
             */
            private final int m_numSlices;

            /**
             * The number of loaned out slices that have been returned
             */
            private int m_returnedSlices = 0;

            /**
             * Storage for the ByteBuffers that are views into the larger diced up BB
             */
            private final DBBContainer m_slices[];

            /**
             * Arena that created this DicedBB
             */
            private final Arena m_arena;

            /**
             * Constructor that allocates a buffer of the specified size and then dices it up
             * into allocationSize views.
             * @param size Size of the larger buffer to allocate
             * @param allocationSize Size the larger buffer should be diced into
             */
            private DicedBB(int size, int allocationSize, Arena arena, boolean foundNativeSupport) {
                if (size % allocationSize != 0) {
                    throw new RuntimeException("A ByteBuffer of size " + size +
                            " can't be evenly divided up into units of size " + allocationSize);
                }
                m_arena = arena;
                m_b = arena.m_pool.allocateBuffer(size);
                m_numSlices = size / allocationSize;
                m_availableSlices = m_numSlices - 1;
                m_slices = new DBBContainer[m_numSlices];
                for (int ii = 0; ii < m_numSlices; ii++) {
                    m_b.b.limit(allocationSize * (ii + 1));
                    m_b.b.position(allocationSize * ii);
                    long address = 0;
                    if (foundNativeSupport) {
                        address = getBufferAddress(m_b.b);
                    }
                    m_slices[ii] = new DBBContainer( m_b.b.slice(), address );
                }
            }

            /**
             * Null out all the references to the views in m_slices.
             * m_b is final so it can't be nulled out. It is an error
             * to clear a DicedBB when there are still slices loaned out.
             * The DicedBB should not be used again after it is cleared.
             */
            private final void clear() {
                if (m_availableSlices + m_returnedSlices != m_numSlices - 1) {
                    throw new RuntimeException("Attempted to clear A DicedByteBuffer " +
                            " while some portions were loaned out");
                }
                for (int ii = 0; ii < m_slices.length; ii++) {
                    m_slices[ii] = null;
                }
                m_b.discard();
            }

            /**
             * Returns true if there are slices available to loan out.
             * @return
             */
            private final boolean hasRemaining() {
                return m_availableSlices >= 0;
            }

            /**
             * Get the next slice to loan out. It is an error to try and get a slice when none
             * are available.
             * @return A slice of the larger ByteBuffer
             */
            private final DBBContainer nextSlice() {
                if (m_availableSlices < 0) {
                    throw new RuntimeException("DicedBB has no more available slices");
                }
                lastUsed = true;
                final DBBContainer slice = m_slices[m_availableSlices];
                m_availableSlices--;
                m_arena.m_pool.bytesLoanedLocally += slice.b.capacity();
                return slice;
            }

            /**
             * Release the view slice ByteBuffer in this container back into the
             * DicedBB's list of slices.
             * @param c
             */
            private final void release( final DBBContainer c) {
                synchronized (m_arena.m_pool) {
                    final int returnIndex = (m_numSlices - m_returnedSlices) - 1;
                    final int capacity = c.b.capacity();
                    m_arena.m_pool.bytesLoanedLocally -= capacity;
                    c.b.clear();
                    m_returnedSlices++;

//                    if (traceAllocations) {
//                        final DBBContainer newContainer = new DBBContainer( c.b, c.address);
//                        c.b = null;
//                        c.address = 0;
//                        m_slices[returnIndex] = newContainer;
//                    } else {
                        m_slices[returnIndex] = c;
//                        c.allocatedForException = null;
//                    }
                    /*
                     * When all the Buffers have been returned it is time
                     * to provide this DicedBB back to the arena
                     */
                    if (returnIndex == 0) {
                        m_availableSlices = m_numSlices - 1;
                        m_returnedSlices = 0;
                        m_arena.m_availableDBBs.push(this);
                    }

                }
            }

            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder(1024);
                sb.append("DBB ").append(this.hashCode()).append(" Last used ").append(lastUsed);
                sb.append(" Num slices ").append(m_numSlices);
                sb.append(" Available slices ").append(m_availableSlices);
                sb.append(" returned slices ").append(m_returnedSlices);
                return sb.toString();
            }
        }

        /**
         * Set of all DicedBBs that this arena has created
         */
        private final HashSet<DicedBB> m_allDBBs = new HashSet<DicedBB>();

        /**
         * Size of the allocations this arena provides
         */
        private final int m_allocationSize;

        /**
         * Maximum size in bytes the arena is allowed to grow to.
         */
        private final int m_maxArenaSize;

        /**
         * Total bytes allocated for this arena
         */
        private int m_arenaSize = 0;

        /**
         * Queue of of available diced byte buffers that can provide slices to loan.
         *
         */
        private ArrayDeque<DicedBB> m_availableDBBs = new ArrayDeque<DicedBB>();

        /**
         * Pointer to the pool that this Arena belongs to. The Arena class
         * is static so that instantiations of the nested
         */
        private final DBBPool m_pool;

        private final boolean m_foundNativeSupport;

        /**
         * Construct an Arena that allocates slices of the specified size
         * @param allocationSize
         * @param pool
         */
        public Arena(int allocationSize, int maxArenaSize, DBBPool pool, boolean foundNativeSupport) {
            m_allocationSize = allocationSize;
            m_pool = pool;
            m_maxArenaSize = maxArenaSize;
            m_foundNativeSupport = foundNativeSupport;
        }

        /**
         * Acquire a ByteBuffer of the size that this Arena allocates. Will allocate
         * a new DicedBB if necessary
         * @param minSize Minimum size of the buffer to be allocated. Use to size a heap byte buffer
         *               if the arena has nothing to loan out and is already too large.
         * @return ByteBuffer of the size that this Arena allocates
         */
        public BBContainer acquire(int minSize) {
//            Throwable caughtException = null;
//            if (traceAllocations) {
//                caughtException = new Throwable();
//                caughtException.fillInStackTrace();
//            }

            /*
             *  First attempt to supply the buffer without allocating a
             *  new DicedBB
             */
            final DicedBB dbb = m_availableDBBs.peek();
            if (dbb != null) {
                final DicedBB.DBBContainer c = dbb.nextSlice();
                if (!dbb.hasRemaining()) {
                    m_availableDBBs.poll();
                }
//                if (traceAllocations) {
//                    c.allocatedForException = caughtException;
//                }
                return c;
            }

            if (m_arenaSize > m_maxArenaSize) {
                return DBBPool.wrapBB(ByteBuffer.allocate(minSize));
            }

            /*
             * Create a new DicedBB to provide the slice. The Diced up BB will be MAX_ALLOCATION_SIZE
             * or whatever size is necessary to fit at least 16 slices. If this allocation
             * grows the arena beyond the max size log an error
             */
            int allocationSize = MAX_ALLOCATION_SIZE;
            if ((MAX_ALLOCATION_SIZE / m_allocationSize) < 16) {
                allocationSize = m_allocationSize * 16;
            }
            m_arenaSize += allocationSize;
            if (m_arenaSize > m_maxArenaSize) {
                m_logger.error("Arena " + m_allocationSize + " grew to " + m_arenaSize +
                        " which is greater then the max of " + m_maxArenaSize +
                        ". This could signal a potential leak of ByteBuffers, an inadequately sized arena, or" +
                        " some other shortcoming in the network subsystem");
                System.err.println("Arena " + m_allocationSize + " grew to " + m_arenaSize +
                        " which is greater then the max of " + m_maxArenaSize +
                        ". This could signal a potential leak of ByteBuffers, an inadequately sized arena, or" +
                        " some other shortcoming in the network subsystem");
            }
            final DicedBB newDBB =
                new DicedBB(
                        allocationSize,
                        m_allocationSize,
                        this,
                        m_foundNativeSupport);
            m_allDBBs.add(newDBB);
            final DicedBB.DBBContainer c = newDBB.nextSlice();
            assert(c != null);
            if (newDBB.hasRemaining()) {
                m_availableDBBs.push(newDBB);
            }
//            if (traceAllocations) {
//                c.allocatedForException = caughtException;
//            }
            return c;
        }

        /**
         * Clear all the DicedBBs out of this arena. It is an error
         * to call clear while the Arena has slices loaned out.
         */
        private void clear() {
            //System.err.println("Clearing pool " + this);
            for (DicedBB dbb : m_availableDBBs) {
                dbb.clear();
            }
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder(2048);
            sb.append("\tArena ").append(m_allocationSize).append(" has ").append(m_allDBBs.size());
            sb.append(" DBBs total ").append(" with ");
            sb.append(m_availableDBBs.size()).append(" available\n");
            for (DicedBB dbb : m_allDBBs) {
                sb.append("\t\t").append(dbb.toString()).append('\n');
            }
            return sb.toString();
        }
    }

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

    /**
     * True if the native library with the functionality necessary to retrieve buffer addresses
     * was found.
     */
    private final boolean foundNativeSupport;

    private static final VoltLogger m_logger = new VoltLogger(DBBPool.class.getName());

    /**
     * Boolean that determines whether code that traces and tracks allocations will be run
     * A lot of this code is commented out anyways because of the extra storage required
     * even if the code is compiled out.
     */
    private static final boolean traceAllocations = true;

    /**
     * The maximum Arena size. Must be a power of 2.
     */
    public static final int MAX_ALLOCATION_SIZE = 262144;

    public static final void doShrink() {

    }

    /**
     * Retrieve the native address of a DirectByteBuffer as a long
     * @param b Buffer you want to retrieve the address of
     * @return Native address of the buffer as a long.
     */
    public static native long getBufferAddress( ByteBuffer b );

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
     * @param b Buffer you want to retrieve the CRC32 of
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

    private long bytesAllocatedLocally = 0;
    private long bytesLoanedLocally = 0;

    /**
     * If set to true then this pool will allocate all buffers on the heap and not
     * direct. Useful if a class is expecting to be passed a Pool as an allocator
     * and it would be better to use heap ByteBuffers
     */
    private final boolean m_allocateOnHeap;

    /**
     * Array containing references to the Arenas for each power of 2 allocation size
     * greater then 16
     */
    private final Arena m_arenas[];

    /**
     * Exception containing the stack trace that describes where this pool
     * was instantiated. Used to idenify pools in log messages
     */
    private final Throwable poolLocation = new Throwable();

    /**
     * No arg constructor that initializes a pool with the default number of buffers
     * and buffer size.
     */
    public DBBPool() {
        this(false, false);
    }

    /**
     * Constructor that initializes the pool with the default {@link Arena} sizes.
     * If <code>allocateOnHeap</code> is <code>true</code> the <code>DBBPool</code> will allocate
     * all {@link java.nio.ByteBuffer ByteBuffer}s as {@link java.nio.HeapByteBuffer HeapByteBuffer}s that are not pooled.
     */
    public DBBPool(boolean allocateOnHeap, boolean loadNativeLib) {
        this(allocateOnHeap, new int[] {
                m_defaultMaxArenaSize,//16
                m_defaultMaxArenaSize,//32
                m_defaultMaxArenaSize,//64
                m_defaultMaxArenaSize,//128
                m_defaultMaxArenaSize,//256
                m_defaultMaxArenaSize,//512
                m_defaultMaxArenaSize,//1024
                m_defaultMaxArenaSize,//2048
                m_defaultMaxArenaSize,//4096
                m_defaultMaxArenaSize,//8192
                m_defaultMaxArenaSize,//16384
                m_defaultMaxArenaSize,//32768
                m_defaultMaxArenaSize,//65536
                m_defaultMaxArenaSize,//131072
                m_defaultMaxArenaSize//262144
        },
        loadNativeLib);
    }

    /**
     * Constructor that allows the pool to be configured to perform all allocations on the heap as well
     * as allowing the maximum size of each {@link Arena} to be configured.
     * @param allocateOnHeap Boolean indicating whether the pool should act as a dummy pool that allocates
     *        all buffers as non-pooled heap {@link java.nio.ByteBuffer ByteBuffer}s
     * @param maxArenaSizes Array of integers indicating the maximum size each arena can grow to. Must contain
     *                      values for arenas from powers of 2 from 16 - 262144 e.g. have 15 positive values.
     *                      May be <code>null</code> but not length zero or an incorrect length.
     */
    public DBBPool(boolean allocateOnHeap, int maxArenaSizes[], boolean loadNativeLib) {
        if (loadNativeLib) {
            foundNativeSupport = org.voltdb.EELibraryLoader.loadExecutionEngineLibrary(false);
        } else {
            foundNativeSupport = false;
        }
        m_allocateOnHeap = allocateOnHeap;
        if (maxArenaSizes == null) {
            m_maxArenaSizes = new int[] {
                    m_defaultMaxArenaSize,//16
                    m_defaultMaxArenaSize,//32
                    m_defaultMaxArenaSize,//64
                    m_defaultMaxArenaSize,//128
                    m_defaultMaxArenaSize,//256
                    m_defaultMaxArenaSize,//512
                    m_defaultMaxArenaSize,//1024
                    m_defaultMaxArenaSize,//2048
                    m_defaultMaxArenaSize,//4096
                    m_defaultMaxArenaSize,//8192
                    m_defaultMaxArenaSize,//16384
                    m_defaultMaxArenaSize,//32768
                    m_defaultMaxArenaSize,//65536
                    m_defaultMaxArenaSize,//131072
                    m_defaultMaxArenaSize//262144
            };
        } else {
            m_maxArenaSizes = maxArenaSizes;
        }
        m_arenas = initDBBPool();
    }

    /**
     * Acquire a byte buffer from the pool that has at least <tt>minSize</tt> capacity.
     * If the size is greater then the size of this pools allocation a dummy allocation
     * will be done with a heap buffer
     * @param minSize Minimum capacity in bytes that the <tt>ByteBuffer</tt> must have
     * @return A <tt>DBBContainer</tt> with a <tt>ByteBuffer</tt> that is at least
     *         the minimum size requested.
     */
    public synchronized BBContainer acquire(final int minSize) {
        assert (minSize > 0);
        if (m_allocateOnHeap) {
            return DBBPool.wrapBB(ByteBuffer.allocate(minSize));
        } else {
            if (minSize > MAX_ALLOCATION_SIZE) {
                return DBBPool.wrapBB(ByteBuffer.allocate(minSize));
            }
            return getArenaForAllocation(minSize).acquire(minSize);
        }
    }

    /**
     * Acquire an array of byte buffers from the pool that has at least <tt>minSize</tt> capacity.
     * @param numBuffers Number of buffers.
     * @param minSize Minimum capacity in bytes that the <tt>ByteBuffer</tt> must have
     * @return An array of <tt>DBBContainer</tt> with <tt>ByteBuffers</tt> that are at least
     *         the minimum size requested.
     */
    public synchronized final BBContainer[] acquire(final int numBuffers, final int minSize) {
        BBContainer buffers[] = new BBContainer[numBuffers];
        for (int ii = 0; ii < numBuffers; ii++) {
            if (m_allocateOnHeap) {
                buffers[ii] = DBBPool.wrapBB(ByteBuffer.allocate(minSize));
            } else {
                buffers[ii] = acquire(minSize);
            }
        }
        return buffers;
    }

    /*
     * Create a direct byte buffer of a specified size
     * @param bufferSize Requested size of the buffer in bytes
     * @return A <tt>ByteBuffer</tt> of the requested size.
     */
    private final BBContainer allocateBuffer(final int bufferSize) {
        bytesAllocatedLocally += bufferSize;
        try {
            final BBContainer container = DBBPool.allocateDirect( bufferSize);
            return container;
        } catch (java.lang.OutOfMemoryError e) {
            m_logger.fatal("Total bytes allocated globally before OOM is " + bytesAllocatedGlobally.get(), e);
            VoltDB.crashVoltDB();
        }
        return null;
    }

    public long bytesAllocatedGlobally() {
        return bytesAllocatedGlobally.longValue();
    }

    public long bytesAllocatedLocally() {
        return bytesAllocatedLocally;
    }

    public long bytesLoanedLocally() {
        return bytesLoanedLocally;
    }

    /**
     * Remove all references to DirectByteBuffers allocated by this pool allowing
     * them to be garbage collected. A pool must be cleared before it is garbage collected
     * to prevent false leak detection. All allocations must be returned to the pool
     * before clearing. This strict policy is to ensure that leaks can be detected.
     */
    public synchronized void clear() {
        //System.err.println("Clearing pool " + this);
        for (Arena pa : m_arenas) {
            pa.clear();
        }
    }

    /**
     * Get the Arena that allocates the next largest power of 2 size
     * @param minSize Size of the requested allocation
     * @return Arena that will allocate a Buffer great then or equal to the requested size
     */
    private final Arena getArenaForAllocation(int minSize) {
        int arenaIndex = 28 - Integer.numberOfLeadingZeros(minSize -1);
        return m_arenas[arenaIndex < 0 ? 0 : arenaIndex];
    }

    private static final int m_defaultMaxArenaSize = 67108864;
    /**
     * The maximum size each arena will be allowed to grow to before the arena
     * starts substituting HeapByteBuffers. This will hurt performance but will ensure the server doesn't run
     * out of memory.
     */
    private final int m_maxArenaSizes[];

    /**
     * Init function shared by various constructors. Returns an Array of arenas
     * to assign to m_arenas
     * @return
     */
    @SuppressWarnings("all")
    private final Arena[] initDBBPool() {
        poolLocation.fillInStackTrace();
        assert(((MAX_ALLOCATION_SIZE & (MAX_ALLOCATION_SIZE -1)) == 0));

        int arenaCount = 0;
        for (int ii = 16; ii <= MAX_ALLOCATION_SIZE; ii *= 2) {
            arenaCount++;
        }
        final Arena arenas[] = new Arena[arenaCount];
        arenaCount = 0;
        for (int ii = 16; ii <= MAX_ALLOCATION_SIZE; ii *= 2) {
            arenas[arenaCount] =
                new Arena(ii,
                        m_maxArenaSizes[arenaCount],
                        this,
                        foundNativeSupport);
            arenaCount++;
        }
        return arenas;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(4096);
        sb.append("\nDBBPool: ").append(this.hashCode()).append(" -- ");
        sb.append(" bytes allocated locally ").append(bytesAllocatedLocally);
        sb.append(" bytes allocated globally ").append(bytesAllocatedGlobally);
        sb.append(" bytes loaned locally\n").append(bytesLoanedLocally);
        for (Arena a : m_arenas) {
            sb.append(a.toString()).append("\n");
        }
        return sb.toString();
    }

    private static final HashMap<Integer, ArrayDeque<ByteBuffer>> m_availableBufferStock =
            new HashMap<Integer, ArrayDeque<ByteBuffer>>();

    public static BBContainer allocateDirect(final int capacity) {
        synchronized (m_availableBufferStock) {
            ArrayDeque<ByteBuffer> buffers = m_availableBufferStock.get(capacity);
            ByteBuffer retval = null;
            if (buffers != null) {
                retval = buffers.poll();
            }
            if (retval != null) {
                retval.clear();
            } else {
                bytesAllocatedGlobally.getAndAdd(capacity);
                retval = ByteBuffer.allocateDirect(capacity);
            }
            return new BBContainer(retval, 0) {

                @Override
                public void discard() {
                    synchronized (m_availableBufferStock) {
                        ArrayDeque<ByteBuffer> buffers = m_availableBufferStock.get(b.capacity());
                        if (buffers == null) {
                            buffers = new ArrayDeque<ByteBuffer>();
                            m_availableBufferStock.put(b.capacity(), buffers);
                        }
                        buffers.offer(b);
                    }
                }

            };
        }
    }

    /*
     * Delete a char array that was allocated on the native heap
     */
    public static native void deleteCharArrayMemory(long pointer);

}
