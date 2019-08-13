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

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32C;
import org.apache.zookeeper_voltpatches.AsyncCallback;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Bits;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.InstanceId;
import org.voltcore.utils.LatencyWatchdog;
import org.voltcore.utils.Pair;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.VoltDB;
import org.voltdb.iv2.TransactionTask;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.utils.PosixAdvise;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Stopwatch;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

import vanilla.java.affinity.impl.PosixJNAAffinity;

public final class DurableCursor {
    private static final VoltLogger log = new VoltLogger("HOST");

    private final File m_exportPath;
    private final long m_syncInterval;
    private boolean m_initialized = false;
    private int m_checksum = 0;
    private int m_currentWriter = 0;
    private ReadWriteLock m_checksumLock = new ReentrantReadWriteLock();

    private static final int MIN_SUPPORTED_PAGE_SIZE = 1024;
    // Checksum of all slots to verify file integrity (4 Bytes) and
    // offset in bytes to the slot mapper data in the file (4 Bytes)
    private static final int FILE_HEADER_SIZE = 8;
    private static final short FILE_VERSION = 1;

    private class DurabilityMarkerFile {
        private File m_durabilityMarker;
        private FileDescriptor m_fd;
        private FileChannel m_channel;
        private volatile boolean m_staleSlotMapper = true;

        DurabilityMarkerFile(File marker) throws IOException {
            if (!marker.isDirectory()) {
                throw new IOException("Path " + marker + " does not exist");
            }
            if (!marker.canRead()) {
                throw new IOException("Path " + marker + " is not readable");
            }
            if (!marker.canWrite()) {
                throw new IOException("Path " + marker + " is not writable");
            }
            if (!marker.canExecute()) {
                throw new IOException("Path " + marker + " is not executable");
            }
            m_durabilityMarker = marker;
            RandomAccessFile ras;
            ras = new RandomAccessFile(m_durabilityMarker, "w");
            m_channel = ras.getChannel();
            m_fd = ras.getFD();
        }

        void writeSlots() {
            if (m_staleSlotMapper) {

            }
        }

        void markSlotMapperStale() {
            m_staleSlotMapper = true;
        }
    }

    interface SymmetricChecksum {
        final AtomicInteger m_checksum = new AtomicInteger(0);

        int get();
    }

    static class SymmetricLongChecksum implements SymmetricChecksum {

        void swap(long oldVal, long newVal) {
            long diff = newVal-oldVal;
            if (diff == 0) {
                return;
            }
            assert(diff > 0 && diff < (1 << 31));
            int collapsed = (int) (diff & 0xffffffff);
            m_checksum.addAndGet(collapsed);
        }

        // Used to reset the slot after the stream is dropped
        void clearVal(long oldVal) {
            int collapsed = (int) (oldVal & 0xffffffff);
            collapsed += ((oldVal >>> 32) & 0xffffffff);
            m_checksum.addAndGet(-collapsed);
        }

        @Override
        public int get() {
            return m_checksum.get();
        }
    }

    private class SlotMapper {
        private final int SLOT_SIZE;
        private final int m_slotBlockSize;
        private final int m_fixedMapBufferHeaderSize;
        int m_maxStreams;
        BBContainer m_slotMapBuffer;
        int m_sparseStreamCount;
        TreeMap<String, Integer> m_streamToSlotBlock = new TreeMap<>();
        TreeMap<Integer, Integer> m_slotBlockToNameOffset = new TreeMap<>();
        Set<Integer> m_localPartitionIds;
        ArrayList<Integer> m_freeSlots = new ArrayList<>();

        SlotMapper(int slotSize, int streamCount, Set<Integer> partitionIds, Long generationId) {
            assert(slotSize % 4 == 0);
            SLOT_SIZE = slotSize;
            m_slotBlockSize = SLOT_SIZE * partitionIds.size();
            updatePageTable(streamCount);
            m_sparseStreamCount = 0;
            m_localPartitionIds = ImmutableSortedSet.copyOf(partitionIds);

            // At a minimum the slotMapBuffer needs a Buffer CRC(4), bufferSize including names(4), generationId(8),
            // version(2), partitionCount(2), partitionList(2 * partitionCount) and maxStreamCount(4)
            m_fixedMapBufferHeaderSize = 4 + 4 + 8 + 2 + (2 * partitionIds.size()) + 4;
        }

        void writeMapBuffer(FileChannel channel, Long generationId) {
            m_slotMapBuffer = DBBPool.allocateUnsafeByteBuffer(m_fixedMapBufferHeaderSize);
            ByteBuffer buff = m_slotMapBuffer.b();
            buff.putInt(0); // CRC
            buff.putInt(0); // total size
            buff.putLong(generationId);
            buff.putShort(FILE_VERSION);
            buff.putShort((short)m_localPartitionIds.size());
            for (int pid : m_localPartitionIds) {
                buff.putShort((short)pid);
            }
            buff.putInt(m_maxStreams);
            for (int ii=0; ii < m_maxStreams; ii++) {
                buff.putInt(0);
            }
            applyCRC(buff);
        }

        private void updatePageTable(int streamCount) {
            int pages = Bits.numPages(streamCount * m_slotBlockSize + FILE_HEADER_SIZE);
            // 12 bytes at beginning of the file is offset to slotMapData (4) and simple checksum (8)
            m_maxStreams = pages * Bits.pageSize() - 12 / m_slotBlockSize;
        }

        void applyCRC(ByteBuffer buff) {
            CRC32 crc = new CRC32();
            int bufferSize = buff.position();
            buff.limit(bufferSize);
            buff.position(4);
            buff.putInt(4, bufferSize);
            crc.reset();
            crc.update(buff);
            buff.position(0);
            buff.putInt(0, (int)crc.getValue());
        }

        void increasePageCount(int streamCount, FileChannel channel, Long generationId) {
            updatePageTable(streamCount);
            writeMapBuffer(channel, generationId);
        }

        int addStream(String streamName, FileChannel channel, Long generationId) {
            Integer block;
            block = m_streamToSlotBlock.get(streamName);
            if (block != null) {
                // streamName already added by another partition
                return block;
            }
            if (m_freeSlots.isEmpty()) {
                block = m_freeSlots.remove(m_freeSlots.size()-1);
            }
            else {
                block = new Integer(m_sparseStreamCount++);
                if (m_sparseStreamCount >= m_maxStreams) {
                    increasePageCount(m_sparseStreamCount, channel, generationId);
                }
            }
            m_streamToSlotBlock.put(streamName, block);
            return block;
        }

        void dropStream(String streamName, Long generationId) {
            Integer block = m_streamToSlotBlock.remove(streamName);
            assert(block != null);
            m_freeSlots.add(block);
        }
    }

    private final Object m_writeThreadCondition = new Object();

    private final Thread m_periodicWriteThread = new Thread(new Runnable() {
        @Override
        public void run() {
            try {
                long lastCurrentTime = System.currentTimeMillis();
                do {
                    long currentTime = System.currentTimeMillis();
                    try {
                        synchronized (m_writeThreadCondition) {
                            m_writeThreadCondition.wait(m_syncInterval + lastCurrentTime - currentTime);
                        }
                    } catch (InterruptedException e) {
                        return;
                    }
                    if (m_initialized) {
                        if (m_currentWriter == 0) {
                            m_currentWriter = 1;
                        }
                        else {
                            m_currentWriter = 0;
                        }
                    }
                    lastCurrentTime = currentTime;
                } while (true);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unexpected exception in log sync thread", true, e);
            }

        }
    }, "Export Cursor Sync Scheduler");

    public DurableCursor(long syncInterval, String exportPath)
    {
        m_syncInterval = syncInterval;
        m_exportPath = new VoltFile(exportPath);
    }

    private final RateLimitedLogger m_writeCollisionLogger =  new RateLimitedLogger(TimeUnit.MINUTES.toMillis(5), log, Level.INFO);

    public void shutdown() throws InterruptedException {
        if (m_syncSchedulerThread.getState() != Thread.State.NEW) {
            m_syncSchedulerThread.interrupt();
            m_syncSchedulerThread.join();
            m_writer.shutdown();

            //Discard pooled memory
            BBContainer cont = null;
            while ((cont = m_bufferPool.poll()) != null) {
                cont.discard();
            }
            if (m_buffer != null) {
                m_buffer.discard();
            }
        }
    }
}
