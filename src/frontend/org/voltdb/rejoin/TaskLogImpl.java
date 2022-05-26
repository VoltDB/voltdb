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

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.VoltDB;
import org.voltdb.utils.BinaryDeque;
import org.voltdb.utils.BinaryDequeReader;
import org.voltdb.utils.PersistentBinaryDeque;

/**
 * A task queue that can overflow to disk.
 */
public class TaskLogImpl implements TaskLog {
    // Overflow limit in MB. Default is 100GB
    private static final long m_overflowLimit =
            Long.parseLong(System.getProperty("REJOIN_OVERFLOW_LIMIT", "102400"));

    private final int m_partitionId;
    private final BinaryDeque<?> m_buffers;
    private final BinaryDequeReader<?> m_reader;
    private RejoinTaskBuffer m_tail = null;
    private RejoinTaskBuffer m_head = null;
    //Not using as a bounded queue
    private final Queue<RejoinTaskBuffer> m_headBuffers = new LinkedBlockingQueue<RejoinTaskBuffer>();
    // The number of tasks in the current buffer
    private int m_taskCount = 0;
    private int m_tasksPendingInCurrentTail = 0;
    private long m_snapshotSpHandle = Long.MAX_VALUE;
    private int m_bufferHeadroom = RejoinTaskBuffer.DEFAULT_BUFFER_SIZE;

    private final ExecutorService m_es;
    private final String m_cursorId;

    public TaskLogImpl(int partitionId, File overflowDir) throws IOException {
        /*
         * Rejoin coordinator should have already cleared everything in the
         * overflow dir. Assume no file name collision will happen.
         *
         * Not clearing the files here because another site might be using its
         * overflow file at the same time.
         */

        if (!overflowDir.exists()) {
            if (!overflowDir.mkdir()) {
                throw new IOException("Cannot create rejoin overflow directory");
            }
        } else if (!overflowDir.canRead() || !overflowDir.canWrite()) {
            throw new IOException("Rejoin overflow directory does not have " +
                    "read or write permissions");
        }

        m_partitionId = partitionId;
        m_cursorId = "TaskLog-" + partitionId;
        m_buffers = PersistentBinaryDeque.builder(Integer.toString(partitionId), overflowDir, new VoltLogger("REJOIN"))
                .build();
        m_reader = m_buffers.openForRead(m_cursorId);
        m_es = CoreUtils.getSingleThreadExecutor("TaskLog partition " + partitionId);
    }

    /**
     * The buffers are bound by the number of tasks in them. Once the current
     * buffer has enough tasks, it will be queued and a new buffer will be
     * created.
     *
     * @throws IOException
     */
    private void bufferCatchup(int messageSize) throws IOException {
        // If the current buffer has too many tasks logged, queue it and
        // create a new one.
        if (m_tail != null && m_tail.size() > 0 && messageSize > m_bufferHeadroom) {
            // compile the invocation buffer
            m_tail.compile();

            final RejoinTaskBuffer boundTail = m_tail;
            final Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        m_buffers.offer(boundTail.getContainer());
                        if (m_reader.sizeInBytes() > m_overflowLimit * 1024 * 1024) {
                            // we can never catch up, should break rejoin.
                            VoltDB.crashLocalVoltDB("On-disk task log is full. Please reduce " +
                                    "workload and try live rejoin again, or use blocking rejoin.");
                        }
                    } catch (Throwable t) {
                        VoltDB.crashLocalVoltDB("Error in task log buffering transactions", true, t);
                    }
                }
            };

            m_es.execute(r);

            // Reset
            m_tail = null;
            m_tasksPendingInCurrentTail = 0;
        }

        // create a new buffer
        if (m_tail == null) {
            m_tail = new RejoinTaskBuffer(m_partitionId, messageSize);
            m_bufferHeadroom = RejoinTaskBuffer.DEFAULT_BUFFER_SIZE;
        }
    }

    @Override
    public void logTask(TransactionInfoBaseMessage message) throws IOException {
        if (message.getSpHandle() <= m_snapshotSpHandle) {
            return;
        }
        if (m_closed) {
            throw new IOException("Closed");
        }

        assert(message != null);
        bufferCatchup(message.getSerializedSize());

        m_bufferHeadroom = m_tail.appendTask(message.m_sourceHSId, message);
        m_taskCount++;
        m_tasksPendingInCurrentTail++;
    }

    private final AtomicInteger m_pendingPolls = new AtomicInteger(0);

    private void scheduleDiscard(RejoinTaskBuffer buffer) {
        final RejoinTaskBuffer b = buffer;
        final Runnable r = new Runnable() {
            @Override
            public void run() {
                b.discard();
            }
        };
        // let service thread deals with I/O
        m_es.execute(r);
    }

    /**
     * Try to get the next task message from the queue.
     *
     * @return the next task message. null will be returned if either
     *         the next task message is not ready or the queue is empty now.
     * @throws IOException
     *             If failed to pull the next message out of the queue.
     */
    @Override
    public TransactionInfoBaseMessage getNextMessage() throws IOException {
        if (m_closed) {
            throw new IOException("Closed");
        }
        if (m_head == null) {
            // Get another buffer asynchronously
            final Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        BBContainer cont = m_reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                        if (cont != null) {
                           m_headBuffers.offer(new RejoinTaskBuffer(cont));
                        }
                    } catch (Throwable t) {
                        VoltDB.crashLocalVoltDB("Error retrieving buffer data in task log", true, t);
                    } finally {
                        m_pendingPolls.decrementAndGet();
                    }
                }
            };

            //Always keep three buffers ready to go
            for (int ii = m_pendingPolls.get() + m_headBuffers.size(); ii < 3; ii++) {
                m_pendingPolls.incrementAndGet();
                m_es.execute(r);
            }

            m_head = m_headBuffers.poll();
        }

        TransactionInfoBaseMessage nextTask = null;
        if (m_head != null) {
            nextTask = m_head.nextTask();
            if (nextTask == null) {
                scheduleDiscard(m_head);
                // current buffer is completely consumed, move to the next
                m_head = null;
            } else {
                m_taskCount--;
            }
        } else if ((m_taskCount - m_tasksPendingInCurrentTail == 0) && m_tail != null) {
            m_tasksPendingInCurrentTail = 0;
            /*
             * there is only one buffer left which hasn't been pushed into the
             * queue yet. set it to head directly, short-circuiting the queue.
             */
            m_tail.compile();
            if (m_head != null) {
                scheduleDiscard(m_head);
            }
            m_head = m_tail;
            m_tail = null;
            nextTask = getNextMessage();
        }

        // SPs or fragments that's before the actual snapshot fragment may end up in the task log,
        // because there can be multiple snapshot fragments enabling the task log due to snapshot
        // collision. Need to filter tasks here based on their spHandles.
        if (nextTask != null && nextTask.getSpHandle() > m_snapshotSpHandle) {
            return nextTask;
        } else {
            return null;
        }
    }

    @Override
    public boolean isEmpty() {
        return m_taskCount < 1;
    }

    @Override
    public void close() throws IOException {
        close(false);
    }

    private boolean m_closed = false;
    public void close(boolean synchronous) throws IOException {
        if (m_closed) {
            return;
        }
        m_closed = true;
        m_es.shutdown();
        if (synchronous) {
            try {
                m_es.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        if (m_buffers != null) {
            m_buffers.closeAndDelete();
        }
        if (m_head != null) {
            m_head.discard();
        }
        if (m_tail != null) {
            m_tail.discard();
        }
        for (RejoinTaskBuffer buf : m_headBuffers) {
            buf.discard();
        }
    }

    @Override
    public void enableRecording(long snapshotSpHandle) {
        m_snapshotSpHandle = snapshotSpHandle;
    }
}
