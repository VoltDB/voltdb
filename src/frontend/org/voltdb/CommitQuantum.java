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
package org.voltdb;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.channels.FileChannel.MapMode;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.messaging.HeartbeatMessage;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.Pair;

/**
 * A commit quantum is a file containing all the initate tasks, membership
 * notices, and heartbeat messages recieved by all the sites on a host. A
 * quantum starts at the txn id of a snapshot (or 0 for the first) and ends at
 * the beginning of the next snapshot.
 *
 */
public class CommitQuantum implements Runnable {
    /**
     * File channel associated for writing commit log data
     */
    private final FileChannel m_fc;

    /**
     * Byte buffer mapped to the log
     */
    private MappedByteBuffer m_buffer;

    /**
     * Where to start mapping the next block of the commit quantum
     */
    private long m_filePosition = Integer.MAX_VALUE;

    /**
     * Mesages that need to be logged are submitted to this queue
     */
    private final LinkedBlockingQueue<Pair< VoltMessage, Mailbox>> m_messagesToCommit =
        new LinkedBlockingQueue<Pair<VoltMessage, Mailbox>>();

    /**
     * These are messages that were written to the mapped byte buffer, but the
     * buffer hasn't been synced so they haven't been marked as durable.
     */
    private ArrayDeque<Pair<VoltMessage, Mailbox>> m_messagesForNextSync = new ArrayDeque<Pair<VoltMessage, Mailbox>>();

    /**
     * Create a thread to act as timer to notify the commit thread of when to sync
     */
    private final Timer m_syncTimer = new Timer("Log sync timer");

    private volatile boolean m_shouldContinue = true;

    private final boolean m_waitForCommit;

    private final File m_file;

    private final Thread m_commitThread;

    private volatile boolean m_needToPerformSync = false;

    /**
     *
     * @param txnId
     * @param commitLogDir
     */
    public CommitQuantum(
            long txnId,
            File commitLogDir,
            int commitInterval,
            boolean waitForCommit) throws IOException {
        m_file = new File(commitLogDir, txnId + ".vcl");
        //m_file.deleteOnExit();//for test purposes
        RandomAccessFile fos = new RandomAccessFile(m_file, "rw");
        m_fc = fos.getChannel();
        m_fc.truncate(0);
        m_buffer = m_fc.map(MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
        m_syncTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (m_needToPerformSync == false) {
                    try {
                        m_needToPerformSync = true;
                        m_messagesToCommit.offer(new Pair<VoltMessage, Mailbox>(new SyncMessage(), null));
                    } catch (Exception e) {
                        //Don't kill the timer with a leaked exception
                        e.printStackTrace();
                    }
                }
            }
        }, commitInterval, commitInterval);
        m_waitForCommit = waitForCommit;
        m_commitThread = new Thread(this, "Commit Quantum: " + m_file.toString());
        m_commitThread.start();
    }

    public void logMessage(VoltMessage message, Mailbox mailbox) {
        assert(mailbox instanceof org.voltdb.messaging.SiteMailbox);
        m_messagesToCommit.offer(Pair.of( message, mailbox));
        if (!m_waitForCommit || (message instanceof HeartbeatMessage)) {
            message.setDurable();
            mailbox.deliver(message);
        }
    }

    public void close() throws InterruptedException {
        m_syncTimer.cancel();
        m_shouldContinue = false;
        m_messagesToCommit.offer(new Pair<VoltMessage, Mailbox>(new SyncMessage(), null));
        m_commitThread.join();
        try {
            m_fc.close();
        } catch (IOException e) {
            //don't care?
        }
    }

    public void delete() {
        m_file.deleteOnExit();
    }

    @Override
    public void run() {
        DBBPool heapPool = new DBBPool(true, false);
        ByteBuffer headerBuffer = ByteBuffer.allocateDirect(8);
        try {
            while (m_shouldContinue) {
                try {
                    Pair<VoltMessage, Mailbox> nextPair = m_messagesToCommit.take();
                    if (nextPair.getFirst() instanceof SyncMessage) {
                        m_buffer.force();
                        //System.err.println("Doing a force with " + m_buffer.remaining() + " remaining");
                        while ((nextPair = m_messagesForNextSync.poll()) != null) {
                            final VoltMessage message = nextPair.getFirst();
                            message.setDurable();
                            nextPair.getSecond().deliver(message);
                        }
                        m_needToPerformSync = false;
                        continue;
                    }
                    final VoltMessage nextMessage = nextPair.getFirst();
                    ByteBuffer messageBuffer = nextMessage.getBuffer();
                    if (messageBuffer == null) {
                        messageBuffer = nextMessage.getBufferForMessaging(heapPool).b;
                    } else {
                        messageBuffer = messageBuffer.duplicate();
                    }
                    messageBuffer.position(VoltMessage.HEADER_SIZE);
                    headerBuffer.clear();
                    headerBuffer.putInt(nextPair.getSecond().getSiteId());
                    headerBuffer.putInt(messageBuffer.remaining());
                    headerBuffer.flip();
                    write(headerBuffer);
                    write(messageBuffer);
                    if (m_waitForCommit && !(nextMessage instanceof HeartbeatMessage)) {
                        m_messagesForNextSync.offer(nextPair);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } finally {
            m_buffer = null;
        }
    }

    private void write(ByteBuffer b) throws IOException {
        if (b.remaining() < m_buffer.remaining()) {
            m_buffer.put(b);
        } else {
            int originalLimit = b.limit();
            b.limit(b.position() + m_buffer.remaining());
            m_buffer.put(b);
            b.limit(originalLimit);
            m_buffer.force();
            m_buffer = m_fc.map( MapMode.READ_WRITE, m_filePosition, Integer.MAX_VALUE);
            m_filePosition += Integer.MAX_VALUE;
            m_buffer.put(b);
        }
    }

    private static class SyncMessage extends VoltMessage {

        @Override
        protected void initFromBuffer() {
            // TODO Auto-generated method stub

        }

        @Override
        protected void flattenToBuffer(DBBPool pool) throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        protected boolean requiresDurabilityP() {
            // TODO Auto-generated method stub
            return false;
        }

    }
}
