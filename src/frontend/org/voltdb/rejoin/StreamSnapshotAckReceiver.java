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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.exceptions.SerializableException;

import com.google_voltpatches.common.base.Preconditions;

/**
 * Thread that blocks on the receipt of Acks.
 */
public class StreamSnapshotAckReceiver implements Runnable {
    public static interface AckCallback {
        public void receiveAck(int blockIndex);

        public void receiveError(Exception exception);
    }

    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    private final Mailbox m_mb;
    private final StreamSnapshotBase.MessageFactory m_msgFactory;
    private final Map<Long, AckCallback> m_callbacks;
    private final AtomicInteger m_expectedEOFs;
    private volatile boolean stopped = false;
    private volatile Thread m_thread;

    public StreamSnapshotAckReceiver(Mailbox mb)
    {
        this(mb, new StreamSnapshotBase.DefaultMessageFactory());
    }

    public StreamSnapshotAckReceiver(Mailbox mb, StreamSnapshotBase.MessageFactory msgFactory) {
        Preconditions.checkArgument(mb != null);
        m_mb = mb;
        m_msgFactory = msgFactory;
        m_callbacks = Collections.synchronizedMap(new HashMap<Long, AckCallback>());
        m_expectedEOFs = new AtomicInteger();
    }

    public void setCallback(long targetId, AckCallback callback, int expectedAcksForEOF) {
        m_expectedEOFs.addAndGet(expectedAcksForEOF);
        m_callbacks.put(targetId, callback);
    }

    public void forceStop() {
        stopped = true;
        Thread thread = this.m_thread;
        if (thread != null) {
            // Interrupt the thread to wake up the call to recvBlocking
            thread.interrupt();
        }
    }

    @Override
    public void run() {
        m_thread = Thread.currentThread();
        rejoinLog.trace("Starting ack receiver thread");

        try {
            while (true) {
                if (stopped) {
                    rejoinLog.debug("Ack receiver thread stopped");
                    return;
                }

                rejoinLog.trace("Blocking on receiving mailbox");
                VoltMessage msg = m_mb.recvBlocking(10 * 60 * 1000); // Wait for 10 minutes
                if (stopped) {
                    rejoinLog.debug("Ack receiver thread stopped");
                    return;
                }

                if (msg == null) {
                    rejoinLog.warn("No stream snapshot ack message was received in the past 10 minutes" +
                                   " or the thread was interrupted (expected eofs: " + m_expectedEOFs.get() + ")" );
                    continue;
                }

                // TestMidRejoinDeath ignores acks to trigger the watchdog
                if (StreamSnapshotDataTarget.m_rejoinDeathTestMode && (m_msgFactory.getAckTargetId(msg) == 1)) {
                    continue;
                }

                SerializableException se = m_msgFactory.getException(msg);
                if (se != null) {
                    handleException("Received exception in ack receiver", se);
                    return;
                }

                AckCallback ackCallback = m_callbacks.get(m_msgFactory.getAckTargetId(msg));
                if (ackCallback == null) {
                    rejoinLog.warn("Unknown target ID " + m_msgFactory.getAckTargetId(msg)
                            + " in stream snapshot ack message");
                    continue;
                }

                int ackBlockIndex = m_msgFactory.getAckBlockIndex(msg);
                if (ackBlockIndex != -1) {
                    ackCallback.receiveAck(ackBlockIndex);
                }

                if (m_msgFactory.isAckEOS(msg)) {
                    // EOS message indicates end of stream.
                    // The receiver is shared by multiple data targets, each of them will
                    // send an end of stream message, must wait until all end of stream
                    // messages are received before terminating the thread.
                    if (m_expectedEOFs.decrementAndGet() == 0) {
                        return;
                    }
                }
            }
        } catch (Exception e) {
            handleException("Error reading a message from a recovery stream", e);
        } finally {
            m_thread = null;
            rejoinLog.trace("Ack receiver thread exiting");
        }
    }

    private void handleException(String message, Exception exception) {
        rejoinLog.error(message, exception);
        m_callbacks.values().forEach(c -> c.receiveError(exception));
    }

    public boolean isStopped() {
        return stopped;
    }
}
