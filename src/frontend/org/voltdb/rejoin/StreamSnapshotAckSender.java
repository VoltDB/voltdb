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

package org.voltdb.rejoin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedBlockingQueue;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;

/**
 * Sends acks of snapshot blocks to the snapshot sender.
 */
public class StreamSnapshotAckSender implements Runnable {
    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    private final long m_HSId;
    private final SocketChannel m_sock;
    private final LinkedBlockingQueue<Integer> m_blockIndices =
            new LinkedBlockingQueue<Integer>();

    public StreamSnapshotAckSender(SocketChannel sock, long HSId) {
        m_sock = sock;
        m_HSId = HSId;
    }

    public void close() {
        // an index of -1 will terminate the thread
        m_blockIndices.offer(-1);
    }

    /**
     * Ack with a positive block index.
     * @param blockIndex
     */
    public void ack(int blockIndex) {
        m_blockIndices.offer(blockIndex);
    }

    @Override
    public void run() {
        while (true) {
            int blockIndex;
            try {
                blockIndex = m_blockIndices.take();
            } catch (InterruptedException e1) {
                break;
            }

            if (blockIndex == -1) {
                rejoinLog.debug(m_blockIndices.size() + " acks remaining, " +
                        "terminating ack sender");
                // special value of -1 terminates the thread
                break;
            }

            ByteBuffer ack = ByteBuffer.allocate(16);
            ack.putInt(12); // length prefix
            ack.putLong(m_HSId);
            ack.putInt(blockIndex);
            ack.flip();

            while (ack.hasRemaining()) {
                int written = 0;
                try {
                    written = m_sock.write(ack);
                } catch (ClosedByInterruptException ignore) {
                    break;
                } catch (IOException e) {
                    VoltDB.crashLocalVoltDB("Unable to ack snapshot block", true, e);
                }
                if (written == -1) {
                    VoltDB.crashLocalVoltDB("Unable to ack snapshot block", false, null);
                }
            }
        }
    }
}
