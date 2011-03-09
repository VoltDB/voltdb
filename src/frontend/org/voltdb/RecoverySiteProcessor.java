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

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.voltdb.dtxn.SiteTracker;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.DBBPool.BBContainer;

/**
 * Base class for functionality used during recovery. Derived classes implement
 * one set of functionality for the source partition and another set of functionality for the destination
 *
 */
public abstract class RecoverySiteProcessor {


    /**
     * Pick a buffer length that is big enough to store at least one of the largest size tuple supported
     * in the system (2 megabytes). Add a fudge factor for metadata.
     */
    public static final int m_bufferLength = (1024 * 1024 * 2) + Short.MAX_VALUE;

    /**
     * Keep track of the origin for each buffer so that they can be freed individually
     * as the each thread with ownership of the buffer discard them post recovery complettion.
     */
    protected final HashMap<BBContainer, BBContainer> m_bufferToOriginMap = new HashMap<BBContainer, BBContainer>();
    protected final LinkedBlockingQueue<BBContainer> m_buffers
                                                                            = new LinkedBlockingQueue<BBContainer>();

    /**
     * After the last table has been streamed the buffers should be returned to the global pool
     * This is set to true while the RecoverySiteProcessorSource lock is held
     * and any already returned buffers are then returned to the global pool. Buffers
     * that have not been returned also check this value while the lock is held
     * to ensure that they are returned to the global pool as well.
     */
    protected boolean m_recoveryComplete = false;

    /** Number of buffers to use */
    static final int m_numBuffers = 3;

    protected RecoverySiteProcessor() {
        initializeBufferPool();
    }

    /**
     * doRecoveryWork loops on receiving messages. This interface is invoked
     * to handle non recovery messages.
     * The transaction id is the id of the earliest known txn at the recovering partition.
     * Txn messages with later txnIds should have regular message handling that result in normal priority
     * queue activity. Txn messages with earlier txn ids should acked immediately if they are multi-part
     * fragment tasks or commit notices do prevent the recovering partition from blocking progress.
     */
    public interface MessageHandler {
        public void handleMessage(VoltMessage message, long txnId);
    }

    abstract public void handleSiteFaults(HashSet<Integer> failedSites, SiteTracker tracker);
    abstract public void doRecoveryWork(long currentTxnId);
    abstract public long bytesTransferred();

    private void initializeBufferPool() {
        for (int ii = 0; ii < m_numBuffers; ii++) {
            final BBContainer origin = org.voltdb.utils.DBBPool.allocateDirect(m_bufferLength);
            long bufferAddress = 0;
            if (VoltDB.getLoadLibVOLTDB()) {
                bufferAddress = org.voltdb.utils.DBBPool.getBufferAddress(origin.b);
            }
            final BBContainer buffer = new BBContainer(origin.b, bufferAddress) {
                /**
                 * This method is careful to check if recovery is complete and if it is,
                 * return the buffer to the global pool via its origin rather then returning it to this
                 * pool where it will be leaked.
                 */
                @Override
                public void discard() {
                    synchronized (this) {
                        if (m_recoveryComplete) {
                            m_bufferToOriginMap.remove(this).discard();
                            return;
                        }
                    }
                    m_buffers.offer(this);
                }
            };
            m_bufferToOriginMap.put(buffer, origin);
            m_buffers.offer(buffer);
        }
    }

    abstract public void notifyBlockedOnMultiPartTxn(long currentTxnId);
}
