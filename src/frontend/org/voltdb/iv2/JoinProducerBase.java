/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.iv2;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SnapshotCompletionInterest;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.utils.CachedByteBufferAllocator;
import org.voltdb.utils.MiscUtils;

import com.google.common.util.concurrent.SettableFuture;

public abstract class JoinProducerBase extends SiteTasker {
    protected static final VoltLogger JOINLOG = new VoltLogger("JOIN");

    protected final int m_partitionId;
    protected final String m_whoami;
    protected final SiteTaskerQueue m_taskQueue;
    protected final CachedByteBufferAllocator m_snapshotBufferAllocator;
    protected InitiatorMailbox m_mailbox = null;
    protected long m_coordinatorHsId = Long.MIN_VALUE;
    protected JoinCompletionAction m_completionAction = null;
    protected TaskLog m_taskLog;

    /**
     * SnapshotCompletionAction waits for the completion
     * notice of m_snapshotNonce and instructs the replay agent
     * that the snapshot completed.
     *
     * Inner class references m_mailbox, m_nonce, m_rejoinCoordinatorHsId.
     */
    protected class SnapshotCompletionAction implements SnapshotCompletionInterest
    {
        private final String m_snapshotNonce;
        private final SettableFuture<SnapshotCompletionEvent> m_future;

        protected SnapshotCompletionAction(String nonce, SettableFuture<SnapshotCompletionEvent> future)
        {
            m_snapshotNonce = nonce;
            m_future = future;
        }

        protected void register()
        {
            JOINLOG.debug(m_whoami + "registering snapshot completion action");
            VoltDB.instance().getSnapshotCompletionMonitor().addInterest(this);
        }

        private void deregister()
        {
            JOINLOG.debug(m_whoami + "deregistering snapshot completion action");
            VoltDB.instance().getSnapshotCompletionMonitor().removeInterest(this);
        }

        @Override
        public CountDownLatch snapshotCompleted(SnapshotCompletionEvent event)
        {
            if (event.nonce.equals(m_snapshotNonce)) {
                JOINLOG.debug(m_whoami + "counting down snapshot monitor completion. "
                        + "Snapshot txnId is: " + event.multipartTxnId);
                deregister();
                kickWatchdog(true);
                m_future.set(event);
            } else {
                JOINLOG.debug(m_whoami
                        + " observed completion of irrelevant snapshot nonce: "
                        + event.nonce);
            }
            return null;
        }
    }

    public static abstract class JoinCompletionAction implements Runnable
    {
        protected long m_snapshotTxnId = Long.MIN_VALUE;

        protected void setSnapshotTxnId(long txnId)
        {
            m_snapshotTxnId = txnId;
        }

        public long getSnapshotTxnId()
        {
            return m_snapshotTxnId;
        }
    }

    JoinProducerBase(int partitionId, String whoami, SiteTaskerQueue taskQueue)
    {
        m_partitionId = partitionId;
        m_whoami = whoami;
        m_taskQueue = taskQueue;
        m_snapshotBufferAllocator = new CachedByteBufferAllocator();
    }

    public void setMailbox(InitiatorMailbox mailbox)
    {
        m_mailbox = mailbox;
    }

    // Load the pro task log
    protected static TaskLog initializeTaskLog(String voltroot, int pid)
    {
        // Construct task log and start logging task messages
        File overflowDir = new File(voltroot, "join_overflow");
        Class<?> taskLogKlass =
                MiscUtils.loadProClass("org.voltdb.rejoin.TaskLogImpl", "Join", false);
        if (taskLogKlass != null) {
            Constructor<?> taskLogConstructor;
            try {
                taskLogConstructor = taskLogKlass.getConstructor(int.class, File.class, boolean.class);
                return (TaskLog) taskLogConstructor.newInstance(pid, overflowDir, true);
            } catch (InvocationTargetException e) {
                VoltDB.crashLocalVoltDB("Unable to construct join task log", true, e.getCause());
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unable to construct join task log", true, e);
            }
        }
        return null;
    }

    // Received a datablock. Reset the watchdog timer and hand the block to the Site.
    protected void restoreBlock(Pair<Integer, ByteBuffer> rejoinWork,
                      SiteProcedureConnection siteConnection)
    {
        kickWatchdog(true);

        int tableId = rejoinWork.getFirst();
        ByteBuffer buffer = rejoinWork.getSecond();
        VoltTable table = PrivateVoltTableFactory.createVoltTableFromBuffer(
                buffer.duplicate(), true);

        // Currently, only export cares about this TXN ID.  Since we don't have one handy, and IV2
        // doesn't yet care about export, just use Long.MIN_VALUE. Long.MAX_VALUE is a no-op
        // don't track undo token

        siteConnection.loadTable(Long.MIN_VALUE, tableId, table, false, Long.MAX_VALUE);
    }

    // Completed all criteria: Kill the watchdog and inform the site.
    protected void setJoinComplete(SiteProcedureConnection siteConnection,
                                     Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers)
    {
        kickWatchdog(false);
        siteConnection.setRejoinComplete(m_completionAction, exportSequenceNumbers);
    }

    // cancel and maybe rearm the snapshot data-segment watchdog.
    protected abstract void kickWatchdog(boolean rearm);

    public abstract boolean acceptPromotion();

    public abstract void deliver(RejoinMessage message);

    public abstract TaskLog constructTaskLog(String voltroot);
}
