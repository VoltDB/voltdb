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

package org.voltdb.iv2;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SnapshotCompletionInterest;
import org.voltdb.SnapshotCompletionMonitor.ExportSnapshotTuple;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.rejoin.StreamSnapshotSink.RestoreWork;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.CachedByteBufferAllocator;
import org.voltdb.utils.ProClass;

import com.google_voltpatches.common.util.concurrent.SettableFuture;

public abstract class JoinProducerBase extends SiteTasker {
    protected final int m_partitionId;
    protected final String m_whoami;
    protected final SiteTaskerQueue m_taskQueue;
    protected final CachedByteBufferAllocator m_snapshotBufferAllocator;
    // data transfer snapshot completion monitor
    protected final SettableFuture<SnapshotCompletionInterest.SnapshotCompletionEvent> m_snapshotCompletionMonitor =
            SettableFuture.create();
    protected InitiatorMailbox m_mailbox = null;
    protected long m_coordinatorHsId = Long.MIN_VALUE;
    protected JoinCompletionAction m_completionAction = null;
    protected TaskLog m_taskLog;
    // Stores the name of the views to pause/resume during a rejoin stream snapshot restore process.
    protected String m_commaSeparatedNameOfViewsToPause = null;
    private String m_snapshotNonce = null;

    /**
     * SnapshotCompletionAction waits for the completion
     * notice of m_snapshotNonce and instructs the replay agent
     * that the snapshot completed.
     *
     * Inner class references m_mailbox, m_nonce, m_rejoinCoordinatorHsId.
     */
    protected class SnapshotCompletionAction implements SnapshotCompletionInterest
    {
        private final SettableFuture<SnapshotCompletionEvent> m_future;

        protected SnapshotCompletionAction(SettableFuture<SnapshotCompletionEvent> future)
        {
            m_future = future;
        }

        protected void register()
        {
            getLogger().debug(m_whoami + "registering snapshot completion action");
            VoltDB.instance().getSnapshotCompletionMonitor().addInterest(this);
        }

        private void deregister()
        {
            getLogger().debug(m_whoami + "deregistering snapshot completion action");
            VoltDB.instance().getSnapshotCompletionMonitor().removeInterest(this);
        }

        @Override
        public CountDownLatch snapshotCompleted(SnapshotCompletionEvent event)
        {
            if (event.nonce.equals(m_snapshotNonce) && event.didSucceed) {
                getLogger().debug(m_whoami + "counting down snapshot monitor completion. "
                            + "Snapshot txnId is: " + event.multipartTxnId);
                deregister();

                // Do not re-arm the watchdog.
                // Once all partitions are done, all watchdogs will be canceled.
                // In live rejoin, there may be a window between two partitions
                //  streaming where there is no active watchdog. #TODO
                kickWatchdog(false);

                m_future.set(event);
            } else {
                getLogger().debug(m_whoami
                        + " observed completion of "
                        + (event.didSucceed ? "succeeded" : "failed")
                        + " snapshot: "
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

    private class ReturnToTaskQueueAction implements Runnable
    {
        @Override
        public void run() {
            m_taskQueue.offer(JoinProducerBase.this);
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

    protected boolean shouldAddToViewsToPause(Database db, Table table) {
        return SnapshotUtil.isSnapshotablePersistentTableView(db, table);
    }

    protected void initListOfViewsToPause() {
        // The very first execution of runForRejoin will lead us here.
        StringBuilder commaSeparatedViewNames = new StringBuilder();
        Database db = VoltDB.instance().getCatalogContext().database;
        for (Table table : VoltDB.instance().getCatalogContext().tables) {
            if (shouldAddToViewsToPause(db, table)) {
                // If the table is a snapshotted persistent table view, we will try to
                // temporarily disable its maintenance job to boost restore performance.
                commaSeparatedViewNames.append(table.getTypeName()).append(",");
            }
        }
        // Get rid of the trailing comma.
        if (commaSeparatedViewNames.length() > 0) {
            commaSeparatedViewNames.setLength(commaSeparatedViewNames.length() - 1);
        }
        m_commaSeparatedNameOfViewsToPause = commaSeparatedViewNames.toString();
    }

    // Load the pro task log
    protected static TaskLog initializeTaskLog(String voltroot, int pid)
    {
        // Construct task log and start logging task messages
        File overflowDir = new File(voltroot, "join_overflow");
        return ProClass.newInstanceOf("org.voltdb.rejoin.TaskLogImpl", "Join", ProClass.HANDLER_LOG, pid,
                overflowDir);
    }

    // Received a datablock. Reset the watchdog timer and hand the block to the Site.
    protected void restoreBlock(RestoreWork rejoinWork, SiteProcedureConnection siteConnection)
    {
        kickWatchdog(true);
        rejoinWork.restore(siteConnection);
    }

    // Completed all criteria: Kill the watchdog and inform the site.
    protected void setJoinComplete(SiteProcedureConnection siteConnection,
                                   Map<String, Map<Integer, ExportSnapshotTuple>> exportSequenceNumbers,
                                   Map<Integer, Long> drSequenceNumbers,
                                   Map<Integer, Map<Integer, Map<Integer, DRSiteDrIdTracker>>> allConsumerSiteTrackers,
                                   Map<Byte, byte[]> drCatalogCommands,
                                   Map<Byte, String[]> replicableTables,
                                   boolean requireExistingSequenceNumbers,
                                   long clusterCreateTime)
    {
        siteConnection.setRejoinComplete(m_completionAction, exportSequenceNumbers, drSequenceNumbers,
                allConsumerSiteTrackers, drCatalogCommands, replicableTables, requireExistingSequenceNumbers,
                clusterCreateTime);
    }

    protected void registerSnapshotMonitor(String nonce) {
        m_snapshotNonce = nonce;
        SnapshotCompletionAction interest = new SnapshotCompletionAction(m_snapshotCompletionMonitor);
        interest.register();
    }

    // cancel and maybe rearm the snapshot data-segment watchdog.
    protected abstract void kickWatchdog(boolean rearm);

    public abstract boolean acceptPromotion();

    public abstract void deliver(RejoinMessage message);

    public abstract TaskLog constructTaskLog(String voltroot);

    protected abstract VoltLogger getLogger();

    public void notifyOfSnapshotNonce(String nonce, long snapshotSpHandle) {
        if (nonce.equals(m_snapshotNonce)) {
            getLogger().debug("Started recording transactions after snapshot nonce " + nonce);
            if (m_taskLog != null) {
                m_taskLog.enableRecording(snapshotSpHandle);
            }
        }
    }

    // Based on whether or not we just did real work, return ourselves to the task queue either now
    // or after waiting a few milliseconds
    protected void returnToTaskQueue(boolean sourcesReady)
    {
        if (sourcesReady) {
            // If we've done something meaningful, go ahead and return ourselves to the queue immediately
            m_taskQueue.offer(this);
        } else {
            // Otherwise, avoid spinning too aggressively, so wait a millisecond before requeueing
            VoltDB.instance().scheduleWork(new ReturnToTaskQueueAction(), 1, -1, TimeUnit.MILLISECONDS);
        }
    }
}
