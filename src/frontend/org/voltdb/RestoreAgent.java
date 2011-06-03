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

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.dtxn.TransactionInitiator;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.network.Connection;
import org.voltdb.network.NIOReadStream;
import org.voltdb.network.WriteStream;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.DeferredSerialization;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.Pair;

/**
 * An agent responsible for the whole restore process when the cluster starts
 * up. It performs the following tasks in order,
 *
 * - Try to restore the last snapshot
 * - Try to replay all command logs
 * - Take a snapshot if command logs were replayed to truncate them
 *
 * Once all of these tasks have finished successfully, it will call RealVoltDB
 * to resume normal operation.
 */
public class RestoreAgent implements CommandLogReinitiator.Callback {
    private final static VoltLogger LOG = new VoltLogger("RESTORE");
    // TODO: Nonce for command-log snapshots, TBD
    public final static String CL_NONCE = "command-log";

    private final CatalogContext m_context;
    private final TransactionInitiator m_initiator;
    private final CommandLogReinitiator m_replayAgent;

    private final int[] m_allPartitions;

    // Different states the restore process can be in
    private enum State { RESTORE, REPLAY, TRUNCATE };

    private volatile State m_state = State.RESTORE;
    private final RestoreAdapter m_restoreAdapter = new RestoreAdapter();

    /**
     * A dummy connection to provide to the DTXN. It routes ClientResponses back
     * to the restore agent.
     */
    private class RestoreAdapter implements Connection, WriteStream {
        @Override
        public boolean hadBackPressure() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enqueue(BBContainer c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enqueue(FastSerializable f) {
            handleResponse((ClientResponse) f);
            return true;
        }

        @Override
        public boolean enqueue(FastSerializable f, int expectedSize) {
            return enqueue(f);
        }

        @Override
        public boolean enqueue(DeferredSerialization ds) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enqueue(ByteBuffer b) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int calculatePendingWriteDelta(long now) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getOutstandingMessageCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WriteStream writeStream() {
            return this;
        }

        @Override
        public NIOReadStream readStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void disableReadSelection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enableReadSelection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getHostname() {
            return "";
        }

        @Override
        public long connectionId() {
            return -1;
        }

        @Override
        public void scheduleRunnable(Runnable r) {
        }

        @Override
        public void unregister() {
        }
    }

    public RestoreAgent(CatalogContext context, TransactionInitiator initiator,
                        CommandLogReinitiator replay) {
        m_context = context;
        m_initiator = initiator;
        m_replayAgent = replay;
        m_replayAgent.setCallback(this);

        m_allPartitions = new int[m_context.numberOfPartitions];
        int i = 0;
        for (Partition p : m_context.cluster.getPartitions()) {
            m_allPartitions[i++] = Integer.parseInt(p.getTypeName());
        }
    }

    /**
     * Start the restore process. It will first try to restore from the last
     * snapshot, then replay the logs, followed by a snapshot to truncate the
     * logs.
     */
    public void restore() {
        // If this is null, it must be running test
        if (VoltDB.instance().getHostMessenger() != null) {
            /*
             * If this has the lowest host ID, initiate the snapshot restore
             */
            int lowestSite = m_context.siteTracker.getLowestLiveNonExecSiteId();
            int lowestHost = m_context.siteTracker.getHostForSite(lowestSite);
            if (VoltDB.instance().getHostMessenger().getHostId() == lowestHost) {
                String path = m_context.cluster.getLogconfig().get("log").getInternalsnapshotpath();
                initSnapshotWork(1l, Pair.of("@SnapshotRestore",
                                             new Object[] {path, CL_NONCE}));
            }
        }

        /*
         * A thread to keep on sending fake heartbeats until the restore is
         * complete, or otherwise the RPQ is gonna be clogged.
         */
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (m_state == State.RESTORE) {
                    m_initiator.sendHeartbeat(2);

                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {}
                }
            }
        }).start();
    }

    /**
     * Initiate a snapshot action to truncate the logs. This should only be
     * called by one initiator.
     *
     * @param txnId
     *            The transaction ID of the SPI to generate
     * @param invocation
     *            The invocation used to create the SPI
     */
    private void initSnapshotWork(Long txnId, final Pair<String, Object[]> invocation) {
        Config restore = SystemProcedureCatalog.listing.get(invocation.getFirst());
        Procedure restoreProc = restore.asCatalogProcedure();
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.procName = invocation.getFirst();
        spi.params = new FutureTask<ParameterSet>(new Callable<ParameterSet>() {
            @Override
            public ParameterSet call() throws Exception {
                ParameterSet params = new ParameterSet();
                params.setParameters(invocation.getSecond());
                return params;
            }
        });

        if (txnId == null) {
            m_initiator.createTransaction(-1, "CommandLog", true, spi,
                                          restoreProc.getReadonly(),
                                          restoreProc.getSinglepartition(),
                                          restoreProc.getEverysite(),
                                          m_allPartitions, m_allPartitions.length,
                                          m_restoreAdapter, 0,
                                          EstTime.currentTimeMillis());
        } else {
            m_initiator.createTransaction(-1, "CommandLog", true,
                                          txnId, spi,
                                          restoreProc.getReadonly(),
                                          restoreProc.getSinglepartition(),
                                          restoreProc.getEverysite(),
                                          m_allPartitions, m_allPartitions.length,
                                          m_restoreAdapter, 0,
                                          EstTime.currentTimeMillis());
        }
    }

    private void handleResponse(ClientResponse res) {
        if (res.getStatus() != ClientResponse.SUCCESS) {
            if (m_state == State.RESTORE && res.getStatus() == ClientResponse.USER_ABORT) {
                // Nothing to restore from, may not be an error, start replay
                LOG.info("No snapshot to restore from");
            } else {
                LOG.fatal("Failed to truncate command logs by snapshot: " +
                          res.getStatusString());
                VoltDB.crashVoltDB();
            }
        }

        if (m_state == State.RESTORE) {
            m_replayAgent.replay();
            m_state = State.REPLAY;
        } else if (m_state == State.TRUNCATE) {
            VoltDB.instance().onRestoreCompletion();
        }
    }

    @Override
    public void onReplayCompletion() {
        if (m_replayAgent.hasReplayed()) {
            // If this is null, it must be running test
            if (VoltDB.instance().getHostMessenger() != null) {
                /*
                 * If this has the lowest host ID, initiate the snapshot that
                 * will truncate the logs
                 */
                int lowestSite = m_context.siteTracker.getLowestLiveNonExecSiteId();
                int lowestHost = m_context.siteTracker.getHostForSite(lowestSite);
                if (VoltDB.instance().getHostMessenger().getHostId() == lowestHost) {
                    String path =
                        m_context.cluster.getLogconfig().get("log").getInternalsnapshotpath();
                    initSnapshotWork(null, Pair.of("@SnapshotSave",
                                                   new Object[] {path, CL_NONCE, 1}));
                }

            }

            m_state = State.TRUNCATE;
        } else {
            VoltDB.instance().onRestoreCompletion();
        }
    }
}
