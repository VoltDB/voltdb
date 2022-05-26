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

package org.voltdb;

import com.google_voltpatches.common.collect.ImmutableMap;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs;
import org.voltcore.logging.VoltLogger;
import org.voltcore.zk.ZKUtil;
import org.voltdb.sysprocs.saverestore.SnapshotWritePlan;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Wraps deferred snapshot setup work and run it on the snapshot IO agent thread.
 */
public class DeferredSnapshotSetup implements Callable<DeferredSnapshotSetup> {
    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    private final SnapshotWritePlan m_plan;
    private final Callable<Boolean> m_deferredSetup;
    private final long m_txnId;
    private final Map<Integer, Long> m_partitionTransactionIds;

    private volatile Exception m_error = null;

    public DeferredSnapshotSetup(SnapshotWritePlan plan, Callable<Boolean> deferredSetup, long txnId,
                                 final Map<Integer, Long> partitionTransactionIds)
    {
        m_plan = plan;
        m_deferredSetup = deferredSetup;
        m_txnId = txnId;
        m_partitionTransactionIds = ImmutableMap.copyOf(partitionTransactionIds);
    }

    public SnapshotWritePlan getPlan()
    {
        return m_plan;
    }

    public Exception getError()
    {
        return m_error;
    }

    @Override
    public DeferredSnapshotSetup call() throws Exception
    {
        if (m_deferredSetup != null) {
            try {
                m_deferredSetup.call();
            } catch (Exception e) {
                m_error = e;
                SNAP_LOG.error("Failed to run deferred snapshot setup", e);

                // Data target creation failed, close all created ones and replace them with DevNull.
                m_plan.createAllDevNullTargets(m_error);
            }
        }

        /*
         * Inform the SnapshotCompletionMonitor of what the partition specific txnids for
         * this snapshot were so it can forward that to completion interests.
         */
        VoltDB.instance().getSnapshotCompletionMonitor().registerPartitionTxnIdsForSnapshot(
                m_txnId, m_partitionTransactionIds);

        // Provide the truncation request ID so the monitor can recognize a specific snapshot.
        logSnapshotStartToZK(m_txnId);

        return this;
    }

    private static void logSnapshotStartToZK(long txnId) {
        /*
         * Going to send out the requests async to make snapshot init move faster
         */
        ZKUtil.StringCallback cb1 = new ZKUtil.StringCallback();

        /*
         * Log that we are currently snapshotting this snapshot
         */
        try {
            //This node shouldn't already exist... should have been erased when the last snapshot finished
            assert(VoltDB.instance().getHostMessenger().getZK().exists(
                    VoltZK.nodes_currently_snapshotting + "/" + VoltDB.instance().getHostMessenger().getHostId(), false)
                   == null);
            ByteBuffer snapshotTxnId = ByteBuffer.allocate(8);
            snapshotTxnId.putLong(txnId);
            VoltDB.instance().getHostMessenger().getZK().create(
                    VoltZK.nodes_currently_snapshotting + "/" + VoltDB.instance().getHostMessenger().getHostId(),
                    snapshotTxnId.array(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, cb1, null);
        } catch (KeeperException.NodeExistsException e) {
            SNAP_LOG.warn("Didn't expect the snapshot node to already exist", e);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }

        try {
            cb1.get();
        } catch (KeeperException.NodeExistsException e) {
            SNAP_LOG.warn("Didn't expect the snapshot node to already exist", e);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
    }

}
