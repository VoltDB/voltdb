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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.voltdb.export.ExportManager;
import org.voltdb.fault.FaultHandler;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.fault.VoltFault;
import org.voltdb.logging.VoltLogger;

class VoltDBNodeFailureFaultHandler implements FaultHandler {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    /**
     * When clearing a fault, specifically a rejoining node, wait to make sure it is cleared
     * before proceeding because proceeding might generate new faults that should
     * be deduped by the FaultManager.
     */
    final Semaphore m_waitForFaultClear = new Semaphore(0);

    /**
     * When starting up as a rejoining node a fault is reported
     * for every currently down node. Once this fault is handled
     * here by RealVoltDB's handler the catalog will be updated.
     * Then the rest of the system can init with the updated catalog.
     */
    final Semaphore m_waitForFaultReported = new Semaphore(0);

    private final RealVoltDB m_rvdb;

    VoltDBNodeFailureFaultHandler(RealVoltDB realVoltDB) {
        m_rvdb = realVoltDB;
    }

    @Override
    public void faultOccured(Set<VoltFault> faults)
    {
        for (VoltFault fault : faults) {
            if (fault instanceof NodeFailureFault)
            {
                NodeFailureFault node_fault = (NodeFailureFault) fault;
                handleNodeFailureFault(node_fault);
            }
            VoltDB.instance().getFaultDistributor().reportFaultHandled(this, fault);
        }
    }

    private void handleNodeFailureFault(NodeFailureFault node_fault) {
        ArrayList<Integer> dead_sites =
            VoltDB.instance().getCatalogContext().
            siteTracker.getAllSitesForHost(node_fault.getHostId());
        Collections.sort(dead_sites);
        hostLog.error("Host failed, host id: " + node_fault.getHostId() +
                " hostname: " + node_fault.getHostname());
        hostLog.error("  Removing sites from cluster: " + dead_sites);
        StringBuilder sb = new StringBuilder();
        for (int site_id : dead_sites)
        {
            sb.append("set ");
            String site_path = VoltDB.instance().getCatalogContext().catalog.
                               getClusters().get("cluster").getSites().
                               get(Integer.toString(site_id)).getPath();
            sb.append(site_path).append(" ").append("isUp false");
            sb.append("\n");
        }
        VoltDB.instance().clusterUpdate(sb.toString());
        if (m_rvdb.getCatalogContext().siteTracker.getFailedPartitions().size() != 0)
        {
            hostLog.fatal("Failure of host " + node_fault.getHostId() +
                          " has rendered the cluster unviable.  Shutting down...");
            VoltDB.crashVoltDB();
        }
        m_waitForFaultReported.release();

        /*
         * Use a new thread since this is a asynchronous (and infrequent)
         * task and locks are being held by the fault distributor.
         */
        new Thread() {
            @Override
            public void run() {
                // if we see an early fault (during startup), then it's ok not to
                // notify the export manager
                if (ExportManager.instance() == null)
                    return;
                //Notify the export manager the cluster topology has changed
                ExportManager.instance().notifyOfClusterTopologyChange();
            }
        }.start();
    }

    @Override
    public void faultCleared(Set<VoltFault> faults) {
        for (VoltFault fault : faults) {
            if (fault instanceof NodeFailureFault) {
                m_waitForFaultClear.release();
            }
        }
    }


}
