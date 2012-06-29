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

package org.voltdb;

import java.util.Set;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.export.ExportManager;
import org.voltdb.fault.FaultHandler;
import org.voltdb.fault.SiteFailureFault;
import org.voltdb.fault.VoltFault;

class VoltDBSiteFailureFaultHandler implements FaultHandler {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    private final RealVoltDB m_rvdb;

    VoltDBSiteFailureFaultHandler(RealVoltDB realVoltDB) {
        m_rvdb = realVoltDB;
    }

    @Override
    public void faultOccured(Set<VoltFault> faults)
    {
        for (VoltFault fault : faults) {
            if (fault instanceof SiteFailureFault)
            {
                SiteFailureFault site_fault = (SiteFailureFault) fault;
                handleSiteFailureFault(site_fault);
            }
        }
    }

    private void handleSiteFailureFault(SiteFailureFault site_fault) {
        hostLog.error("Sites failed, site ids: " + CoreUtils.hsIdCollectionToString(site_fault.getSiteIds()));

        // kill the cluster if this all happened too soon
        if (m_rvdb.getHostMessenger().isLocalHostReady() == false) {
            // check that this isn't a rejoining node
            if (m_rvdb.getConfig().m_rejoinToHostAndPort == null) {
                String message = "Node fault detected before all nodes finished " +
                                 "initializing. Cluster will not start.";
                VoltDB.crashGlobalVoltDB(message, false, null);
            }
        }
        if (m_rvdb.rejoining()) {
            VoltDB.crashLocalVoltDB("Detected a node failure during recovery", false, null);
        }
        /*
         * Use a new thread since this is a asynchronous (and infrequent)
         * task and locks are being held by the fault distributor.
         */
        new Thread() {
            @Override
            public void run() {
                // if we see an early fault (during startup), then it's ok not to
                // notify the export manager
                if (ExportManager.instance() != null) {
                    //Notify the export manager the cluster topology has changed
                    ExportManager.instance().notifyOfClusterTopologyChange();
                }
            }
        }.start();
    }
}
