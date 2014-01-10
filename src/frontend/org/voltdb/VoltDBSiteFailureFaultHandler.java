/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.util.Set;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
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
            if (!m_rvdb.getConfig().m_startAction.doesRejoin()) {
                String message = "Node fault detected before all nodes finished " +
                                 "initializing. Cluster will not start.";
                VoltDB.crashGlobalVoltDB(message, false, null);
            }
        }
        if (m_rvdb.rejoining()) {
            VoltDB.crashLocalVoltDB("Detected a node failure during recovery", false, null);
        }
    }
}
