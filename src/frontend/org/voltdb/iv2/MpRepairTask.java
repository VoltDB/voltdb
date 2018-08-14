/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.rejoin.TaskLog;

import com.google_voltpatches.common.base.Suppliers;

/**
 * This task runs on the MPI's site threads after a topology change which doesn't
 * kill the current master MPI.
 *
 * Must run the repair while pausing the site task queue;
 * Otherwise, a new MP might immediately be blocked in a
 * confused world of semi-repair. So just do the repair
 * work on the site thread....
 *
 * When repairing while the MPI is executing concurrent reads,
 * we need to ensure that the full repair algorithm only runs on one of
 * the in-use RO MPI site threads.  That site will perform all of the message collection
 * and delivery.  All of the other MP RO sites need to block at this point.
 */
public class MpRepairTask extends SiteTasker
{
    static VoltLogger repairLogger = new VoltLogger("REPAIR");

    private InitiatorMailbox m_mailbox;
    private List<Long> m_spMasters;
    private Object m_lock = new Object();
    private boolean m_repairRan = false;
    private final String whoami;
    private final RepairAlgo algo;

    public MpRepairTask(InitiatorMailbox mailbox, List<Long> spMasters, boolean balanceSPI)
    {
        m_mailbox = mailbox;
        m_spMasters = new ArrayList<Long>(spMasters);
        whoami = "MP leader repair " +
                CoreUtils.hsIdToString(m_mailbox.getHSId()) + " ";
        algo = mailbox.constructRepairAlgo(Suppliers.ofInstance(m_spMasters), Integer.MAX_VALUE, whoami, balanceSPI);
    }

    @Override
    public void run(SiteProcedureConnection connection) {
        synchronized (m_lock) {
            if (!m_repairRan) {
                try {
                    boolean success = false;
                    try {
                        algo.start().get();
                        success = true;
                    } catch (CancellationException e) {}
                    if (success) {
                        repairLogger.info(whoami + "finished repair.");
                    }
                    else {
                        repairLogger.info(whoami + "interrupted during repair.  Retrying.");
                    }
                }
                catch (InterruptedException ie) {}
                catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Terminally failed MPI repair.", true, e);
                }
                finally {
                    m_repairRan = true;
                }
            }
        }
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
        throw new RuntimeException("Rejoin while repairing the MPI should be impossible.");
    }
}
