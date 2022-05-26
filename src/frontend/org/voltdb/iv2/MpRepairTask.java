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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.iv2.MpTerm.RepairType;
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
    private AtomicBoolean m_repairRan = new AtomicBoolean(false);
    private final String whoami;
    private final RepairAlgo algo;

    // Indicate if this repair is triggered via partition leader migration
    private final boolean m_leaderMigration;

    private final boolean m_txnRestartTrigger;
    public MpRepairTask(InitiatorMailbox mailbox, List<Long> spMasters, RepairType repairType)
    {
        m_mailbox = mailbox;
        m_spMasters = new ArrayList<Long>(spMasters);
        whoami = "MP repair task " +
                CoreUtils.hsIdToString(m_mailbox.getHSId()) + " ";
        m_leaderMigration = repairType.isSkipTxnRestart();
        m_txnRestartTrigger = repairType.isTxnRestart();
        algo = mailbox.constructRepairAlgo(Suppliers.ofInstance(m_spMasters), Integer.MAX_VALUE, whoami, m_leaderMigration);
    }

    @Override
    public void run(SiteProcedureConnection connection) {

        // When MP is processing reads, the task will be queued to all MpRoSite but the task is processed only on one of MpRoSite.
        if (m_repairRan.compareAndSet(false, true)) {
            try {
                try {
                    algo.start().get();
                    repairLogger.info(whoami + "completed.");
                } catch (CancellationException e) {
                    repairLogger.info(whoami + "interrupted. Retrying.");
                }
            } catch (InterruptedException ie) {
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Terminally failed MPI repair.", true, e);
            } finally {
                if (m_txnRestartTrigger) {
                    MpTerm.removeTxnRestartTrigger(m_mailbox.m_messenger.getZK());
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
