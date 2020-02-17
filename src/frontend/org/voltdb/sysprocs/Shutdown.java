/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

/**
 * A wholly improper shutdown. No promise is given to return a result to a client,
 * to finish work queued behind this procedure or to return meaningful errors for
 * those queued transactions.
 *
 * Invoking this procedure immediately attempts to terminate each node in the cluster.
 */
public class Shutdown extends VoltSystemProcedure {

    private static AtomicBoolean m_failsafeArmed = new AtomicBoolean(false);
    private static Thread m_failsafe = new Thread() {
        @Override
        public void run() {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {}
            VoltLogger voltLogger = new VoltLogger("HOST");
            String msg = "VoltDB shutting down as requested by @Shutdown command.";
            CoreUtils.printAsciiArtLog(voltLogger, msg, Level.INFO);
            System.exit(0);
        }
    };

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{
            SysProcFragmentId.PF_shutdownSync,
            SysProcFragmentId.PF_shutdownSyncDone,
            SysProcFragmentId.PF_shutdownCommand,
            SysProcFragmentId.PF_procedureDone
        };
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies,
                                           long fragmentId,
                                           ParameterSet params,
                                           SystemProcedureExecutionContext context)
    {
        if (fragmentId == SysProcFragmentId.PF_shutdownSync) {
            VoltDB.instance().getHostMessenger().prepareForShutdown();
            if (!m_failsafeArmed.getAndSet(true)) {
                m_failsafe.start();
                VoltLogger voltLogger = new VoltLogger("HOST");
                String msg = "VoltDB shutdown operation requested and in progress. Cluster will terminate shortly.";
                CoreUtils.printAsciiArtLog(voltLogger, msg, Level.INFO);
            }
            VoltTable rslt = new VoltTable(new ColumnInfo[] { new ColumnInfo("HA", VoltType.STRING) });
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_shutdownSync, rslt);
        }
        else if (fragmentId == SysProcFragmentId.PF_shutdownSyncDone) {
            VoltTable rslt = new VoltTable(new ColumnInfo[] { new ColumnInfo("HA", VoltType.STRING) });
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_shutdownSyncDone, rslt);
        }
        else if (fragmentId == SysProcFragmentId.PF_shutdownCommand) {
            Thread shutdownThread = new Thread() {
                @Override
                public void run() {
                    boolean die = false;
                    try {
                        die = VoltDB.instance().shutdown(this);
                    } catch (InterruptedException e) {
                        new VoltLogger("HOST").error(
                                "Exception while attempting to shutdown VoltDB from shutdown sysproc",
                                e);
                    }
                    if (die) {
                        VoltLogger voltLogger = new VoltLogger("HOST");
                        String msg = "VoltDB shutting down as requested by @Shutdown command.";
                        CoreUtils.printAsciiArtLog(voltLogger, msg, Level.INFO);
                        System.exit(0);
                    }
                    else {
                        try {
                            Thread.sleep(10000);
                        }
                        catch (InterruptedException e) {}
                    }
                }
            };
            shutdownThread.start();
        }
        return null;
    }

    /**
     * Begin an un-graceful shutdown.
     * @param ctx Internal parameter not exposed to the end-user.
     * @return Never returned, no he never returned...
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
        createAndExecuteSysProcPlan(SysProcFragmentId.PF_shutdownSync, SysProcFragmentId.PF_shutdownSyncDone);

        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[] {
                new SynthesizedPlanFragment(SysProcFragmentId.PF_shutdownCommand, true) };

        executeSysProcPlanFragments(pfs, SysProcFragmentId.PF_procedureDone);
        return new VoltTable[0];
    }
}
