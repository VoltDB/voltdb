/* This file is part of VoltDB.
 * Copyright (C) 2020 VoltDB Inc.
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

package org.voltdb.operator;

import java.util.List;
import java.util.Map;

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
import org.voltdb.sysprocs.SysProcFragmentId;
import org.voltdb.utils.VoltTableUtil;

/**
 * This is intended only calling from K8s operator, the operator kills cluster via reducing
 * the k8s cluster size to zero, instead of directly shutting down the database.
 *
 * The reason is that k8s will restart VoltDB instance automatically if the instance is
 * stopped via @Shutdown or @StopNode, so a special shutdown procedure is needed to do
 * proper cleanup but doesn't exit the process.
 *
 * The goal of @OpPseudoShutdown is to do the minimum cleanups so that the database can be
 * terminated (un-gracefully) without affecting recoverability after the call.
 */
public class OpPseudoShutdown extends VoltSystemProcedure {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{
            SysProcFragmentId.PF_pseudoShutdownSync,
            SysProcFragmentId.PF_pseudoShutdownSyncDone,
            SysProcFragmentId.PF_pseudoShutdown,
            SysProcFragmentId.PF_pseudoShutdownDone
        };
    }

    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        if (fragmentId == SysProcFragmentId.PF_pseudoShutdownSync) {
            VoltDB.instance().getHostMessenger().prepareForShutdown();
            if (context.isLowestSiteId()) {
                String msg = "VoltDB shutdown operation requested by external controller in progress. Cluster will terminate shortly.";
                CoreUtils.printAsciiArtLog(hostLog, msg, Level.INFO);
            }
            VoltTable result = new VoltTable(new ColumnInfo[] { new ColumnInfo("UNUSED", VoltType.STRING) });
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_pseudoShutdownSync, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_pseudoShutdownSyncDone) {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_pseudoShutdownSync));
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_pseudoShutdownSyncDone, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_pseudoShutdown) {
            if (context.isLowestSiteId()) {
                VoltDB.instance().pseudoShutdown();
            }
            VoltTable result = new VoltTable(new ColumnInfo[] { new ColumnInfo("UNUSED", VoltType.STRING) });
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_pseudoShutdown, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_pseudoShutdownDone) {
            VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            result.addRow(VoltSystemProcedure.STATUS_OK);
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_pseudoShutdownDone, result);
        }
        else {
            VoltDB.crashLocalVoltDB(
                    "Received unrecognized plan fragment id " + fragmentId + " in OpPseudoShutdown",
                    false,
                    null);
        }
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
        createAndExecuteSysProcPlan(SysProcFragmentId.PF_pseudoShutdownSync, SysProcFragmentId.PF_pseudoShutdownSyncDone);
        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_pseudoShutdown, SysProcFragmentId.PF_pseudoShutdownDone);
    }
}
