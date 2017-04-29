/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import static org.voltdb.sysprocs.SysProcFragmentId.PF_prepareShutdown;
import static org.voltdb.sysprocs.SysProcFragmentId.PF_prepareShutdownAggregate;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.voltcore.agreement.DtxnConstants;
import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;

/**
 *The system stored procedure will pause the cluster and set a flag indicating that
 *the cluster is preparing for shutdown. All reads and writes except the system stored procedures which are allowed as
 *specified in SystemProcedureCatalog will be blocked.
 *
 */
@ProcInfo(singlePartition = false)
public class PrepareShutdown extends Pause {

    private final static int DEP_prepareShutdown = (int)
            PF_prepareShutdown | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private final static int DEP_prepareShutdonwAggregate = (int)
            PF_prepareShutdownAggregate;

    private final static VoltLogger LOG = new VoltLogger("HOST");

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{PF_prepareShutdown, PF_prepareShutdownAggregate};
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {

        if (fragmentId == PF_prepareShutdown) {

            super.run(context);

            VoltTable t = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            if (context.isLowestSiteId()){
                VoltDB.instance().setShuttingdown(true);
                t.addRow(m_stat.getMzxid());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("@PrepareShutdown returning sigil " + ll(m_stat.getMzxid()));
                }
            }
            return new DependencyPair.TableDependencyPair(DEP_prepareShutdown, t);

        } else if (fragmentId == PF_prepareShutdownAggregate) {

            NavigableSet<Long> uniqueTxnIds = new TreeSet<>();
            for (VoltTable t: dependencies.get(DEP_prepareShutdown)) {
                while (t.advanceRow()) {
                    uniqueTxnIds.add(t.getLong(0));
                }
            }

            VoltTable t = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            for (long zktxnid: uniqueTxnIds) {
                t.addRow(zktxnid);
            }

            return new DependencyPair.TableDependencyPair(DEP_prepareShutdonwAggregate, t);

        } else {

            VoltDB.crashLocalVoltDB(
                    "Received unrecognized plan fragment id " + fragmentId + " in PrepareShutdown",
                    false,
                    null);
        }
        throw new RuntimeException("Should not reach this code");
    }

    private SynthesizedPlanFragment[] createPrepareFragments() {
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];

        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = PF_prepareShutdown;
        pfs[0].outputDepId = DEP_prepareShutdown;
        pfs[0].inputDepIds = new int[]{};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.emptyParameterSet();

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = PF_prepareShutdownAggregate;
        pfs[1].outputDepId = DEP_prepareShutdonwAggregate;
        pfs[1].inputDepIds = new int[] {DEP_prepareShutdown};
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        return pfs;

    }

    @Override
    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
        return executeSysProcPlanFragments(createPrepareFragments(), DEP_prepareShutdonwAggregate);
    }
}
