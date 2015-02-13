/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.dtxn.DtxnConstants;

/**
 * Forces a flush of committed Export data to the connector queues.
 * An operator can drain all {@link org.voltdb.client.Client} instances
 * generating stored procedure work, call the Quiesce system procedure,
 * and then can poll the Export connector until all data sources return
 * empty buffers.  This process guarantees the poller received all
 * Export data.
  */
@ProcInfo(singlePartition = false)
public class Quiesce extends VoltSystemProcedure {

    static final int DEP_SITES = (int) SysProcFragmentId.PF_quiesce_sites | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_PROCESSED_SITES = (int) SysProcFragmentId.PF_quiesce_processed_sites;
    private static final VoltLogger HOST_LOG = new VoltLogger("HOST");

    @Override
    public void init() {
        registerPlanFragment(SysProcFragmentId.PF_quiesce_sites);
        registerPlanFragment(SysProcFragmentId.PF_quiesce_processed_sites);
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer,List<VoltTable>> dependencies,
        long fragmentId, ParameterSet params, SystemProcedureExecutionContext context)
    {
        try {
            if (fragmentId == SysProcFragmentId.PF_quiesce_sites) {
                // tell each site to quiesce
                context.getSiteProcedureConnection().quiesce();
                VoltTable results = new VoltTable(new ColumnInfo("id", VoltType.BIGINT));
                results.addRow(context.getSiteId());
                return new DependencyPair(DEP_SITES, results);
            }
            else if (fragmentId == SysProcFragmentId.PF_quiesce_processed_sites) {
                VoltTable dummy = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
                dummy.addRow(VoltSystemProcedure.STATUS_OK);
                return new DependencyPair(DEP_PROCESSED_SITES, dummy);
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * There are no user specified parameters.
     * @param ctx Internal parameter not visible the end-user.
     * @return {@link org.voltdb.VoltSystemProcedure#STATUS_SCHEMA}
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
            VoltTable[] result = null;

            SynthesizedPlanFragment pfs1[] = new SynthesizedPlanFragment[2];
            pfs1[0] = new SynthesizedPlanFragment();
            pfs1[0].fragmentId = SysProcFragmentId.PF_quiesce_sites;
            pfs1[0].outputDepId = DEP_SITES;
            pfs1[0].inputDepIds = new int[]{};
            pfs1[0].multipartition = true;
            pfs1[0].parameters = ParameterSet.emptyParameterSet();

            pfs1[1] = new SynthesizedPlanFragment();
            pfs1[1].fragmentId = SysProcFragmentId.PF_quiesce_processed_sites;
            pfs1[1].outputDepId = DEP_PROCESSED_SITES;
            pfs1[1].inputDepIds = new int[] { DEP_SITES };
            pfs1[1].multipartition = false;
            pfs1[1].parameters = ParameterSet.emptyParameterSet();

            try {
                result = executeSysProcPlanFragments(pfs1, DEP_PROCESSED_SITES);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
            return result;
    }

}
