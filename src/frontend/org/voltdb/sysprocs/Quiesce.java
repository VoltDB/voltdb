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

package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

/**
 * Forces a flush of committed Export data to the connector queues.
 * An operator can drain all {@link org.voltdb.client.Client} instances
 * generating stored procedure work, call the Quiesce system procedure,
 * and then can poll the Export connector until all data sources return
 * empty buffers.  This process guarantees the poller received all
 * Export data.
  */
public class Quiesce extends VoltSystemProcedure {

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{SysProcFragmentId.PF_quiesce_sites, SysProcFragmentId.PF_quiesce_processed_sites};
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
                return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_quiesce_sites, results);
            }
            else if (fragmentId == SysProcFragmentId.PF_quiesce_processed_sites) {
                VoltTable dummy = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
                dummy.addRow(VoltSystemProcedure.STATUS_OK);
                return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_quiesce_processed_sites, dummy);
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

        try {
            result = createAndExecuteSysProcPlan(SysProcFragmentId.PF_quiesce_sites,
                    SysProcFragmentId.PF_quiesce_processed_sites);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

}
