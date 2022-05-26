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


/**
 * Dummy MP Write System Procedure for generate fragments to all sites in cluster
 */
public class PingPartitions extends VoltSystemProcedure {

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{
                SysProcFragmentId.PF_pingPartitions,
                SysProcFragmentId.PF_pingPartitionsAggregate,
                SysProcFragmentId.PF_enableScoreboard,
                SysProcFragmentId.PF_enableScoreboardAggregate
        };
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies,
                                              long fragmentId,
                                              ParameterSet params,
                                              SystemProcedureExecutionContext context) {
        VoltTable dummy = new VoltTable(STATUS_SCHEMA);
        dummy.addRow(STATUS_OK);

        if (fragmentId == SysProcFragmentId.PF_pingPartitions) {
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_pingPartitions, dummy);
        } else if (fragmentId == SysProcFragmentId.PF_pingPartitionsAggregate) {
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_pingPartitionsAggregate, dummy);
        } else if (fragmentId == SysProcFragmentId.PF_enableScoreboard) {
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_enableScoreboard, dummy);
        } else if (fragmentId == SysProcFragmentId.PF_enableScoreboardAggregate) {
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_enableScoreboardAggregate, dummy);
        }

        assert (false);
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, byte enableScoreboard) {
        if (enableScoreboard == (byte) 1) {
            return runPingAndEnableScoreboard();
        }
        return runDummyPings();
    }

    private VoltTable[] runDummyPings() {
        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_pingPartitions,
                SysProcFragmentId.PF_pingPartitionsAggregate);
    }

    private VoltTable[] runPingAndEnableScoreboard() {
        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_enableScoreboard,
                SysProcFragmentId.PF_enableScoreboardAggregate);
    }

}
