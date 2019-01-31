/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;

import java.util.List;
import java.util.Map;


/**
 * Dummy MP Write System Procedure for generate fragments to all sites in cluster
 *
 * @throws VoltAbortException
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
            return new DependencyPair.TableDependencyPair((int) SysProcFragmentId.PF_pingPartitions, dummy);
        } else if (fragmentId == SysProcFragmentId.PF_pingPartitionsAggregate) {
            return new DependencyPair.TableDependencyPair((int) SysProcFragmentId.PF_pingPartitionsAggregate, dummy);
        } else if (fragmentId == SysProcFragmentId.PF_enableScoreboard) {
            return new DependencyPair.TableDependencyPair((int) SysProcFragmentId.PF_enableScoreboard, dummy);
        } else if (fragmentId == SysProcFragmentId.PF_enableScoreboardAggregate) {
            return new DependencyPair.TableDependencyPair((int) SysProcFragmentId.PF_enableScoreboardAggregate, dummy);
        }

        assert (false);
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, byte enableScoreboard) throws VoltAbortException {
        if (enableScoreboard == (byte) 1) {
            return runPingAndEnableScoreboard();
        }
        return runDummyPings();
    }

    private VoltTable[] runDummyPings() {
        SynthesizedPlanFragment spf[] = new SynthesizedPlanFragment[2];
        spf[0] = new SynthesizedPlanFragment();
        spf[0].fragmentId = SysProcFragmentId.PF_pingPartitions;
        spf[0].outputDepId = (int) SysProcFragmentId.PF_pingPartitions;
        spf[0].multipartition = true;
        spf[0].parameters = ParameterSet.emptyParameterSet();

        spf[1] = new SynthesizedPlanFragment();
        spf[1].fragmentId = SysProcFragmentId.PF_pingPartitionsAggregate;
        spf[1].outputDepId = (int) SysProcFragmentId.PF_pingPartitionsAggregate;
        spf[1].multipartition = false;
        spf[1].parameters = ParameterSet.emptyParameterSet();
        return executeSysProcPlanFragments(spf, (int) SysProcFragmentId.PF_pingPartitionsAggregate);
    }

    private VoltTable[] runPingAndEnableScoreboard() {
        SynthesizedPlanFragment spf[] = new SynthesizedPlanFragment[2];
        spf[0] = new SynthesizedPlanFragment();
        spf[0].fragmentId = SysProcFragmentId.PF_enableScoreboard;
        spf[0].outputDepId = (int) SysProcFragmentId.PF_enableScoreboard;
        spf[0].multipartition = true;
        spf[0].parameters = ParameterSet.emptyParameterSet();

        spf[1] = new SynthesizedPlanFragment();
        spf[1].fragmentId = SysProcFragmentId.PF_enableScoreboardAggregate;
        spf[1].outputDepId = (int) SysProcFragmentId.PF_enableScoreboardAggregate;
        spf[1].multipartition = false;
        spf[1].parameters = ParameterSet.emptyParameterSet();
        return executeSysProcPlanFragments(spf, (int) SysProcFragmentId.PF_enableScoreboardAggregate);
    }

}
