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

import static org.voltdb.sysprocs.SysProcFragmentId.PF_cancelShutdown;
import static org.voltdb.sysprocs.SysProcFragmentId.PF_cancelShutdownAggregate;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;

/**
 * @author rdykiel
 *
 */
public class CancelShutdown extends Resume {
    private final static VoltLogger LOG = new VoltLogger("HOST");

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{PF_cancelShutdown, PF_cancelShutdownAggregate};
    }

    private String ll(long l) {
        return Long.toString(l, Character.MAX_RADIX);
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {

        if (fragmentId == PF_cancelShutdown) {

            // First clear the shutdown condition
            if (context.isLowestSiteId()){
                VoltDB.instance().setShuttingdown(false);
            }

            // Then resume cluster
            super.run(context);
            VoltTable t = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            if (context.isLowestSiteId()){
                t.addRow(m_stat.getMzxid());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("@CancelShutdown returning sigil " + ll(m_stat.getMzxid()));
                }
            }
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_cancelShutdown, t);
        }
        else if (fragmentId == PF_cancelShutdownAggregate) {

            NavigableSet<Long> uniqueTxnIds = new TreeSet<>();
            for (VoltTable t: dependencies.get(SysProcFragmentId.PF_cancelShutdown)) {
                while (t.advanceRow()) {
                    uniqueTxnIds.add(t.getLong(0));
                }
            }

            VoltTable t = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            for (long zktxnid: uniqueTxnIds) {
                t.addRow(zktxnid);
            }
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_cancelShutdownAggregate, t);

        } else {

            VoltDB.crashLocalVoltDB(
                    "Received unrecognized plan fragment id " + fragmentId + " in CancelShutdown",
                    false,
                    null);
        }
        throw new RuntimeException("Should not reach this code");
    }

    @Override
    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
        return createAndExecuteSysProcPlan(PF_cancelShutdown, PF_cancelShutdownAggregate);
    }
}
