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
package org.voltdb.planner;

import org.voltdb.planner.parseinfo.StmtTableScan;

/**
 * An ephemeral table is one which is not persistent.  These are
 * derived tables, which we call subqueries, and common tables, which
 * are defined by WITH clauses.  This just aggregates some common
 * behavior.
 */
public abstract class StmtEphemeralTableScan extends StmtTableScan {
    public StmtEphemeralTableScan(String tableAlias, int stmtId) {
        super(tableAlias, stmtId);
    }

    /**
     * When this scan is planned, this is where the best plan will be cached.
     */
    private CompiledPlan m_bestCostPlan = null;

    private StatementPartitioning m_scanPartitioning = null;

    public final CompiledPlan getBestCostPlan() {
        return m_bestCostPlan;
    }

    public final void setBestCostPlan(CompiledPlan costPlan) {
        m_bestCostPlan = costPlan;
    }

    public void setScanPartitioning(StatementPartitioning scanPartitioning) {
        m_scanPartitioning = scanPartitioning;
    }

    public final StatementPartitioning getScanPartitioning() {
        return m_scanPartitioning;
    }

    public abstract boolean canRunInOneFragment();

    public abstract boolean isOrderDeterministic(boolean orderIsDeterministic);

    public abstract String isContentDeterministic(String isContentDeterministic);

    public abstract boolean hasSignificantOffsetOrLimit(boolean hasSignificantOffsetOrLimit);
}
