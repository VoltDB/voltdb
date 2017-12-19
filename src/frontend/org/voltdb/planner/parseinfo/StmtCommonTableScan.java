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
package org.voltdb.planner.parseinfo;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CommonTableLeafNode;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.StmtEphemeralTableScan;

public class StmtCommonTableScan extends StmtEphemeralTableScan {
    StmtCommonTableScanShared m_sharedScan;

    public StmtCommonTableScan(String tableName, String tableAlias, int stmtId) {
        super(tableName, tableAlias, stmtId);
        m_sharedScan = new StmtCommonTableScanShared();
    }

    @Override
    public boolean getIsReplicated() {
        return m_sharedScan.isReplicated();
    }

    public void setIsReplicated(boolean isReplicated) {
        m_sharedScan.setReplicated(isReplicated);
    }

    @Override
    public List<Index> getIndexes() {
        return noIndexesSupportedOnSubqueryScansOrCommonTables;
    }

    @Override
    public String getColumnName(int columnIndex) {
        return getScanColumns().get(columnIndex).getColumnName();
    }

    public final AbstractParsedStmt getBaseQuery() {
        return m_sharedScan.getBaseQuery();
    }

    public final AbstractParsedStmt getRecursiveQuery() {
        return m_sharedScan.getRecursiveQuery();
    }

    public final void setBaseQuery(AbstractParsedStmt baseQuery) {
        m_sharedScan.setBaseQuery(baseQuery);
    }

    public final void setRecursiveQuery(AbstractParsedStmt recursiveQuery) {
        m_sharedScan.setRecursiveQuery(recursiveQuery);
    }

    @Override
    public JoinNode makeLeafNode(int nodeId, AbstractExpression joinExpr, AbstractExpression whereExpr) {
        return new CommonTableLeafNode(nodeId, joinExpr, whereExpr, this);
    }

    @Override
    public boolean canRunInOneFragment() {
        return true;
    }

    public final void setBestCostBasePlan(CompiledPlan plan, int stmtId) {
        m_sharedScan.setBestCostBasePlan(plan, stmtId, this);
    }

    public final void setBestCostRecursivePlan(CompiledPlan plan, int stmtId) {
        m_sharedScan.setBestCostRecursivePlan(plan, stmtId);
    }

    public final CompiledPlan getBestCostBasePlan() {
        return m_sharedScan.getBestCostBasePlan();
    }

    public final CompiledPlan getBestCostRecursivePlan() {
        return m_sharedScan.getBestCostRecursivePlan();
    }

    @Override
    public boolean isOrderDeterministic(boolean orderIsDeterministic) {
        return false;
    }

    @Override
    public String isContentDeterministic(String isContentDeterministic) {
        // If it's already known to be content non-deterministic
        // than that's all we really need to know.
        if (isContentDeterministic != null) {
            return isContentDeterministic;
        }
        CompiledPlan recursivePlan = getBestCostRecursivePlan();
        CompiledPlan basePlan = getBestCostBasePlan();
        // Look at the base plan and then at the recursive plan,
        // if there is a recursive plan.
        if ( ! basePlan.isContentDeterministic()) {
            return basePlan.nondeterminismDetail();
        }
        if ((recursivePlan != null) && ! recursivePlan.isContentDeterministic()) {
            return recursivePlan.nondeterminismDetail();
        }
        // All deterministic so far, so we've nothing to kvetch about.
        return null;
    }

    @Override
    public boolean hasSignificantOffsetOrLimit(boolean hasSignificantOffsetOrLimit) {
        // These never have limits or offset.
        return false;
    }

    public Integer getBaseStmtId() {
        return m_sharedScan.getBestCostBaseStmtId();
    }

    public Integer getRecursiveStmtId() {
        return m_sharedScan.getBestCostRecursiveStmtId();
    }

    public int overidePlanIds(int nextId) {
        if (  m_sharedScan.needsIdOverride() ) {
            m_sharedScan.setNeedsIdOverride(false);
            if (m_sharedScan.getBestCostBasePlan() != null) {
                nextId = m_sharedScan.getBestCostBasePlan().resetPlanNodeIds(nextId);
            }
            if (m_sharedScan.getBestCostRecursivePlan() != null) {
                nextId = m_sharedScan.getBestCostRecursivePlan().resetPlanNodeIds(nextId);
            }
        }
        return nextId;
    }

    public boolean isRecursiveCTE() {
        return m_sharedScan.getBestCostRecursiveStmtId() != null;
    }

    public void generateOutputSchema(Database db) {
        m_sharedScan.generateOutputSchema(db);
    }

    public void resolveColumnIndexes() {
        m_sharedScan.resolveColumnIndexes();
    }

    public void getTablesAndIndexesFromCommonTableQueries(Map<String, StmtTargetTableScan> tablesRead,
            Collection<String> indexes) {
        m_sharedScan.getTablesAndIndexesFromCommonTableQueries(tablesRead, indexes);
    }
}
