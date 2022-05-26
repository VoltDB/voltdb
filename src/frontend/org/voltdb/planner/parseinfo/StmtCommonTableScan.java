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
package org.voltdb.planner.parseinfo;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CommonTableLeafNode;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.StmtEphemeralTableScan;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;

/**
 * These are the objects which represent common table scans.  They mostly
 * just have data which is shared between all the scans of the same
 * common table.  The data specific to a particular scan, like the
 * table alias, is stored in the StmtEphemeralTableScan part of this
 * class, which is not shared.
 *
 * The only really interesting parts of this class are some of
 * the methods, like harmonizeOutputSchema.
 */
public class StmtCommonTableScan extends StmtEphemeralTableScan {
    private final StmtCommonTableScanShared m_sharedScan;

    public StmtCommonTableScan(String tableName, String tableAlias, StmtCommonTableScanShared sharedScan) {
        super(tableName, tableAlias, sharedScan.getStatementId());
        m_sharedScan = sharedScan;
        copyTableSchemaFromShared();
    }

    @Override
    public boolean getIsReplicated() {
        return m_sharedScan.isReplicated();
    }

    public boolean calculateReplicatedState(Set<String> visitedTables) {
        return m_sharedScan.calculateReplicatedState(visitedTables);
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

    private boolean isOrderDeterministic(CompiledPlan plan) {
        if (plan != null) {
            return plan.isOrderDeterministic();
        }
        return true;
    }
    @Override
    public boolean isOrderDeterministic(boolean orderIsDeterministic) {
        return orderIsDeterministic
                && isOrderDeterministic(getBestCostBasePlan())
                && isOrderDeterministic(getBestCostRecursivePlan());
    }

    @Override
    public String contentNonDeterminismMessage(String isContentDeterministic) {
        // If it's already known to be content non-deterministic
        // than that's all we really need to know.
        if (isContentDeterministic != null) {
            return isContentDeterministic;
        }
        CompiledPlan recursivePlan = getBestCostRecursivePlan();
        CompiledPlan basePlan = getBestCostBasePlan();
        // Look at the base plan and then at the recursive plan,
        // if there is a recursive plan.  If the base plan is null,
        // there some error in the SQL, but we can't fail here.
        if ( basePlan == null) {
            return null;
        }
        if ( ! basePlan.isContentDeterministic()) {
            return basePlan.nondeterminismDetail();
        }
        if ((recursivePlan != null) && ! recursivePlan.isContentDeterministic()) {
            return recursivePlan.nondeterminismDetail();
        }
        // All deterministic so far, so no kvetching required.
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

    /**
     * Copy the table schema from the shared part to here.
     * We have to repair the table aliases.
     */
    private void copyTableSchemaFromShared() {
        for (SchemaColumn scol : m_sharedScan.getOutputSchema()) {
            SchemaColumn copy = new SchemaColumn(scol.getTableName(),
                                                 getTableAlias(),
                                                 scol.getColumnName(),
                                                 scol.getColumnAlias(),
                                                 scol.getExpression(),
                                                 scol.getDifferentiator());
            addOutputColumn(copy);
        }
    }

    /**
     * We have just planned the base query and perhaps
     * the recursive query.  We need to make sure that the
     * output schema of the scan and the output schemas
     * of the base and recursive plans are all compatible.
     *
     * <ol>
     *   <li>If they have different lengths, then it is an error,
     *       probably an internal error.</li>
     *   <li>If they have different types, then it is ok if there
     *       is a common type to which the two types can be converted.</li>
     *   <li>If they have different lengths, we need to make the length
     *       the larger of the two.</li>
     * </ol>
     * @param scan The scan to harmonize.
     */
    public void harmonizeOutputSchema() {
        boolean changedCurrent;
        boolean changedBase;
        boolean changedRecursive = false;
        NodeSchema currentSchema = getOutputSchema();
        NodeSchema baseSchema = getBestCostBasePlan().rootPlanGraph.getTrueOutputSchema(false);
        NodeSchema recursiveSchema
            = (getBestCostRecursivePlan() == null)
                ? null
                : getBestCostRecursivePlan().rootPlanGraph.getTrueOutputSchema(true);
        // First, make the current schema
        // the widest.
        changedCurrent = currentSchema.harmonize(baseSchema, "Base Query");
        if (recursiveSchema != null) {
            // Widen the current schema to the recursive
            // schema if necessary as well.
            boolean changedRec = currentSchema.harmonize(recursiveSchema, "Recursive Query");
            changedCurrent = changedCurrent || changedRec;
        }
        // Then change the base and current
        // schemas.
        changedBase = baseSchema.harmonize(currentSchema, "Base Query");
        if (recursiveSchema != null) {
            changedRecursive = recursiveSchema.harmonize(currentSchema, "Recursive Query");
        }
        // If we changed something, update the output schemas
        // which depend on the one we changed.
        if (changedBase) {
            getBestCostBasePlan().rootPlanGraph.getTrueOutputSchema(true);
        }
        if (changedRecursive) {
            getBestCostRecursivePlan().rootPlanGraph.getTrueOutputSchema(true);
        }
    }
}
