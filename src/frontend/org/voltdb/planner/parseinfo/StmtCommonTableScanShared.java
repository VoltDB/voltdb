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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.voltdb.catalog.Database;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.plannodes.CommonTablePlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;

/**
 * StmtCommonTableScan scans are replicated.  This
 * is the part which is shared between them.
 *
 * @author bwhite
 */
public class StmtCommonTableScanShared {
    private final String m_tableName;
    private final int m_statementId;
    private Boolean m_isReplicated = null;
    private AbstractParsedStmt m_baseQuery;
    private AbstractParsedStmt m_recursiveQuery;
    // This is the equivalent of m_table in StmtTargetTableScan.
    // We don't actually have a catalog Table object for this
    // table.  All we have is this very scan node, which is
    // referenced in the m_tableAliasMap of all the
    // ParsedSelectStmt objects.  That's ok, because we
    // only need what's in this schema anyway.
    private CompiledPlan m_bestCostBasePlan = null;
    private Integer m_bestCostBaseStmtId = null;
    private CompiledPlan m_bestCostRecursivePlan = null;
    private Integer m_bestCostRecursiveStmtId = null;
    private final NodeSchema m_outputSchema = new NodeSchema();

    public StmtCommonTableScanShared(String tableName, int stmtId) {
        m_tableName = tableName;
        m_statementId = stmtId;
    }


    public final boolean isReplicated() {
        // Memoize m_isReplicated.
        if ( m_isReplicated == null) {
            // If this is the first time we query this
            // scan, then we need to calculate whether
            // all the tables here are replicated.
            m_isReplicated = calculateReplicatedState(new HashSet<String>());
        }
        return m_isReplicated;
    }

    protected boolean calculateReplicatedState(Set<String> visitedTables) {
        // If we are working on this scan then
        // we will calculate an answer later on.
        boolean alreadyThere = ! visitedTables.add(m_tableName);
        if (alreadyThere) {
            return true;
        }
        // Look at the base and recursive queries.  If m_recursiveQuery == null this
        // will do the right thing.
        return calculateReplicatedStateForStmt(m_baseQuery, visitedTables)
                    && calculateReplicatedStateForStmt(m_recursiveQuery, visitedTables);
    }


    private boolean calculateReplicatedStateForStmt(AbstractParsedStmt stmt, Set<String> visitedTables) {
        if (stmt == null) {
            return true;
        }
        for ( StmtTableScan scan : stmt.allScans()) {
            boolean isReplicated;
            if (scan instanceof StmtCommonTableScan) {
                isReplicated = ((StmtCommonTableScan)scan).calculateReplicatedState(visitedTables);
            }
            else {
                isReplicated = scan.getIsReplicated();
            }
            if ( ! isReplicated ) {
                return isReplicated;
            }
        }
        return true;
    }
    public final AbstractParsedStmt getBaseQuery() {
        return m_baseQuery;
    }

    public final void setBaseQuery(AbstractParsedStmt baseQuery) {
        m_baseQuery = baseQuery;
    }

    public final AbstractParsedStmt getRecursiveQuery() {
        return m_recursiveQuery;
    }

    public final void setRecursiveQuery(AbstractParsedStmt recursiveQuery) {
        m_recursiveQuery = recursiveQuery;
    }

    public final CompiledPlan getBestCostBasePlan() {
        return m_bestCostBasePlan;
    }

    public final void setBestCostBasePlan(CompiledPlan plan, int stmtId, StmtCommonTableScan alias) {
        // We want to add a CommonTable plan note at the top
        // of the root plan graph.  The subPlanGraph must be
        // empty as well.
        assert(plan.subPlanGraph == null);
        CommonTablePlanNode ctplan = new CommonTablePlanNode(alias, alias.getTableName());
        // We will add the recursive table id later.
        ctplan.addAndLinkChild(plan.rootPlanGraph);
        plan.rootPlanGraph = ctplan;
        m_bestCostBasePlan = plan;
        m_bestCostBaseStmtId = stmtId;
    }

    public final Integer getBestCostBaseStmtId() {
        return m_bestCostBaseStmtId;
    }

    public final void setBestCostBaseStmtId(Integer bestCostBaseStmtId) {
        m_bestCostBaseStmtId = bestCostBaseStmtId;
    }

    public final CompiledPlan getBestCostRecursivePlan() {
        return m_bestCostRecursivePlan;
    }

    public final void setBestCostRecursivePlan(CompiledPlan bestCostRecursivePlan, int stmtId) {
        m_bestCostRecursivePlan = bestCostRecursivePlan;
        m_bestCostRecursiveStmtId = stmtId;
    }

    public final Integer getBestCostRecursiveStmtId() {
        return m_bestCostRecursiveStmtId;
    }
    // We can only override the ids in this scan once.
    private boolean m_needsOutputSchemaGenerated = true;
    private boolean m_needsColumnIndexesResolved = true;
    private boolean m_needsIdOverride = true;
    private boolean m_needsTablesAndIndexes = true;

    public final boolean needsOutputSchemaGenerated() {
        return m_needsOutputSchemaGenerated;
    }

    public final void setNeedsOutputSchemaGenerated(boolean needsOutputSchemaGenerated) {
        m_needsOutputSchemaGenerated = needsOutputSchemaGenerated;
    }

    public final boolean needsColumnIndexesResolved() {
        return m_needsColumnIndexesResolved;
    }

    public final void setNeedsColumnIndexesResolved(boolean needsColumnIndexesResolved) {
        m_needsColumnIndexesResolved = needsColumnIndexesResolved;
    }

    public final boolean needsIdOverride() {
        return m_needsIdOverride;
    }

    public final void setNeedsIdOverride(boolean needsIdOverride) {
        m_needsIdOverride = needsIdOverride;
    }

    private void generateOutputSchema(CompiledPlan plan, Database db) {
        if (plan != null) {
            plan.rootPlanGraph.generateOutputSchema(db);
            if (plan.subPlanGraph != null) {
                plan.subPlanGraph.generateOutputSchema(db);
            }
        }
    }
    public void generateOutputSchema(Database db) {
        if (m_needsOutputSchemaGenerated) {
            m_needsOutputSchemaGenerated = false;
            generateOutputSchema(m_bestCostBasePlan, db);
            generateOutputSchema(m_bestCostRecursivePlan, db);
        }
    }
    private void resolveColumnIndexes(CompiledPlan plan) {
        if (plan != null) {
            plan.rootPlanGraph.resolveColumnIndexes();
            if (plan.subPlanGraph != null) {
                plan.subPlanGraph.resolveColumnIndexes();
            }
        }
    }

    public void resolveColumnIndexes() {
        if (m_needsColumnIndexesResolved) {
            m_needsColumnIndexesResolved = false;
            resolveColumnIndexes(m_bestCostBasePlan);
            resolveColumnIndexes(m_bestCostRecursivePlan);
        }
    }

    public void getTablesAndIndexesFromCommonTableQueries(Map<String, StmtTargetTableScan> tablesRead,
            Collection<String> indexes) {
        if (m_needsTablesAndIndexes) {
            m_needsTablesAndIndexes = false;
            assert(getBestCostBasePlan() != null);
            getBestCostBasePlan().rootPlanGraph.getTablesAndIndexes(tablesRead, indexes);
            if (getBestCostRecursivePlan() != null) {
                getBestCostRecursivePlan().rootPlanGraph.getTablesAndIndexes(tablesRead, indexes);
            }
        }
    }

    public final String getTableName() {
        return m_tableName;
    }


    public final int getStatementId() {
        return m_statementId;
    }

    public void addOutputColumn(SchemaColumn col) {
        m_outputSchema.addColumn(col);
    }

    public NodeSchema getOutputSchema() {
        return m_outputSchema;
    }
}
