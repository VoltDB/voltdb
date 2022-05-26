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

package org.voltdb.plannodes;

import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.DatabaseEstimates.TableEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.ScanPlanNodeWhichCanHaveInlineInsert;
import org.voltdb.planner.parseinfo.StmtCommonTableScan;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class SeqScanPlanNode extends AbstractScanPlanNode implements ScanPlanNodeWhichCanHaveInlineInsert {
    private Integer m_CTEBaseStmtId;
    private AbstractPlanNode m_CTEBaseNode = null;

    public SeqScanPlanNode() {
        super();
    }

    public SeqScanPlanNode(StmtTableScan tableScan) {
        setTableScan(tableScan);
        setupForCTEScan();
    }

    public SeqScanPlanNode(String tableName, String tableAlias) {
        super(tableName, tableAlias);
        assert(tableName != null && tableAlias != null);
    }

    public static SeqScanPlanNode createDummyForTest(String tableName,
                                                     List<SchemaColumn> scanColumns) {
        SeqScanPlanNode result = new SeqScanPlanNode(tableName, tableName);
        result.setScanColumns(scanColumns);
        return result;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.SEQSCAN;
    }

    /**
     * Accessor for flag marking the plan as guaranteeing an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     * @return false
     */
    @Override
    public boolean isOrderDeterministic() {
        return false; // TODO: enhance to return true for any supportable cases of in-order storage
    }

    @Override
    public boolean isOutputOrdered (List<AbstractExpression> sortExpressions, List<SortDirectionType> sortDirections) {
        return false;
    }

    private static final TableEstimates SUBQUERY_TABLE_ESTIMATES_HACK = new TableEstimates();
    @Override
    public void computeCostEstimates(long childOutputTupleCountEstimate,
                                     DatabaseEstimates estimates,
                                     ScalarValueHints[] paramHints) {
        if (m_isSubQuery) {
            // Get estimates from the sub-query
            // @TODO For the sub-query the cost estimates will be calculated separately
            // At the moment its contribution to the parent's cost plan is irrelevant because
            // all parent plans have the same best cost plan for the sub-query
            m_estimatedProcessedTupleCount = SUBQUERY_TABLE_ESTIMATES_HACK.minTuples;
            m_estimatedOutputTupleCount = SUBQUERY_TABLE_ESTIMATES_HACK.minTuples;
            return;
        }
        if (m_tableScan instanceof StmtCommonTableScan) {
            // This will do for the moment. %%%
            m_estimatedProcessedTupleCount = SUBQUERY_TABLE_ESTIMATES_HACK.minTuples;
            m_estimatedOutputTupleCount = SUBQUERY_TABLE_ESTIMATES_HACK.minTuples;
            return;
        }
        Table target = ((StmtTargetTableScan)m_tableScan).getTargetTable();
        TableEstimates tableEstimates = estimates.getEstimatesForTable(target.getTypeName());
        // This maxTuples value estimates the number of tuples fetched from the sequential scan.
        // It's a vague measure of the cost of the scan.
        // Its accuracy depends a lot on what kind of post-filtering or projection needs to happen, if any.
        // The tuplesRead value is also used to estimate the number of RESULT rows, regardless of
        // how effective post-filtering might be -- as if all rows passed the filters.
        // This is at least semi-consistent with the ignoring of post-filter effects in IndexScanPlanNode.
        // In effect, though, it gives index scans an "unfair" advantage when they reduce the estimated result size
        // by taking into account the indexed filters -- follow-on plan steps, sorts (etc.), are costed lower
        // as if they are operating on fewer rows than would have come out of the seqscan,
        // though that's nonsense.
        // In any case, it's important to keep an eye on any changes (discounts) to SeqScanPlanNode's costing
        // here to make sure that SeqScanPlanNode never gains an unfair advantage over IndexScanPlanNode.
        m_estimatedProcessedTupleCount = tableEstimates.maxTuples;
        m_estimatedOutputTupleCount = tableEstimates.maxTuples;
    }

    @Override
    public void generateOutputSchema(Database db) {
        super.generateOutputSchema(db);
        StmtCommonTableScan ctScan = getCommonTableScan();

        if (ctScan != null) {
            ctScan.generateOutputSchema(db);
        }
    }

    @Override
    public void resolveColumnIndexes() {
        if (m_isSubQuery) {
            assert(m_children.size() == 1);
            m_children.get(0).resolveColumnIndexes();
        } else {
            StmtCommonTableScan ctScan = getCommonTableScan();
            if (ctScan != null) {
                ctScan.resolveColumnIndexes();
            }
        }
        super.resolveColumnIndexes();
    }

    /**
     * Is this a scan of a common table?
     * @return a boolean value indicating whether this is a common table scan.
     */
    public boolean isCommonTableScan() {
        return m_CTEBaseStmtId != null;
    }

    public Integer getCTEBaseNodeId() {
        return m_CTEBaseStmtId;
    }

    @Override
    protected String explainPlanForNode(String indent) {
        String extraIndent = " ";
        String tableName = m_targetTableName == null? m_targetTableAlias: m_targetTableName;
        if (m_targetTableAlias != null && !m_targetTableAlias.equals(tableName)) {
            tableName += " (" + m_targetTableAlias +")";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("SEQUENTIAL SCAN of ");
        if (isCommonTableScan()) {
            sb.append("COMMON TABLE ");
        }
        sb.append("\"").append(tableName).append("\"")
          .append(explainPredicate("\n" + indent + " filter by "));
        if (isCommonTableScan() && m_CTEBaseNode != null) {
            sb.append(m_CTEBaseNode.explainPlanForNode(indent + extraIndent));
        }
        return sb.toString();
    }

    public StmtCommonTableScan getCommonTableScan() {
        if (m_tableScan instanceof StmtCommonTableScan) {
            return (StmtCommonTableScan)m_tableScan;
        }
        return null;
    }

    @Override
    public boolean hasInlineAggregateNode() {
        return AggregatePlanNode.getInlineAggregationNode(this) != null;
    }

    @Override
    public AbstractPlanNode getAbstractNode() {
        return this;
    }

    public void setCTEBaseNode(AbstractPlanNode cteBaseNode) {
        m_CTEBaseNode = cteBaseNode;
    }

    public AbstractPlanNode getCTEBaseNode() {
        return m_CTEBaseNode;
    }

    enum Members {
        CTE_STMT_ID
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        // This may do nothing if it's not a CTE scan.
        if (isCommonTableScan()) {
            stringer.key(Members.CTE_STMT_ID.name()).value(m_CTEBaseStmtId);
        }
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        if (jobj.has(Members.CTE_STMT_ID.name())) {
            m_CTEBaseStmtId = jobj.getInt(Members.CTE_STMT_ID.name());
        } else {
            m_CTEBaseStmtId = null;
        }
    }

    @Override
    /**
     * To override this node's id we need to make sure that the
     * plans in the override scan are overridden.
     */
    public int overrideId(int nextId) {
        nextId = super.overrideId(nextId);
        if (isCommonTableScan()) {
            nextId = getCommonTableScan().overidePlanIds(nextId);
        }
        return nextId;
    }

    private void setupForCTEScan() {
        StmtCommonTableScan scan = getCommonTableScan();
        if (scan != null) {
            // This is logically unnecessary.  All this
            // data is in the scan node.  But when we recover
            // a plan from JSON we won't have the scan node.
            // So, in order to keep all the metadata from
            // the JSON string in the plan node we need
            // to capture it here.
            m_CTEBaseStmtId = scan.getBaseStmtId();
        }
    }

}

