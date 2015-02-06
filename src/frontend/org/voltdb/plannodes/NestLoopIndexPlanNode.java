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

package org.voltdb.plannodes;

import java.util.TreeMap;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class NestLoopIndexPlanNode extends AbstractJoinPlanNode {

    public NestLoopIndexPlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.NESTLOOPINDEX;
    }

    @Override
    public void generateOutputSchema(Database db)
    {
        // Important safety tip regarding this inlined
        // index scan and ITS inlined projection:
        // That projection is currently only used/usable as
        // a means to narrow the set of columns from the
        // indexscan's target table that make it into the
        // rest of the plan.  the expressions that are
        // given to the projection are currently not ever used
        IndexScanPlanNode inlineScan =
            (IndexScanPlanNode) m_inlineNodes.get(PlanNodeType.INDEXSCAN);
        assert(inlineScan != null);
        inlineScan.generateOutputSchema(db);
        assert(m_children.size() == 1);
        m_children.get(0).generateOutputSchema(db);
        // Join the schema together to form the output schema
        // The child subplan's output is the outer table
        // The inlined node's output is the inner table
        m_outputSchemaPreInlineAgg =
            m_children.get(0).getOutputSchema().
            join(inlineScan.getOutputSchema()).copyAndReplaceWithTVE();
        m_hasSignificantOutputSchema = true;

        generateRealOutputSchema();
    }

    @Override
    public void resolveColumnIndexes()
    {
        IndexScanPlanNode inline_scan =
            (IndexScanPlanNode) m_inlineNodes.get(PlanNodeType.INDEXSCAN);
        assert (m_children.size() == 1 && inline_scan != null);
        for (AbstractPlanNode child : m_children)
        {
            child.resolveColumnIndexes();
        }

        LimitPlanNode limit = (LimitPlanNode)getInlinePlanNode(PlanNodeType.LIMIT);
        if (limit != null)
        {
            // output schema of limit node has not been used
            limit.m_outputSchema = m_outputSchemaPreInlineAgg;
            limit.m_hasSignificantOutputSchema = false;
        }

        // We need the schema from the target table from the inlined index
        final NodeSchema index_schema = inline_scan.getTableSchema();
        // We need the output schema from the child node
        final NodeSchema outer_schema = m_children.get(0).getOutputSchema();

        // pull every expression out of the inlined index scan
        // and resolve all of the TVEs against our two input schema from above.
        resolvePredicate(inline_scan.getPredicate(), outer_schema, index_schema);
        resolvePredicate(inline_scan.getEndExpression(), outer_schema, index_schema);
        resolvePredicate(inline_scan.getInitialExpression(), outer_schema, index_schema);
        resolvePredicate(inline_scan.getSkipNullPredicate(), outer_schema, index_schema);
        resolvePredicate(inline_scan.getSearchKeyExpressions(), outer_schema, index_schema);

        // resolve other predicates
        resolvePredicate(m_preJoinPredicate, outer_schema, index_schema);
        resolvePredicate(m_joinPredicate, outer_schema, index_schema);
        resolvePredicate(m_wherePredicate, outer_schema, index_schema);

        // need to resolve the indexes of the output schema and
        // order the combined output schema coherently
        TreeMap<Integer, SchemaColumn> sort_cols =
            new TreeMap<Integer, SchemaColumn>();
        for (SchemaColumn col : m_outputSchemaPreInlineAgg.getColumns())
        {
            // Right now these all need to be TVEs
            assert(col.getExpression() instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression)col.getExpression();
            int index = outer_schema.getIndexOfTve(tve);
            int tableIdx = 0;   // 0 for outer table
            if (index == -1)
            {
                index = tve.resolveColumnIndexesUsingSchema(index_schema);
                if (index == -1)
                {
                    throw new RuntimeException("Unable to find index for column: " +
                                               col.toString());
                }
                sort_cols.put(index + outer_schema.size(), col);
                tableIdx = 1;   // 1 for inner table
            }
            else
            {
                sort_cols.put(index, col);
            }
            tve.setColumnIndex(index);
            tve.setTableIndex(tableIdx);
        }
        // rebuild the output schema from the tree-sorted columns
        NodeSchema new_output_schema = new NodeSchema();
        for (SchemaColumn col : sort_cols.values())
        {
            new_output_schema.addColumn(col);
        }
        m_outputSchemaPreInlineAgg = new_output_schema;
        m_hasSignificantOutputSchema = true;

        resolveRealOutputSchema();
    }

    @Override
    public void resolveSortDirection() {
        super.resolveSortDirection();
        // special treatment for NLIJ, when the outer table is a materialized scan node
        // the sort direction from the outer table should be the same as the that in the inner table
        // (because we set when building this NLIJ)
        if (m_children.get(0).getPlanNodeType() == PlanNodeType.MATERIALIZEDSCAN) {
            IndexScanPlanNode ispn = (IndexScanPlanNode) m_inlineNodes.get(PlanNodeType.INDEXSCAN);
            assert (((MaterializedScanPlanNode)(m_children.get(0))).getSortDirection() == ispn.getSortDirection());
            m_sortDirection = ispn.getSortDirection();
        }
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        // Check that we have an inline IndexScanPlanNode
        if (m_inlineNodes.isEmpty()) {
            throw new Exception("ERROR: No inline PlanNodes are set for " + this);
        } else if (!m_inlineNodes.containsKey(PlanNodeType.INDEXSCAN)) {
            throw new Exception("ERROR: No inline PlanNode with type '" + PlanNodeType.INDEXSCAN + "' was set for " + this);
        }
    }

    /**
     * Does the (sub)plan guarantee an identical result/effect when "replayed"
     * against the same database state, such as during replication or CL recovery.
     * @return
     */
    @Override
    public boolean isOrderDeterministic() {
        if ( ! super.isOrderDeterministic()) {
            return false;
        }
        IndexScanPlanNode index_scan =
            (IndexScanPlanNode) getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assert(index_scan != null);
        if ( ! index_scan.isOrderDeterministic()) {
            m_nondeterminismDetail = index_scan.m_nondeterminismDetail;
            return false;
        }
        return true;
    }

    @Override
    public boolean hasInlinedIndexScanOfTable(String tableName) {
        IndexScanPlanNode index_scan = (IndexScanPlanNode) getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assert(index_scan != null);
        if (index_scan.getTargetTableName().equals(tableName)) {
            return true;
        } else {
            return getChild(0).hasInlinedIndexScanOfTable(tableName);
        }
    }

    @Override
    public void computeCostEstimates(long childOutputTupleCountEstimate, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {

        // Add the cost of the inlined index scan to the cost of processing the input tuples.
        // This isn't really a fair representation of what's going on, as the index is scanned once
        // per input tuple, but I think it will still cause the plan selector to pick the join
        // order with the lowest total access cost.

        IndexScanPlanNode indexScan =
                (IndexScanPlanNode) getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assert(indexScan != null);

        m_estimatedOutputTupleCount = indexScan.getEstimatedOutputTupleCount() + childOutputTupleCountEstimate;
        m_estimatedProcessedTupleCount = indexScan.getEstimatedProcessedTupleCount() + childOutputTupleCountEstimate;
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "NESTLOOP INDEX " + this.m_joinType.toString() + " JOIN" +
                (m_sortDirection == SortDirectionType.INVALID ? "" : " (" + m_sortDirection + ")") +
                explainFilters(indent);
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException
    {
        super.toJSONString(stringer);
        if (m_sortDirection != SortDirectionType.INVALID) {
            stringer.key(Members.SORT_DIRECTION.name()).value(m_sortDirection.toString());
        }
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException
    {
        super.loadFromJSONObject(jobj, db);
        if (!jobj.isNull(Members.SORT_DIRECTION.name())) {
            m_sortDirection = SortDirectionType.get( jobj.getString( Members.SORT_DIRECTION.name() ) );
        }
    }
}
