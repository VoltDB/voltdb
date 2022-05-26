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

import java.util.Collection;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.exceptions.ValidationError;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractSubqueryExpression;
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
    public void generateOutputSchema(Database db) {
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
        // The inlined node's output is the inner table.
        //
        // Note that the inner table's contribution to the join_tuple doesn't include
        // all the columns from the inner table---just the ones needed as determined by
        // the inlined scan's own inlined projection, as described above.
        m_outputSchemaPreInlineAgg =
            m_children.get(0).getOutputSchema().
            join(inlineScan.getOutputSchema()).copyAndReplaceWithTVE();
        m_hasSignificantOutputSchema = true;

        generateRealOutputSchema(db);

        // Generate the output schema for subqueries
        Collection<AbstractExpression> subqueryExpressions = findAllSubquerySubexpressions();
        for (AbstractExpression subqueryExpression : subqueryExpressions) {
            assert(subqueryExpression instanceof AbstractSubqueryExpression);
            ((AbstractSubqueryExpression) subqueryExpression).generateOutputSchema(db);
        }
    }

    @Override
    public void resolveColumnIndexes() {
        IndexScanPlanNode inlineScan =
            (IndexScanPlanNode) m_inlineNodes.get(PlanNodeType.INDEXSCAN);
        assert (m_children.size() == 1 && inlineScan != null);
        for (AbstractPlanNode child : m_children) {
            child.resolveColumnIndexes();
        }

        LimitPlanNode limit = (LimitPlanNode)getInlinePlanNode(PlanNodeType.LIMIT);
        if (limit != null) {
            // output schema of limit node has not been used
            limit.m_outputSchema = m_outputSchemaPreInlineAgg;
            limit.m_hasSignificantOutputSchema = false;
        }

        // We need the schema from the target table from the inlined index
        final NodeSchema completeInnerTableSchema = inlineScan.getTableSchema();
        // We need the output schema from the child node
        final NodeSchema outerSchema = m_children.get(0).getOutputSchema();

        // pull every expression out of the inlined index scan
        // and resolve all of the TVEs against our two input schema from above.
        //
        // Tickets ENG-9389, ENG-9533: we use the complete schema for the inner
        // table (rather than the smaller schema from the inlined index scan's
        // inlined project node) because the inlined scan has no temp table,
        // so predicates will be accessing the index-scanned table directly.
        resolvePredicate(inlineScan.getPredicate(), outerSchema, completeInnerTableSchema);
        resolvePredicate(inlineScan.getEndExpression(), outerSchema, completeInnerTableSchema);
        resolvePredicate(inlineScan.getInitialExpression(), outerSchema, completeInnerTableSchema);
        resolvePredicate(inlineScan.getSkipNullPredicate(), outerSchema, completeInnerTableSchema);
        resolvePredicate(inlineScan.getSearchKeyExpressions(), outerSchema, completeInnerTableSchema);
        resolvePredicate(m_preJoinPredicate, outerSchema, completeInnerTableSchema);
        resolvePredicate(m_joinPredicate, outerSchema, completeInnerTableSchema);
        resolvePredicate(m_wherePredicate, outerSchema, completeInnerTableSchema);

        // Resolve subquery expression indexes
        resolveSubqueryColumnIndexes();

        // Resolve TVE indexes for each schema column.
        for (int i = 0; i < m_outputSchemaPreInlineAgg.size(); ++i) {
            SchemaColumn col = m_outputSchemaPreInlineAgg.getColumn(i);

            // These are all TVEs.
            assert(col.getExpression() instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression)col.getExpression();

            int index;
            int tableIdx;
            if (i < outerSchema.size()) {
                tableIdx = 0; // 0 for outer table
                index = outerSchema.getIndexOfTve(tve);
                if (index >= 0) {
                    tve.setColumnIndex(index);
                }
            } else {
                tableIdx = 1;   // 1 for inner table
                index = tve.setColumnIndexUsingSchema(completeInnerTableSchema);
            }

            if (index == -1) {
                throw new RuntimeException("Unable to find index for column: " +
                                           col.toString());
            }

            tve.setTableIndex(tableIdx);
        }

        // We want the output columns to be ordered like [outer table columns][inner table columns],
        // and further ordered by TVE index within the left- and righthand sides.
        // generateOutputSchema already places outer columns on the left and inner on the right,
        // so we just need to order the left- and righthand sides by TVE index separately.
        m_outputSchemaPreInlineAgg.sortByTveIndex(0, outerSchema.size());
        m_outputSchemaPreInlineAgg.sortByTveIndex(outerSchema.size(), m_outputSchemaPreInlineAgg.size());
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
    public void validate() {
        super.validate();

        // Check that we have an inline IndexScanPlanNode
        if (m_inlineNodes.isEmpty()) {
            throw new ValidationError("No inline PlanNodes are set for %s", toString());
        } else if (!m_inlineNodes.containsKey(PlanNodeType.INDEXSCAN)) {
            throw new ValidationError("No inline PlanNode with type '%s' was set for %s",
                    PlanNodeType.INDEXSCAN, toString());
        }
    }

    /**
     * Does the (sub)plan guarantee an identical result/effect when "replayed"
     * against the same database state, such as during replication or CL recovery.
     * @return
     */
    @Override
    public boolean isOrderDeterministic() {
        if (! super.isOrderDeterministic()) {
            return false;
        }
        IndexScanPlanNode index_scan = getInlineIndexScan();
        if (! index_scan.isOrderDeterministic()) {
            m_nondeterminismDetail = index_scan.m_nondeterminismDetail;
            return false;
        }
        return true;
    }

    @Override
    public boolean hasInlinedIndexScanOfTable(String tableName) {
        IndexScanPlanNode index_scan = getInlineIndexScan();
        if (index_scan.getTargetTableName().equals(tableName)) {
            return true;
        } else {
            return getChild(0).hasInlinedIndexScanOfTable(tableName);
        }
    }

    @Override
    public void computeCostEstimates(long childOutputTupleCountEstimate, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {

        // Add the cost of the inlined index scan to the cost of processing the input tuples.
        // This isn't really a fair representation of what's going on, as the index is scanned once
        // per input tuple, but I think it will still cause the plan selector to pick the join
        // order with the lowest total access cost.

        IndexScanPlanNode indexScan = getInlineIndexScan();

        m_estimatedOutputTupleCount = indexScan.getEstimatedOutputTupleCount() + childOutputTupleCountEstimate;
        // Discount outer child estimates based on the number of its filters
        m_estimatedProcessedTupleCount = indexScan.getEstimatedProcessedTupleCount() +
                discountEstimatedProcessedTupleCount(m_children.get(0));
    }

    public IndexScanPlanNode getInlineIndexScan() {
        IndexScanPlanNode indexScan =
                (IndexScanPlanNode) getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assert(indexScan != null);
        return indexScan;
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "NESTLOOP INDEX " + this.m_joinType.toString() + " JOIN" +
                (m_sortDirection == SortDirectionType.INVALID ? "" : " (" + m_sortDirection + ")") +
                explainFilters(indent);
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        if (m_sortDirection != SortDirectionType.INVALID) {
            stringer.keySymbolValuePair(Members.SORT_DIRECTION.name(),
                    m_sortDirection.toString());
        }
    }

    @Override
    public void loadFromJSONObject(JSONObject jobj, Database db) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        if ( ! jobj.isNull(Members.SORT_DIRECTION.name())) {
            m_sortDirection = SortDirectionType.get(
                    jobj.getString(Members.SORT_DIRECTION.name()));
        }
    }

}
