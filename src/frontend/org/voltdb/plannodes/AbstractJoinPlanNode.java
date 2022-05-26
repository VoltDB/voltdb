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
import java.util.List;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractSubqueryExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public abstract class AbstractJoinPlanNode extends AbstractPlanNode implements IndexSortablePlanNode {

    public enum Members {
        SORT_DIRECTION,
        JOIN_TYPE,
        PRE_JOIN_PREDICATE,
        JOIN_PREDICATE,
        WHERE_PREDICATE,
        OUTPUT_SCHEMA_PRE_AGG;
    }

    protected JoinType m_joinType = JoinType.INNER;
    // sortDirection is only used in handleOrderBy(),
    // and the sortDirection used in EE is from inlined IndexScan node for NLIJ
    protected SortDirectionType m_sortDirection = SortDirectionType.INVALID;
    protected AbstractExpression m_preJoinPredicate = null;
    protected AbstractExpression m_joinPredicate = null;
    protected AbstractExpression m_wherePredicate = null;

    protected NodeSchema m_outputSchemaPreInlineAgg = null;
    private final IndexUseForOrderBy m_indexUse = new IndexUseForOrderBy();

    protected AbstractJoinPlanNode() {
        super();
    }

    @Override
    public void validate() {
        super.validate();

        if (m_preJoinPredicate != null) {
            m_preJoinPredicate.validate();
        }
        if (m_joinPredicate != null) {
            m_joinPredicate.validate();
        }
        if (m_wherePredicate != null) {
            m_wherePredicate.validate();
        }
    }

    /**
     * @return the join_type
     */
    public JoinType getJoinType() {
        return m_joinType;
    }

    /**
     * @param join_type the join_type to set
     */
    public void setJoinType(JoinType join_type) {
        m_joinType = join_type;
    }

    /**
     * @return the  pre join predicate
     */
    public AbstractExpression getPreJoinPredicate() {
        return m_preJoinPredicate;
    }

    /**
     * @return the  join predicate
     */
    public AbstractExpression getJoinPredicate() {
        return m_joinPredicate;
    }

    /**
     * @return the  where predicate
     */
    public AbstractExpression getWherePredicate() {
        return m_wherePredicate;
    }

    /**
     * @param predicate the where predicate to set
     */
    public void setWherePredicate(AbstractExpression predicate) {
        if (predicate != null) {
            m_wherePredicate = predicate.clone();
        } else {
            m_wherePredicate = null;
        }
    }

    /**
     * @param predicate the join predicate to set
     */
    public void setPreJoinPredicate(AbstractExpression predicate) {
        if (predicate != null) {
            m_preJoinPredicate = predicate.clone();
        } else {
            m_preJoinPredicate = null;
        }
    }

    /**
     * @param predicate the join predicate to set
     */
    public void setJoinPredicate(AbstractExpression predicate) {
        if (predicate != null) {
            m_joinPredicate = predicate.clone();
        } else {
            m_joinPredicate = null;
        }
    }

    @Override
    public void generateOutputSchema(Database db) {
        // FUTURE: At some point it would be awesome to further
        // cull the columns out of the join to remove columns that were only
        // used by scans/joins.  I think we can coerce HSQL into provide this
        // info relatively easily. --izzy

        // Index join will have to override this method.
        // Assert and provide functionality for generic join
        assert(m_children.size() == 2);
        for (AbstractPlanNode child : m_children) {
            child.generateOutputSchema(db);
        }

        // Generate the output schema for subqueries
        Collection<AbstractExpression> subqueryExpressions = findAllSubquerySubexpressions();
        for (AbstractExpression expr : subqueryExpressions) {
            ((AbstractSubqueryExpression) expr).generateOutputSchema(db);
        }

        // Join the schema together to form the output schema
        m_outputSchemaPreInlineAgg = m_children.get(0).getOutputSchema().
                join(m_children.get(1).getOutputSchema()).copyAndReplaceWithTVE();
        m_hasSignificantOutputSchema = true;
        generateRealOutputSchema(db);
    }

    public void setOutputSchemaPreInlineAgg(NodeSchema schema) {
        m_outputSchemaPreInlineAgg = schema;
    }

    NodeSchema getOutputSchemaPreInlineAgg() {
        return m_outputSchemaPreInlineAgg;
    }

    protected void generateRealOutputSchema(Database db) {
        final AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(this);
        if (aggNode != null) {
            // generate its subquery output schema
            aggNode.generateOutputSchema(db);
            m_outputSchema = aggNode.getOutputSchema().copyAndReplaceWithTVE();
        } else {
            m_outputSchema = m_outputSchemaPreInlineAgg;
        }
    }

    // Given any non-inlined type of join, this method will resolve the column
    // order and TVE indexes for the output SchemaColumns.
    @Override
    public void resolveColumnIndexes() {
        // First, assert that our topology is sane and then
        // recursively resolve all child/inline column indexes
        IndexScanPlanNode index_scan = (IndexScanPlanNode) getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assert(m_children.size() == 2 && index_scan == null);
        for (AbstractPlanNode child : m_children) {
            child.resolveColumnIndexes();
        }

        final NodeSchema outer_schema = m_children.get(0).getOutputSchema();
        final NodeSchema inner_schema = m_children.get(1).getOutputSchema();
        final int outerSize = outer_schema.size();

        // resolve predicates
        resolvePredicate(m_preJoinPredicate, outer_schema, inner_schema);
        resolvePredicate(m_joinPredicate, outer_schema, inner_schema);
        resolvePredicate(m_wherePredicate, outer_schema, inner_schema);

        // Resolve subquery expression indexes
        resolveSubqueryColumnIndexes();

        // Resolve TVE indexes for each schema column.
        for (int i = 0; i < m_outputSchemaPreInlineAgg.size(); ++i) {
            SchemaColumn col = m_outputSchemaPreInlineAgg.getColumn(i);

            // These will all be TVEs.
            assert(col.getExpression() instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression)col.getExpression();
            int index;
            if (i < outerSize) {
                index = tve.setColumnIndexUsingSchema(outer_schema);
            } else {
                index = tve.setColumnIndexUsingSchema(inner_schema);
                index += outerSize;
            }

            if (index == -1) {
                throw new RuntimeException("Unable to find index for column: " + col.toString());
            }

            tve.setColumnIndex(index);
            tve.setDifferentiator(index);
        }

        // We want the output columns to be ordered like [outer table columns][inner table columns],
        // and further ordered by TVE index within the left- and righthand sides.
        // generateOutputSchema already places outer columns on the left and inner on the right,
        // so we just need to order the left- and righthand sides by TVE index separately.
        m_outputSchemaPreInlineAgg.sortByTveIndex(0, outer_schema.size());
        m_outputSchemaPreInlineAgg.sortByTveIndex(outer_schema.size(), m_outputSchemaPreInlineAgg.size());
        m_hasSignificantOutputSchema = true;

        resolveRealOutputSchema();
    }

    protected void resolveRealOutputSchema() {
        AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(this);
        if (aggNode != null) {
            aggNode.resolveColumnIndexesUsingSchema(m_outputSchemaPreInlineAgg);
            m_outputSchema = aggNode.getOutputSchema().clone();
        } else {
            m_outputSchema = m_outputSchemaPreInlineAgg;
        }
    }

    public SortDirectionType getSortDirection() {
        return m_sortDirection;
    }

    @Override
    public boolean isOutputOrdered (List<AbstractExpression> sortExpressions, List<SortDirectionType> sortDirections) {
        AbstractPlanNode outerTable = m_children.get(0);
        AbstractPlanNode aggrNode = AggregatePlanNode.getInlineAggregationNode(this);
        if (aggrNode != null && aggrNode.getPlanNodeType() == PlanNodeType.HASHAGGREGATE) {
            return false;
        } else if (outerTable.getPlanNodeType() == PlanNodeType.INDEXSCAN || outerTable instanceof AbstractJoinPlanNode) {
            // Not yet handling ORDER BY expressions based on more than just the left-most table
            return outerTable.isOutputOrdered(sortExpressions, sortDirections);
        } else {
            return false;
        }
    }

    // TODO: need to extend the sort direction for join from one table to the other table if possible
    // right now, only consider the sort direction on the outer table
    public void resolveSortDirection() {
        AbstractPlanNode outerTable = m_children.get(0);
        if (m_joinType == JoinType.FULL) {
            // Disable the usual optimizations for ordering join output by
            // outer table only. In case of FULL join, the unmatched inner table tuples
            // get appended to the end of the join's output table thus invalidating
            // the outer table join order.
            m_sortDirection = SortDirectionType.INVALID;
            return;
        }
        if (outerTable instanceof IndexSortablePlanNode) {
            m_sortDirection = ((IndexSortablePlanNode)outerTable).indexUse().getSortOrderFromIndexScan();
        }
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.keySymbolValuePair(Members.JOIN_TYPE.name(), m_joinType.toString());
        stringer.key(Members.PRE_JOIN_PREDICATE.name()).value(m_preJoinPredicate);
        stringer.key(Members.JOIN_PREDICATE.name()).value(m_joinPredicate);
        stringer.key(Members.WHERE_PREDICATE.name()).value(m_wherePredicate);

        if (m_outputSchemaPreInlineAgg != m_outputSchema) {
            stringer.key(Members.OUTPUT_SCHEMA_PRE_AGG.name());
            stringer.array();
            for (int colNo = 0; colNo < m_outputSchemaPreInlineAgg.size(); colNo += 1) {
                SchemaColumn column = m_outputSchemaPreInlineAgg.getColumn(colNo);
                column.toJSONString(stringer, true, colNo);
            }
            stringer.endArray();
        }
    }

    @Override
    public void loadFromJSONObject(JSONObject jobj, Database db) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        m_joinType = JoinType.get( jobj.getString( Members.JOIN_TYPE.name() ) );
        m_preJoinPredicate = AbstractExpression.fromJSONChild(jobj, Members.PRE_JOIN_PREDICATE.name());
        m_joinPredicate = AbstractExpression.fromJSONChild(jobj, Members.JOIN_PREDICATE.name());
        m_wherePredicate = AbstractExpression.fromJSONChild(jobj, Members.WHERE_PREDICATE.name());

        if (! jobj.isNull( Members.OUTPUT_SCHEMA_PRE_AGG.name())) {
            m_hasSignificantOutputSchema = true;
            m_outputSchemaPreInlineAgg = loadSchemaFromJSONObject(jobj, Members.OUTPUT_SCHEMA_PRE_AGG.name());
        } else {
            m_outputSchemaPreInlineAgg = m_outputSchema;
        }
    }


    /**
     *
     * @param expression
     * @param outer_schema
     * @param inner_schema
     */
    protected static void resolvePredicate(AbstractExpression expression,
            NodeSchema outer_schema, NodeSchema inner_schema) {
        for (TupleValueExpression tve : ExpressionUtil.getTupleValueExpressions(expression)) {
            int index = tve.setColumnIndexUsingSchema(outer_schema);
            int tableIdx = 0;   // 0 for outer table
            if (index == -1) {
                index = tve.setColumnIndexUsingSchema(inner_schema);
                if (index == -1) {
                    throw new RuntimeException("Unable to resolve column index for join TVE: " + tve.toString());
                }
                tableIdx = 1;   // 1 for inner table
            }
            tve.setTableIndex(tableIdx);
        }
    }

    protected static void resolvePredicate(List<AbstractExpression> expressions,
            NodeSchema outer_schema, NodeSchema inner_schema) {
        for (AbstractExpression expr : expressions) {
            resolvePredicate(expr, outer_schema, inner_schema);
        }
    }

    protected String explainFilters(String indent) {
        StringBuilder result = new StringBuilder();
        String prefix = "\n" + indent + " filter by ";
        AbstractExpression[] predicates = { m_preJoinPredicate, m_joinPredicate, m_wherePredicate };
        for (AbstractExpression pred : predicates) {
            if (pred != null) {
                result.append(prefix).append(pred.explain("!?")); // No default table name prefix for columns.
                prefix = " AND ";
            }
        }
        return result.toString();
    }

    @Override
    public void findAllExpressionsOfClass(Class< ? extends AbstractExpression> aeClass, Set<AbstractExpression> collected) {
        super.findAllExpressionsOfClass(aeClass, collected);
        if (m_preJoinPredicate != null) {
            collected.addAll(m_preJoinPredicate.findAllSubexpressionsOfClass(aeClass));
        }
        if (m_joinPredicate != null) {
            collected.addAll(m_joinPredicate.findAllSubexpressionsOfClass(aeClass));
        }
        if (m_wherePredicate != null) {
            collected.addAll(m_wherePredicate.findAllSubexpressionsOfClass(aeClass));
        }
    }

    /**
     * Discount join node child estimates based on the number of its filters
     *
     * @param childNode
     * @return discounted estimates
     */
    protected long discountEstimatedProcessedTupleCount(AbstractPlanNode childNode) {
        // Discount estimated processed tuple count for the outer child based on the number of
        // filter expressions this child has with a rapidly diminishing effect
        // that ranges from a discount of 0.09 (ORETATION_EQAUL)
        // or 0.045 (all other expression types) for one post filter to a max discount approaching
        // 0.888... (=8/9) for many EQUALITY filters.
        // The discount value is less than the partial index discount (0.1) to make sure
        // the index wins
        final AbstractExpression predicate;
        if (childNode instanceof AbstractScanPlanNode) {
            predicate = ((AbstractScanPlanNode) childNode).getPredicate();
        } else if (childNode instanceof NestLoopPlanNode) {
            predicate = ((NestLoopPlanNode) childNode).getWherePredicate();
        } else if (childNode instanceof NestLoopIndexPlanNode) {
            AbstractPlanNode inlineIndexScan = childNode.getInlinePlanNode(PlanNodeType.INDEXSCAN);
            assert(inlineIndexScan != null);
            predicate = ((AbstractScanPlanNode) inlineIndexScan).getPredicate();
        } else {
            return childNode.getEstimatedProcessedTupleCount();
        }

        if (predicate == null) {
            return childNode.getEstimatedProcessedTupleCount();
        }

        List<AbstractExpression> predicateExprs = ExpressionUtil.uncombinePredicate(predicate);
        // Counters to count the number of equality and all other expressions
        int eqCount = 0;
        int otherCount = 0;
        final double MAX_EQ_POST_FILTER_DISCOUNT = 0.09;
        final double MAX_OTHER_POST_FILTER_DISCOUNT = 0.045;
        double discountCountFactor = 1.0;
        // Discount tuple count.
        for (AbstractExpression predicateExpr: predicateExprs) {
            if (ExpressionType.COMPARE_EQUAL == predicateExpr.getExpressionType()) {
                discountCountFactor -= Math.pow(MAX_EQ_POST_FILTER_DISCOUNT, ++eqCount);
            } else {
                discountCountFactor -= Math.pow(MAX_OTHER_POST_FILTER_DISCOUNT, ++otherCount);
            }
        }
        return  (long) (childNode.getEstimatedProcessedTupleCount() * discountCountFactor);
    }

    /**
     * When a project node is added to the top of the plan, we need to adjust
     * the differentiator field of TVEs to reflect differences in the scan
     * schema vs the storage schema of a table, so that fields with duplicate
     * names produced by expanding "SELECT *" can resolve correctly.
     *
     * We recurse until we find either a join node or a scan node.
     *
     * Resolution of columns produced by "SELECT *" is not a problem for
     * joins because there is always a sequential scan at the top of plans
     * that have this problem, so just use the tve's coluymn index as its
     * differentiator here.
     *
     * @param  tve
     */
    @Override
    public void adjustDifferentiatorField(TupleValueExpression tve) {
        tve.setDifferentiator(tve.getColumnIndex());
    }

    @Override
    public IndexUseForOrderBy indexUse() {
        return m_indexUse;
    }

    @Override
    public AbstractPlanNode planNode() {
        return this;
    }

}
