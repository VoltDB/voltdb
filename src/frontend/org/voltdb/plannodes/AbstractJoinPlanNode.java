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

import java.util.List;
import java.util.TreeMap;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public abstract class AbstractJoinPlanNode extends AbstractPlanNode {

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

    protected AbstractJoinPlanNode() {
        super();
    }

    @Override
    public void validate() throws Exception {
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
    public void setWherePredicate(AbstractExpression predicate)
    {
        if (predicate != null) {
            m_wherePredicate = (AbstractExpression) predicate.clone();
        }
    }

    /**
     * @param predicate the join predicate to set
     */
    public void setPreJoinPredicate(AbstractExpression predicate)
    {
        if (predicate != null) {
            m_preJoinPredicate = (AbstractExpression) predicate.clone();
        }
    }

    /**
     * @param predicate the join predicate to set
     */
    public void setJoinPredicate(AbstractExpression predicate)
    {
        if (predicate != null) {
            m_joinPredicate = (AbstractExpression) predicate.clone();
        }
    }

    @Override
    public void generateOutputSchema(Database db)
    {
        // FUTURE: At some point it would be awesome to further
        // cull the columns out of the join to remove columns that were only
        // used by scans/joins.  I think we can coerce HSQL into provide this
        // info relatively easily. --izzy

        // Index join will have to override this method.
        // Assert and provide functionality for generic join
        assert(m_children.size() == 2);
        for (AbstractPlanNode child : m_children)
        {
            child.generateOutputSchema(db);
        }
        // Join the schema together to form the output schema
        m_outputSchemaPreInlineAgg =
            m_children.get(0).getOutputSchema().
            join(m_children.get(1).getOutputSchema()).copyAndReplaceWithTVE();
        m_hasSignificantOutputSchema = true;

        generateRealOutputSchema();
    }

    protected void generateRealOutputSchema() {
        AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(this);
        if (aggNode != null) {
            m_outputSchema = aggNode.getOutputSchema().copyAndReplaceWithTVE();
        } else {
            m_outputSchema = m_outputSchemaPreInlineAgg;
        }
    }

    // Given any non-inlined type of join, this method will resolve the column
    // order and TVE indexes for the output SchemaColumns.
    @Override
    public void resolveColumnIndexes()
    {
        // First, assert that our topology is sane and then
        // recursively resolve all child/inline column indexes
        IndexScanPlanNode index_scan =
            (IndexScanPlanNode) getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assert(m_children.size() == 2 && index_scan == null);
        for (AbstractPlanNode child : m_children)
        {
            child.resolveColumnIndexes();
        }

        final NodeSchema outer_schema = m_children.get(0).getOutputSchema();
        final NodeSchema inner_schema = m_children.get(1).getOutputSchema();

        // resolve predicates
        resolvePredicate(m_preJoinPredicate, outer_schema, inner_schema);
        resolvePredicate(m_joinPredicate, outer_schema, inner_schema);
        resolvePredicate(m_wherePredicate, outer_schema, inner_schema);

        // need to order the combined input schema coherently.  We make the
        // output schema ordered: [outer table columns][inner table columns]
        TreeMap<Integer, SchemaColumn> sort_cols =
            new TreeMap<Integer, SchemaColumn>();
        for (SchemaColumn col : m_outputSchemaPreInlineAgg.getColumns())
        {
            // Right now these all need to be TVEs
            assert(col.getExpression() instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression)col.getExpression();
            int index = tve.resolveColumnIndexesUsingSchema(outer_schema);
            if (index == -1)
            {
                index = tve.resolveColumnIndexesUsingSchema(inner_schema);
                if (index == -1)
                {
                    throw new RuntimeException("Unable to find index for column: " +
                                               col.toString());
                }
                sort_cols.put(index + outer_schema.size(), col);
            }
            else
            {
                sort_cols.put(index, col);
            }
            tve.setColumnIndex(index);
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

    // TODO: need to extend the sort direction for join from one table to the other table if possible
    // right now, only consider the sort direction on the outer table
    public void resolveSortDirection() {
        AbstractPlanNode outerTable = m_children.get(0);
        if (outerTable.getPlanNodeType() == PlanNodeType.INDEXSCAN) {
            m_sortDirection = ((IndexScanPlanNode)outerTable).getSortDirection();
            return;
        }
        if (outerTable instanceof AbstractJoinPlanNode) {
            ((AbstractJoinPlanNode)outerTable).resolveSortDirection();
            m_sortDirection = ((AbstractJoinPlanNode)outerTable).getSortDirection();
        }
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException
    {
        super.toJSONString(stringer);
        stringer.key(Members.JOIN_TYPE.name()).value(m_joinType.toString());
        stringer.key(Members.PRE_JOIN_PREDICATE.name()).value(m_preJoinPredicate);
        stringer.key(Members.JOIN_PREDICATE.name()).value(m_joinPredicate);
        stringer.key(Members.WHERE_PREDICATE.name()).value(m_wherePredicate);

        if (m_outputSchemaPreInlineAgg != m_outputSchema) {
            stringer.key(Members.OUTPUT_SCHEMA_PRE_AGG.name());
            stringer.array();
            for (SchemaColumn column : m_outputSchemaPreInlineAgg.getColumns()) {
                column.toJSONString(stringer, true);
            }
            stringer.endArray();
        }
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException
    {
        helpLoadFromJSONObject(jobj, db);
        m_joinType = JoinType.get( jobj.getString( Members.JOIN_TYPE.name() ) );
        m_preJoinPredicate = AbstractExpression.fromJSONChild(jobj, Members.PRE_JOIN_PREDICATE.name());
        m_joinPredicate = AbstractExpression.fromJSONChild(jobj, Members.JOIN_PREDICATE.name());
        m_wherePredicate = AbstractExpression.fromJSONChild(jobj, Members.WHERE_PREDICATE.name());

        if ( !jobj.isNull( Members.OUTPUT_SCHEMA_PRE_AGG.name() ) ) {
            m_outputSchemaPreInlineAgg = new NodeSchema();
            m_hasSignificantOutputSchema = true;
            JSONArray jarray = jobj.getJSONArray( Members.OUTPUT_SCHEMA_PRE_AGG.name() );
            int size = jarray.length();
            for( int i = 0; i < size; i++ ) {
                m_outputSchemaPreInlineAgg.addColumn( SchemaColumn.fromJSONObject(jarray.getJSONObject(i)) );
            }
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
            NodeSchema outer_schema, NodeSchema inner_schema)
    {
        // Finally, resolve m_predicate
        List<TupleValueExpression> predicate_tves =
                ExpressionUtil.getTupleValueExpressions(expression);
        for (TupleValueExpression tve : predicate_tves)
        {
            int index = tve.resolveColumnIndexesUsingSchema(outer_schema);
            int tableIdx = 0;   // 0 for outer table
            if (index == -1)
            {
                index = tve.resolveColumnIndexesUsingSchema(inner_schema);
                if (index == -1)
                {
                    throw new RuntimeException("Unable to find index for join TVE: " +
                                               tve.toString());
                }
                tableIdx = 1;   // 1 for inner table
            }
            tve.setColumnIndex(index);
            tve.setTableIndex(tableIdx);
        }
    }

    protected static void resolvePredicate(List<AbstractExpression> expressions,
            NodeSchema outer_schema, NodeSchema inner_schema)
    {
        for (AbstractExpression expr : expressions) {
            resolvePredicate(expr, outer_schema, inner_schema);
        }
    }

    protected String explainFilters(String indent) {
        String result = "";
        String prefix = "\n" + indent + " filter by ";
        AbstractExpression[] predicates = { m_preJoinPredicate, m_joinPredicate, m_wherePredicate };
        for (AbstractExpression pred : predicates) {
            if (pred != null) {
                result += prefix + pred.explain("!?"); // No default table name prefix for columns.
                prefix = " AND ";
            }
        }
        return result;
    }

}
