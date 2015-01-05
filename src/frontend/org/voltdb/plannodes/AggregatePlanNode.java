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

import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

public class AggregatePlanNode extends AbstractPlanNode {

    public enum Members {
        PRE_PREDICATE,   // ENG-1565: to accelerate min() / max() using index purpose only
        POST_PREDICATE,
        AGGREGATE_COLUMNS,
        AGGREGATE_TYPE,
        AGGREGATE_DISTINCT,
        AGGREGATE_OUTPUT_COLUMN,
        AGGREGATE_EXPRESSION,
        GROUPBY_EXPRESSIONS,
        PARTIAL_GROUPBY_COLUMNS
        ;
    }

    protected List<ExpressionType> m_aggregateTypes = new ArrayList<ExpressionType>();
    // a list of whether the aggregate is over distinct elements
    // 0 is not distinct, 1 is distinct
    protected List<Integer> m_aggregateDistinct = new ArrayList<Integer>();
    // a list of column offsets/indexes not plan column guids.
    protected List<Integer> m_aggregateOutputColumns = new ArrayList<Integer>();
    // List of the input TVEs into the aggregates.  Maybe should become
    // a list of SchemaColumns someday
    protected List<AbstractExpression> m_aggregateExpressions =
        new ArrayList<AbstractExpression>();

    // At the moment these are guaranteed to be TVES.  This might always be true
    protected List<AbstractExpression> m_groupByExpressions
        = new ArrayList<AbstractExpression>();

    // This list is only used for the special case of instances of PartialAggregatePlanNode.
    protected List<Integer> m_partialGroupByColumns = null;

    // True if this aggregate node is the coordinator summary aggregator
    // for an aggregator that was pushed down. Must know to correctly
    // decide if other nodes can be pushed down / past this node.
    public boolean m_isCoordinatingAggregator = false;

    protected AbstractExpression m_prePredicate;
    protected AbstractExpression m_postPredicate;

    public AggregatePlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.AGGREGATE;
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        //
        // We need to have an aggregate type and column
        // We're not checking that it's a valid ExpressionType because this plannode is a temporary hack
        //
        if (m_aggregateTypes.size() != m_aggregateDistinct.size() ||
            m_aggregateDistinct.size() != m_aggregateExpressions.size() ||
            m_aggregateExpressions.size() != m_aggregateOutputColumns.size())
        {
            throw new Exception("ERROR: Mismatched number of aggregate expression column attributes for PlanNode '" + this + "'");
        } else if (m_aggregateTypes.isEmpty()|| m_aggregateTypes.contains(ExpressionType.INVALID)) {
            throw new Exception("ERROR: Invalid Aggregate ExpressionType or No Aggregate Expression types for PlanNode '" + this + "'");
        } else if (m_aggregateExpressions.isEmpty()) {
            throw new Exception("ERROR: No Aggregate Expressions for PlanNode '" + this + "'");
        }
    }

    public boolean isTableCountStar() {
        if (m_groupByExpressions.isEmpty() == false)
            return false;
        if (m_aggregateTypes.size() != 1)
            return false;
        if (m_aggregateTypes.get(0).equals(ExpressionType.AGGREGATE_COUNT_STAR) == false)
            return false;

        return true;
    }

    // single min() without GROUP BY?
    public boolean isTableMin() {
        // do not support GROUP BY for now
        if (m_groupByExpressions.isEmpty() == false)
            return false;
        if (m_aggregateTypes.size() != 1)
            return false;
        if (m_aggregateTypes.get(0).equals(ExpressionType.AGGREGATE_MIN) == false)
            return false;

        return true;
    }

    // single max() without GROUP BY?
    public boolean isTableMax() {
        // do not support GROUP BY for now
        if (m_groupByExpressions.isEmpty() == false)
            return false;
        if (m_aggregateTypes.size() != 1)
            return false;
        if (m_aggregateTypes.get(0).equals(ExpressionType.AGGREGATE_MAX) == false)
            return false;

        return true;
    }

    // set predicate for SELECT MAX(X) FROM T WHERE X > / >= ? case
    public void setPrePredicate(AbstractExpression predicate) {
        m_prePredicate = predicate;
    }

    public void setPostPredicate(AbstractExpression predicate) {
        m_postPredicate = predicate;
    }

    public AbstractExpression getPostPredicate() {
        return m_postPredicate;
    }

    // for single min() / max(), return the single aggregate expression
    public AbstractExpression getFirstAggregateExpression() {
        return m_aggregateExpressions.get(0);
    }

    public int getAggregateTypesSize () {
        return m_aggregateTypes.size();
    }

    public List<AbstractExpression> getGroupByExpressions() {
        return m_groupByExpressions;
    }

    public int getGroupByExpressionsSize () {
        return m_groupByExpressions.size();
    }

    public void setOutputSchema(NodeSchema schema)
    {
        // aggregates currently have their output schema specified
        m_outputSchema = schema.clone();
        m_hasSignificantOutputSchema = true;
    }

    @Override
    public void generateOutputSchema(Database db)
    {
        // aggregate's output schema is pre-determined
        if (m_children.size() == 1) {
            m_children.get(0).generateOutputSchema(db);

            assert(m_hasSignificantOutputSchema);
        }
    }

    @Override
    public void resolveColumnIndexes()
    {
        // Aggregates need to resolve indexes for the output schema but don't need
        // to reorder it.  Some of the outputs may be local aggregate columns and
        // won't have a TVE to resolve.
        assert (m_children.size() == 1);
        m_children.get(0).resolveColumnIndexes();
        NodeSchema input_schema = m_children.get(0).getOutputSchema();

        resolveColumnIndexesUsingSchema(input_schema);
    }

    void resolveColumnIndexesUsingSchema(NodeSchema input_schema)
    {
        // get all the TVEs in the output columns
        List<TupleValueExpression> output_tves = new ArrayList<TupleValueExpression>();
        for (SchemaColumn col : m_outputSchema.getColumns()) {
            output_tves.addAll(ExpressionUtil.getTupleValueExpressions(col.getExpression()));
        }
        for (TupleValueExpression tve : output_tves) {
            int index = tve.resolveColumnIndexesUsingSchema(input_schema);
            if (index == -1) {
                // check to see if this TVE is the aggregate output
                // XXX SHOULD MODE THIS STRING TO A STATIC DEF SOMEWHERE
                if (!tve.getTableName().equals("VOLT_TEMP_TABLE")) {
                    throw new RuntimeException("Unable to find index for column: " +
                                               tve.getColumnName());
                }
            }
            else
            {
                tve.setColumnIndex(index);
            }
        }

        // Aggregates also need to resolve indexes for aggregate inputs
        // Find the proper index for the sort columns.  Not quite
        // sure these should be TVEs in the long term.
        List<TupleValueExpression> agg_tves =
            new ArrayList<TupleValueExpression>();
        for (AbstractExpression agg_exp : m_aggregateExpressions)
        {
            agg_tves.addAll(ExpressionUtil.getTupleValueExpressions(agg_exp));
        }
        for (TupleValueExpression tve : agg_tves)
        {
            int index = tve.resolveColumnIndexesUsingSchema(input_schema);
            tve.setColumnIndex(index);
        }

        // Aggregates also need to resolve indexes for group_by inputs
        List<TupleValueExpression> group_tves =
            new ArrayList<TupleValueExpression>();
        for (AbstractExpression group_exp : m_groupByExpressions)
        {
            group_tves.addAll(ExpressionUtil.getTupleValueExpressions(group_exp));
        }
        for (TupleValueExpression tve : group_tves)
        {
            int index = tve.resolveColumnIndexesUsingSchema(input_schema);
            tve.setColumnIndex(index);
        }

        // Post filter also needs to resolve indexes.
        List<TupleValueExpression> postFilter_tves =
                ExpressionUtil.getTupleValueExpressions(m_postPredicate);
        for (TupleValueExpression tve : postFilter_tves)
        {
            int index = m_outputSchema.getIndexOfTve(tve);
            tve.setColumnIndex(index);
        }

    }

    /**
     * Add an aggregate to this plan node.
     * @param aggType
     * @param isDistinct  Is distinct being applied to the argument of this aggregate?
     * @param aggOutputColumn  Which output column in the output schema this
     *        aggregate should occupy
     * @param aggInputExpr  The input expression which should get aggregated
     */
    public void addAggregate(ExpressionType aggType,
                             boolean isDistinct,
                             Integer aggOutputColumn,
                             AbstractExpression aggInputExpr)
    {
        m_aggregateTypes.add(aggType);
        if (isDistinct)
        {
            m_aggregateDistinct.add(1);
        }
        else
        {
            m_aggregateDistinct.add(0);
        }
        m_aggregateOutputColumns.add(aggOutputColumn);
        if (aggType == ExpressionType.AGGREGATE_COUNT_STAR) {
            assert(aggInputExpr == null);
            m_aggregateExpressions.add(null);
        } else {
            assert(aggInputExpr != null);
            m_aggregateExpressions.add((AbstractExpression) aggInputExpr.clone());
        }
    }

    public void addGroupByExpression(AbstractExpression expr)
    {
        if (expr == null) {
            return;
        }
        m_groupByExpressions.add((AbstractExpression) expr.clone());
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);

        stringer.key("AGGREGATE_COLUMNS");
        stringer.array();
        for (int ii = 0; ii < m_aggregateTypes.size(); ii++) {
            stringer.object();
            stringer.key(Members.AGGREGATE_TYPE.name()).value(m_aggregateTypes.get(ii).name());
            stringer.key(Members.AGGREGATE_DISTINCT.name()).value(m_aggregateDistinct.get(ii));
            stringer.key(Members.AGGREGATE_OUTPUT_COLUMN.name()).value(m_aggregateOutputColumns.get(ii));
            AbstractExpression ae = m_aggregateExpressions.get(ii);
            if (ae != null) {
                stringer.key(Members.AGGREGATE_EXPRESSION.name());
                stringer.object();
                ae.toJSONString(stringer);
                stringer.endObject();
            }
            stringer.endObject();
        }
        stringer.endArray();

        if (! m_groupByExpressions.isEmpty()) {
            stringer.key(Members.GROUPBY_EXPRESSIONS.name()).array();
            for (int i = 0; i < m_groupByExpressions.size(); i++) {
                stringer.object();
                m_groupByExpressions.get(i).toJSONString(stringer);
                stringer.endObject();
            }
            stringer.endArray();

            if (m_partialGroupByColumns != null) {
                assert(! m_partialGroupByColumns.isEmpty());
                stringer.key(Members.PARTIAL_GROUPBY_COLUMNS.name()).array();
                for (Integer ith: m_partialGroupByColumns) {
                    stringer.value(ith.longValue());
                }
                stringer.endArray();
            }
        }

        if (m_prePredicate != null) {
            stringer.key(Members.PRE_PREDICATE.name()).value(m_prePredicate);
        }
        if (m_postPredicate != null) {
            stringer.key(Members.POST_PREDICATE.name()).value(m_postPredicate);
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        StringBuilder sb = new StringBuilder();
        String optionalTableName = "*NO MATCH -- USE ALL TABLE NAMES*";
        String aggType = "Hash";
        if (getPlanNodeType() == PlanNodeType.AGGREGATE) {
            aggType = "Serial";
        } else if (getPlanNodeType() == PlanNodeType.PARTIALAGGREGATE) {
            aggType = "Partial";
        } else {
            assert(getPlanNodeType() == PlanNodeType.HASHAGGREGATE);
        }

        sb.append(aggType + " AGGREGATION ops: ");
        int ii = 0;
        for (ExpressionType e : m_aggregateTypes) {
            sb.append(e.symbol());
            if (e != ExpressionType.AGGREGATE_COUNT_STAR) {
                if (m_aggregateDistinct.get(ii) == 1) {
                    sb.append(" DISTINCT");
                }
                sb.append("(");
                AbstractExpression ae = m_aggregateExpressions.get(ii);
                if (ae != null) {
                    sb.append(ae.explain(optionalTableName));
                }
                sb.append("), ");
            }
            ++ii;
        }
        // trim the last ", " from the string
        sb.setLength(sb.length() - 2);
        if (m_prePredicate != null) {
            sb.append(" ONLY IF " + m_prePredicate.explain(optionalTableName));
        }
        if (m_postPredicate != null) {
            // HAVING is always defined WRT to the current outputSchema (NOT inputschema).
            // This might be a little surprising to the user
            // -- maybe we can find some better way to describe the TVEs, here.
            sb.append(" HAVING " + m_postPredicate.explain("VOLT_TEMP_TABLE"));
        }

        return sb.toString();
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        JSONArray jarray = jobj.getJSONArray( Members.AGGREGATE_COLUMNS.name() );
        int size = jarray.length();
        for (int i = 0; i < size; i++) {
            JSONObject tempObj = jarray.getJSONObject( i );
            m_aggregateTypes.add( ExpressionType.get( tempObj.getString( Members.AGGREGATE_TYPE.name() )));
            m_aggregateDistinct.add( tempObj.getInt( Members.AGGREGATE_DISTINCT.name() ) );
            m_aggregateOutputColumns.add( tempObj.getInt( Members.AGGREGATE_OUTPUT_COLUMN.name() ));

            if (jobj.isNull(Members.AGGREGATE_EXPRESSION.name())) {
                m_aggregateExpressions.add(null);
            }
            else {
                m_aggregateExpressions.add(
                    AbstractExpression.fromJSONChild(tempObj, Members.AGGREGATE_EXPRESSION.name()));
            }
        }
        AbstractExpression.loadFromJSONArrayChild(m_groupByExpressions, jobj,
                                                  Members.GROUPBY_EXPRESSIONS.name(), null);

        if ( ! jobj.isNull(Members.PARTIAL_GROUPBY_COLUMNS.name())) {
            JSONArray jarray2 = jobj.getJSONArray(Members.PARTIAL_GROUPBY_COLUMNS.name());
            int numCols = jarray2.length();
            m_partialGroupByColumns = new ArrayList<>(numCols);
            for (int ii = 0; ii < numCols; ++ii) {
                m_partialGroupByColumns.add(jarray2.getInt(ii));
            }
        }

        m_prePredicate = AbstractExpression.fromJSONChild(jobj, Members.PRE_PREDICATE.name());
        m_postPredicate = AbstractExpression.fromJSONChild(jobj, Members.POST_PREDICATE.name());
    }

    public static AggregatePlanNode getInlineAggregationNode(AbstractPlanNode node) {
        AggregatePlanNode aggNode =
                (AggregatePlanNode) (node.getInlinePlanNode(PlanNodeType.AGGREGATE));
        if (aggNode == null) {
            aggNode = (HashAggregatePlanNode) (node.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        }
        if (aggNode == null) {
            aggNode = (PartialAggregatePlanNode) (node.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));
        }

        return aggNode;
    }
}
