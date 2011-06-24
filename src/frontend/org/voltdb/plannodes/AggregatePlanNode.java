/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.plannodes;

import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

public class AggregatePlanNode extends AbstractPlanNode {

    public enum Members {
        AGGREGATE_COLUMNS,
        AGGREGATE_TYPE,
        AGGREGATE_DISTINCT,
        AGGREGATE_OUTPUT_COLUMN,
        AGGREGATE_EXPRESSION,
        GROUPBY_EXPRESSIONS;
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

    // True if this aggregate node is the coordinator summary aggregator
    // for an aggregator that was pushed down. Must know to correctly
    // decide if other nodes can be pushed down / past this node.
    public boolean m_isCoordinatingAggregator = false;

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

    public void setOutputSchema(NodeSchema schema)
    {
        // aggregates currently have their output schema specified
        m_outputSchema = schema.clone();
    }

    @Override
    public void generateOutputSchema(Database db)
    {
        assert(m_children.size() == 1);
        m_children.get(0).generateOutputSchema(db);
        // aggregate's output schema is pre-determined, don't touch
        return;
    }

    @Override
    public void resolveColumnIndexes()
    {
        // Aggregates need to resolve indexes for the output schema but don't need
        // to reorder it.  Some of the outputs may be local aggregate columns and
        // won't have a TVE to resolve.
        assert(m_children.size() == 1);
        m_children.get(0).resolveColumnIndexes();
        NodeSchema input_schema = m_children.get(0).getOutputSchema();
        for (SchemaColumn col : m_outputSchema.getColumns())
        {
            // At this point, they'd better all be TVEs.
            assert(col.getExpression() instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression)col.getExpression();
            int index = input_schema.getIndexOfTve(tve);
            if (index == -1)
            {
                // check to see if this TVE is the aggregate output
                // XXX SHOULD MODE THIS STRING TO A STATIC DEF SOMEWHERE
                if (!tve.getTableName().equals("VOLT_TEMP_TABLE"))
                {
                    throw new RuntimeException("Unable to find index for column: " +
                                               col.toString());
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
            int index = input_schema.getIndexOfTve(tve);
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
            int index = input_schema.getIndexOfTve(tve);
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
        assert(aggInputExpr != null);
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
        try
        {
            m_aggregateExpressions.add((AbstractExpression) aggInputExpr.clone());
        }
        catch (CloneNotSupportedException e)
        {
            // This shouldn't ever happen
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    public void addGroupByExpression(AbstractExpression expr)
    {
        if (expr != null)
        {
            try
            {
                m_groupByExpressions.add((AbstractExpression) expr.clone());
            }
            catch (CloneNotSupportedException e)
            {
                // This shouldn't ever happen
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
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
            stringer.key(Members.AGGREGATE_EXPRESSION.name());
            stringer.object();
            m_aggregateExpressions.get(ii).toJSONString(stringer);
            stringer.endObject();
            stringer.endObject();
        }
        stringer.endArray();

        if (!m_groupByExpressions.isEmpty())
        {
            stringer.key(Members.GROUPBY_EXPRESSIONS.name()).array();
            for (int i = 0; i < m_groupByExpressions.size(); i++) {
                stringer.object();
                m_groupByExpressions.get(i).toJSONString(stringer);
                stringer.endObject();
            }
            stringer.endArray();
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        StringBuilder sb = new StringBuilder();
        sb.append("AGGREGATION ops: ");
        for (ExpressionType e : m_aggregateTypes) {
            switch (e) {
            case AGGREGATE_AVG: sb.append("avg, "); break;
            case AGGREGATE_COUNT: sb.append("count, "); break;
            case AGGREGATE_COUNT_STAR: sb.append("count(*), "); break;
            case AGGREGATE_MAX: sb.append("max, "); break;
            case AGGREGATE_MIN: sb.append("min, "); break;
            case AGGREGATE_SUM: sb.append("sum, "); break;
            default: assert(false);
            }
        }
        // trim the last ", " from the string
        sb.setLength(sb.length() - 2);
        return sb.toString();
    }
}
