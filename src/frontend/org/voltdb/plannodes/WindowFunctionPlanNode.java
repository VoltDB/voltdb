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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.WindowFunctionExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

/**
 * This plan node represents windowed aggregate computations.
 */
public class WindowFunctionPlanNode extends AbstractPlanNode {
    public enum Members {
        AGGREGATE_COLUMNS,
        AGGREGATE_TYPE,
        AGGREGATE_OUTPUT_COLUMN,
        AGGREGATE_EXPRESSIONS,
        PARTITIONBY_EXPRESSIONS

    };
    // A list of aggregate types.  These are not programming language
    // types.  They are more like expression operator types, like
    // MIN, MAX etc.
    protected List<ExpressionType> m_aggregateTypes = new ArrayList<>();
    // a list of column offsets/indexes not plan column guids.
    protected List<Integer> m_aggregateOutputColumns = new ArrayList<>();
    // List of the input TVEs into the aggregates.  Maybe should become
    // a list of SchemaColumns someday
    protected List<List<AbstractExpression>> m_aggregateExpressions = new ArrayList<>();
    // There is one list of partition by expressions for this WindowFunctionPlanNode.
    // If there is more than one window we will need more than one WindowFunctionPlanNode.
    // At the moment these are guaranteed to be TVES.  This might always be true
    protected List<AbstractExpression> m_partitionByExpressions = new ArrayList<>();
    // This is the list of TVEs for the window functions.  There
    // will only be one of them for now.
    private List<TupleValueExpression> m_outputTVEs = new ArrayList<>();

    // List of the order by expressions.  If there
    // are no order by expressions in one aggregate the list
    // is empty, not null.
    protected List<AbstractExpression> m_orderByExpressions = new ArrayList<>();

    private int getAggregateFunctionCount() {
        return m_aggregateTypes.size();
    }

    @Override
    public void generateOutputSchema(Database db) {
        // We only expect one window function here.
        assert(getAggregateFunctionCount() == 1);
        if (m_outputSchema == null) {
            m_outputSchema = new NodeSchema();
        } else {
            assert(getOutputSchema().size() == 0);
        }
        assert(m_children.size() == 1);
        m_children.get(0).generateOutputSchema(db);
        NodeSchema inputSchema = m_children.get(0).getOutputSchema();
        // We already created the TVE for this expression.
        for (int ii = 0; ii < getAggregateFunctionCount(); ii += 1) {
            TupleValueExpression tve = m_outputTVEs.get(ii);
            getOutputSchema().addColumn(
                    tve.getTableName(), tve.getTableAlias(),
                    tve.getColumnName(), tve.getColumnAlias(),
                    tve);
        }
        // Just copy the input columns to the output schema.
        for (SchemaColumn col : inputSchema) {
            getOutputSchema().addColumn(col.clone());
        }
        m_hasSignificantOutputSchema = true;
    }

    public final void setWindowFunctionExpression(WindowFunctionExpression winExpr) {
        // Currently, we can have only one window function in this kind of a
        // plan node.  So, if we are adding one here, we can't have
        // any already.
        assert(getAggregateFunctionCount() == 0);
        m_aggregateOutputColumns.add(getAggregateFunctionCount());
        m_aggregateTypes.add(winExpr.getExpressionType());
        if (winExpr.getAggregateArguments().size() > 0) {
            m_aggregateExpressions.add(winExpr.getAggregateArguments());
        } else {
            m_aggregateExpressions.add(null);
        }
        m_partitionByExpressions = winExpr.getPartitionByExpressions();
        m_orderByExpressions = winExpr.getOrderByExpressions();
        m_outputTVEs.add(winExpr.getDisplayListExpression());
    }

    /**
     * Serialize to JSON.  We only serialize the expressions, and not the
     * directions.  We won't need them in the executor.  The directions will
     * be in the order by plan node in any case.
     */
    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key("AGGREGATE_COLUMNS")
                .array();
        for (int ii = 0; ii < m_aggregateTypes.size(); ii++) {
            stringer.object();
            stringer.keySymbolValuePair(Members.AGGREGATE_TYPE.name(), m_aggregateTypes.get(ii).name());
            stringer.keySymbolValuePair(Members.AGGREGATE_OUTPUT_COLUMN.name(), m_aggregateOutputColumns.get(ii));
            AbstractExpression.toJSONArray(stringer, Members.AGGREGATE_EXPRESSIONS.name(), m_aggregateExpressions.get(ii));
            stringer.endObject();
        }
        stringer.endArray();
        AbstractExpression.toJSONArray(stringer, Members.PARTITIONBY_EXPRESSIONS.name(), m_partitionByExpressions);
        AbstractExpression.toJSONArrayFromSortList(stringer, m_orderByExpressions, null);
    }

    /**
     * Deserialize a PartitionByPlanNode from JSON.  Since we don't need the
     * sort directions, and we don't serialize them in toJSONString, then we
     * can't in general get them here.
     */
    @Override
    public void loadFromJSONObject(JSONObject jobj, Database db) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        JSONArray jarray = jobj.getJSONArray( Members.AGGREGATE_COLUMNS.name() );
        int size = jarray.length();
        for (int i = 0; i < size; i++) {
            // We only expect one of these for now.
            assert(i == 0);
            JSONObject tempObj = jarray.getJSONObject( i );
            m_aggregateTypes.add( ExpressionType.get( tempObj.getString( Members.AGGREGATE_TYPE.name() )));
            m_aggregateOutputColumns.add( tempObj.getInt( Members.AGGREGATE_OUTPUT_COLUMN.name() ));
            m_aggregateExpressions.add(
                    AbstractExpression.loadFromJSONArrayChild(null, tempObj,
                            Members.AGGREGATE_EXPRESSIONS.name(), null));
        }
        m_partitionByExpressions = AbstractExpression.loadFromJSONArrayChild(null, jobj,
                Members.PARTITIONBY_EXPRESSIONS.name(), null);
        m_orderByExpressions = new ArrayList<>();
        AbstractExpression.loadSortListFromJSONArray(m_orderByExpressions, null, jobj);
    }

    @Override
    public boolean planNodeClassNeedsProjectionNode() {
        return true;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.WINDOWFUNCTION;
    }

    public void resolveColumnIndexesUsingSchema(NodeSchema inputSchema) {
        Collection<TupleValueExpression> allTves;
        // get all the TVEs in the output columns
        for (SchemaColumn col : m_outputSchema) {
            AbstractExpression colExpr = col.getExpression();
            allTves = ExpressionUtil.getTupleValueExpressions(colExpr);
            for (TupleValueExpression tve : allTves) {
                int index = tve.setColumnIndexUsingSchema(inputSchema);
                if (index == -1) {
                    // check to see if this TVE is the aggregate output
                    if ( ! tve.getTableName().equals(AbstractParsedStmt.TEMP_TABLE_NAME)) {
                        throw new RuntimeException("Unable to find index for column: " +
                                tve.getColumnName());
                    }
                }
            }
        }

        // Aggregates also need to resolve indexes for aggregate inputs
        // Find the proper index for the sort columns.  Not quite
        // sure these should be TVEs in the long term.
        for (List<AbstractExpression> agg_exps : m_aggregateExpressions) {
            if (agg_exps != null) {
                for (AbstractExpression agg_exp : agg_exps) {
                    allTves = ExpressionUtil.getTupleValueExpressions(agg_exp);
                    for (TupleValueExpression tve : allTves) {
                        tve.setColumnIndexUsingSchema(inputSchema);
                    }
                }
            }
        }

        // Aggregates also need to resolve indexes for partition by inputs
        for (AbstractExpression group_exp : m_partitionByExpressions) {
            allTves = ExpressionUtil.getTupleValueExpressions(group_exp);
            for (TupleValueExpression tve : allTves) {
                tve.setColumnIndexUsingSchema(inputSchema);
            }
        }

        // Resolve column indices for the order by expressions.  We will
        // only have one of these, but we will act as if we have more than
        // one, which is the general case.
        for (AbstractExpression obExpr : m_orderByExpressions) {
            allTves = ExpressionUtil.getTupleValueExpressions(obExpr);
            for (TupleValueExpression tve : allTves) {
                tve.setColumnIndexUsingSchema(inputSchema);
            }
        }
        /*
         * Is this needed?
         */
        resolveSubqueryColumnIndexes();
    }

    @Override
    public void resolveColumnIndexes() {
        // Aggregates need to resolve indexes for the output schema but don't need
        // to reorder it.  Some of the outputs may be local aggregate columns and
        // won't have a TVE to resolve.
        assert (m_children.size() == 1);
        m_children.get(0).resolveColumnIndexes();
        NodeSchema inputSchema = m_children.get(0).getOutputSchema();

        resolveColumnIndexesUsingSchema(inputSchema);
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return("WINDOW FUNCTION AGGREGATION: ops: " + m_aggregateTypes.get(0).name() + "()");
    }

    public List<AbstractExpression> getPartitionByExpressions() {
        return m_partitionByExpressions;
    }
    public final List<ExpressionType> getAggregateTypes() {
        return m_aggregateTypes;
    }

    public final void setAggregateTypes(List<ExpressionType> aggregateTypes) {
        m_aggregateTypes = aggregateTypes;
    }

    public final List<List<AbstractExpression>> getAggregateExpressions() {
        return m_aggregateExpressions;
    }
}
