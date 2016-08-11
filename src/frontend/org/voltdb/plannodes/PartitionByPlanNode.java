/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.WindowedExpression;
import org.voltdb.types.PlanNodeType;

/**
 * This plan node represents windowed aggregate computations.
 * The only one we implement now is windowed RANK.  But more
 * could be possible.
 *
 * Note that this is a trivial kind of an AggregatePlanNode.
 */
public class PartitionByPlanNode extends AggregatePlanNode {
    @Override
    public void generateOutputSchema(Database db) {
        assert(m_children.size() == 1);
        m_children.get(0).generateOutputSchema(db);
        NodeSchema inputSchema = m_children.get(0).getOutputSchema();
        List<SchemaColumn> inputColumns = inputSchema.getColumns();
        m_outputSchema = new NodeSchema(1 + inputColumns.size());
        // We already created the TVE for this expression.
        TupleValueExpression tve = m_windowedExpression.getDisplayListExpression();
        SchemaColumn aggCol = new SchemaColumn(tve.getTableName(),
                                               tve.getTableAlias(),
                                               tve.getColumnName(),
                                               tve.getColumnAlias(),
                                               tve);
        m_outputSchema.addColumn(aggCol);
        // Just copy the input columns to the output schema.
        for (SchemaColumn col : inputSchema.getColumns()) {
            m_outputSchema.addColumn(col.clone());
        }
        m_hasSignificantOutputSchema = true;
    }

    public final WindowedExpression getWindowedExpression() {
        return m_windowedExpression;
    }

    public final void setWindowedExpression(WindowedExpression winExpr) {
        m_windowedExpression = winExpr;
        m_aggregateOutputColumns.add(0);
        m_aggregateTypes.add(winExpr.getExpressionType());
        m_aggregateDistinct.add(0);
        // This could be the first order by expression.  We currently
        // just support RANGE units, so there is only one order by expression.
        // Furthermore, the RANK() operation does not have an argument, unlike,
        // say, the MEAN(EXP) operation.  So, we pass the only order by
        // expression in as the aggregate expression.  However, it seems
        // like propagating this hack out of the EE seems wrong.  So, we
        // don't add an expression here, and we will add order by expressions
        // to the plan node, as if we were doing it right.  We will fix it
        // up, which is to say, we will break it, in the EE.
        m_aggregateExpressions.add(null);
        for (AbstractExpression expr : winExpr.getPartitionByExpressions()) {
            m_groupByExpressions.add(expr);
        }
    }

    /**
     * Serialize to JSON.  We only serialize the expressions, and not the
     * directions.  We won't need them in the executor.  The directions will
     * be in the order by plan node in any case.
     */
    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);

        AbstractExpression.toJSONArrayFromSortList(stringer,
                                                   getWindowedExpression().getOrderByExpressions(),
                                                   null);
    }

    /**
     * Deserialize a PartitionByPlanNode from JSON.  Since we don't need the
     * sort directions, and we don't serialize them in toJSONString, then we
     * can't in general get them here.
     */
    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        WindowedExpression winExpr = new WindowedExpression();
        AbstractExpression.loadSortListFromJSONArray(winExpr.getOrderByExpressions(),
                                                     null,
                                                     jobj);
        winExpr.setExpressionType(m_aggregateTypes.get(0));
        // WE don't really care about the column and table
        // names and aliases.  These are not the ones from the
        // original expression, but we'll never use them to
        // look up things again.
        m_windowedExpression = winExpr;
    }

    @Override
    public boolean planNodeClassNeedsProjectionNode() {
        return true;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.PARTITIONBY;
    }

    @Override
    protected List<TupleValueExpression> getExpressionsNeedingResolution() {
        List<TupleValueExpression> answer = super.getExpressionsNeedingResolution();
        WindowedExpression we = getWindowedExpression();
        // The partition by expressions are in the group by list.  So
        // they have been managed by the AggregatePlanNode.  We do need
        // to resolve column indices for the order by expressions.  We will
        // only have one of these, but we will act as if we have more than
        // one, which is the general case.
        for (AbstractExpression ae : we.getOrderByExpressions()) {
            if (ae instanceof TupleValueExpression) {
                answer.add((TupleValueExpression)ae);
            }
        }
        return answer;
    }

    private WindowedExpression     m_windowedExpression = null;
}
