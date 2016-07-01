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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.WindowedExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

/**
 * This plan node represents windowed aggregate computations.
 * The only one we implement now is windowed RANK.  But more
 * could be possible.
 *
 * Note that this is a trivial kind of an AggregatePlanNode.
 */
public class PartitionByPlanNode extends AggregatePlanNode {
    private enum Members {
        ORDER_BY_EXPRS
    };
    public final SchemaColumn getWindowedSchemaColumn() {
        return m_windowedSchemaColumn;
    }

    public final WindowedExpression getWindowedExpression() {
        return (WindowedExpression)m_windowedSchemaColumn.getExpression();
    }

    @Override
    public void generateOutputSchema(Database db)
    {
        if (m_outputSchema == null) {
            m_outputSchema = new NodeSchema();
        } else {
            assert(getOutputSchema().size() == 0);
        }
        assert(m_children.size() == 1);
        m_children.get(0).generateOutputSchema(db);
        NodeSchema inputSchema = m_children.get(0).getOutputSchema();
        // The first column is the aggregate.
        TupleValueExpression tve = new TupleValueExpression(m_windowedSchemaColumn.getTableName(),
                                                            m_windowedSchemaColumn.getTableAlias(),
                                                            m_windowedSchemaColumn.getColumnName(),
                                                            m_windowedSchemaColumn.getColumnAlias());
        tve.setExpressionType(ExpressionType.VALUE_TUPLE);
        tve.setValueType(m_windowedSchemaColumn.getType());
        tve.setValueSize(m_windowedSchemaColumn.getSize());
        // This doesn't really matter, since we will be
        // generating this.  But it can't be negative.
        tve.setColumnIndex(0);
        SchemaColumn aggCol = new SchemaColumn(m_windowedSchemaColumn.getTableName(),
                                               m_windowedSchemaColumn.getTableAlias(),
                                               m_windowedSchemaColumn.getColumnName(),
                                               m_windowedSchemaColumn.getColumnAlias(),
                                               tve);
        getOutputSchema().addColumn(aggCol);
        // Just copy the input columns to the output schema.
        for (SchemaColumn col : inputSchema.getColumns()) {
            getOutputSchema().addColumn(col.clone());
        }
        m_hasSignificantOutputSchema = true;
    }

    public final void setWindowedColumn(SchemaColumn  windowedSchemaColumn) {
        m_windowedSchemaColumn = windowedSchemaColumn;
        assert(windowedSchemaColumn.getExpression() instanceof WindowedExpression);
        WindowedExpression we = (WindowedExpression)windowedSchemaColumn.getExpression();
        m_aggregateOutputColumns.add(0);
        m_aggregateTypes.add(we.getExpressionType());
        m_aggregateDistinct.add(0);
        m_aggregateExpressions.add(null);
        for (AbstractExpression expr : we.getPartitionByExpressions()) {
            m_groupByExpressions.add(expr);
        }
    }

    /**
     * Serialize to JSON.  We only serialize the expressions, and not the
     * directions.  We won't need them in the exector.  The directions will
     * be in the order by plan node in any case.
     */
    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);

        AbstractExpression.toJSONArrayFromSortList(stringer,
                                                   getWindowedExpression().getOrderByExpressions(),
                                                   null);
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        WindowedExpression we = new WindowedExpression();
        AbstractExpression.loadSortListFromJSONArray(we.getOrderByExpressions(), null, jobj);
        we.setExpressionType(m_aggregateTypes.get(0));
        // WE don't really care about the column and table
        // names and aliases.  These are not the ones from the
        // original expression, but we'll never use them to
        // look up things again.
        m_windowedSchemaColumn = new SchemaColumn("WTN", "WTA", "WCN", "WCA", we);
    }

    @Override
    public boolean planNodeClassNeedsProjectionNode() {
        return true;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.PARTITIONBY;
    }

    private SchemaColumn       m_windowedSchemaColumn = null;
}
