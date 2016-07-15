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

import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.WindowedExpression;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

/**
 * This plan node represents windowed aggregate computations.
 * The only one we implement now is windowed RANK.  But more
 * could be possible.
 */
public class PartitionByPlanNode extends AbstractPlanNode {
    public enum Members {
        WINDOWED_COLUMN
    };

    public PartitionByPlanNode() {
        setOutputSchema(new NodeSchema());
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.PARTITIONBY;
    }

    @Override
    public void resolveColumnIndexes() {
        /*
         * We need to resolve all the tves in the partition by and
         * order by expressions of the windowed expression.
         */
        assert (m_children.size() == 1);
        m_children.get(0).resolveColumnIndexes();
        NodeSchema input_schema = m_children.get(0).getOutputSchema();

        List<AbstractExpression> tves = getAllTVEs();
        // Resolve all of them.
        for (AbstractExpression ae : tves) {
            TupleValueExpression tve = (TupleValueExpression)ae;
            int index = tve.resolveColumnIndexesUsingSchema(input_schema);
            if (index == -1) {
                // check to see if this TVE is the aggregate output
                // XXX SHOULD MODE THIS STRING TO A STATIC DEF SOMEWHERE
                if (!tve.getTableName().equals("VOLT_TEMP_TABLE")) {
                    throw new RuntimeException("Unable to find index for column: " +
                                               tve.getColumnName());
                }
            } else {
                tve.setColumnIndex(index);
            }
        }
    }

    /**
     * This is used to get all the TVEs in the partition by plan node.  It's
     * only used for resolution of TVE indices.  But it may be useful in
     * testing that the TVEs are properly resolved,
     *
     * @return All the TVEs in expressions in this plan node.
     */
    public List<AbstractExpression> getAllTVEs() {
        List<AbstractExpression> tves =  new ArrayList<AbstractExpression>();
        // Get all the partition by expressions.
        for (AbstractExpression ae : getWindowedExpression().getPartitionByExpressions()) {
            tves.addAll(ae.findAllSubexpressionsOfClass(TupleValueExpression.class));
        }
        // Get all the order by expressions.
        for (AbstractExpression ae : getWindowedExpression().getOrderByExpressions()) {
            tves.addAll(ae.findAllSubexpressionsOfClass(TupleValueExpression.class));
        }
        // Get everything else.
        for (SchemaColumn col : getOutputSchema().getColumns()) {
            AbstractExpression expr = col.getExpression();
            if (col != null) {
                tves.addAll(expr.findAllSubexpressionsOfClass(TupleValueExpression.class));
            }
        }
        return tves;
    }

    /**
     * Generate the output schema.  This node will already have
     * an output schema with one column, which is a windowed aggregate
     * expression.  But we need to add the output schema of the one
     * child node.
     */
    @Override
    public void generateOutputSchema(Database db) {
        assert(getChildCount() == 1);

        // Do the children's generation.
        m_children.get(0).generateOutputSchema(db);

        // Now, generate the output columns for this plan node.  First
        // add the windowed column, and then add the input columns.
        // The output schema must have one column, which
        // is an windowed expression.
        assert(getOutputSchema() != null);
        assert(0 == getOutputSchema().getColumns().size());
        getOutputSchema().addColumn(m_windowedSchemaColumn);
        m_hasSignificantOutputSchema = true;
        NodeSchema inputSchema = getChild(0).getOutputSchema();
        assert(inputSchema != null);
        for (SchemaColumn schemaCol : inputSchema.getColumns()) {
            // We have to clone this because we will be
            // horsing around with the indices of TVEs.  However,
            // we don't need to resolve anything here, because
            // the default column index algorithm will work quite
            // nicely for us.
            SchemaColumn newCol = schemaCol.clone();
            getOutputSchema().addColumn(newCol);
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        String optionalTableName = "*NO MATCH -- USE ALL TABLE NAMES*";
        String newIndent = "  " + indent;
        StringBuilder sb = new StringBuilder(indent + "PARTITION BY PLAN\n");
        sb.append(newIndent + "PARTITION BY:\n");
        int numExprs = getNumberOfPartitionByExpressions();
        for (int idx = 0; idx < numExprs; idx += 1) {
            AbstractExpression ae = getPartitionByExpression(idx);
            // Apparently ae.toString() adds a trailing newline.  That's
            // unfortunate, but it works out ok here.
            sb.append("  ")
              .append(newIndent)
              .append(idx).append(": ")
              .append(ae.toString());
        }
        String sep = "";
        sb.append(newIndent).append("SORT BY:\n");
        numExprs = numberSortExpressions();
        for (int idx = 0; idx < numExprs; idx += 1) {
            AbstractExpression ae = getSortExpression(idx);
            SortDirectionType dir = getSortDirection(idx);
            sb.append(sep).append("  ")
              .append(newIndent)
              .append(idx).append(":")
              .append(ae.explain(optionalTableName))
              .append(" ")
              .append(dir.name());
            sep = "\n";
        }
        return sb.toString();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        if (m_windowedSchemaColumn != null) {
            stringer.key(Members.WINDOWED_COLUMN.name());
            m_windowedSchemaColumn.toJSONString(stringer, true);
        }
    }

    @Override
    public void loadFromJSONObject(JSONObject jobj, Database db) throws JSONException {
        JSONObject windowedColumn = (JSONObject) jobj.get(Members.WINDOWED_COLUMN.name());
        if (windowedColumn != null) {
            m_windowedSchemaColumn = SchemaColumn.fromJSONObject(windowedColumn);
        }
    }

    private WindowedExpression getWindowedExpression() {
        assert(m_windowedSchemaColumn != null);
        AbstractExpression abstractSE = m_windowedSchemaColumn.getExpression();
        assert(abstractSE instanceof WindowedExpression);
        return (WindowedExpression)abstractSE;
    }

    public AbstractExpression getPartitionByExpression(int idx) {
        WindowedExpression we = getWindowedExpression();
        return we.getPartitionByExpressions().get(idx);
    }

    public int getNumberOfPartitionByExpressions() {
        return getWindowedExpression().getPartitionbySize();
    }

    public AbstractExpression getSortExpression(int idx) {
        WindowedExpression we = getWindowedExpression();
        return we.getOrderByExpressions().get(idx);
    }

    public SortDirectionType getSortDirection(int idx) {
        WindowedExpression we = getWindowedExpression();
        return (we.getOrderByDirections().get(idx));
    }

    public int numberSortExpressions() {
        WindowedExpression we = getWindowedExpression();
        return we.getOrderbySize();
    }

    public void setWindowedColumn(SchemaColumn col) {
        m_windowedSchemaColumn = col;
    }

    @Override
    /**
     * This is an AggregatePlanNode.  Normally these don't need projection
     * nodes, but this one does.
     */
    public boolean planNodeClassNeedsProjectionNode() {
        return true;
    }

    private SchemaColumn       m_windowedSchemaColumn = null;
}
