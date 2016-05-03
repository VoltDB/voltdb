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
import org.voltdb.expressions.WindowedExpression;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

/**
 * This plan node represents windowed aggregate computations.
 * The only one we implement now is windowed RANK.  But more
 * could be possible.
 */
public class PartitionByPlanNode extends AggregatePlanNode {
    public enum Members {
        WINDOWED_COLUMN
    };

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.PARTITIONBY;
    }

    @Override
    public void resolveColumnIndexes() {
        super.resolveColumnIndexes();
    }

    @Override
    protected String explainPlanForNode(String indent) {
        String optionalTableName = "*NO MATCH -- USE ALL TABLE NAMES*";
        StringBuilder sb = new StringBuilder(" PARTITION BY PLAN: " + super.explainPlanForNode(indent) + "\n");
        sb.append(indent).append(" SORT BY: \n");
        for (int idx = 0; idx < numberSortExpressions(); idx += 1) {
            AbstractExpression ae = getSortExpression(idx);
            SortDirectionType dir = getSortDirection(idx);
            sb.append(indent)
               .append(ae.explain(optionalTableName))
               .append(": ")
               .append(dir.name())
               .append("\n");
        }
        sb.append("  PARTITION BY:\n");
        WindowedExpression we = getWindowedExpression();
        for (int idx = 0; idx < numberPartitionByExpressions(); idx += 1) {
            sb.append("  ")
              .append(idx).append(": ")
              .append(we.getPartitionByExpressions().get(idx).toString())
              .append("\n");
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
        super.loadFromJSONObject(jobj, db);
        JSONObject windowedColumn = (JSONObject) jobj.get(Members.WINDOWED_COLUMN.name());
        if (windowedColumn != null) {
            m_windowedSchemaColumn = SchemaColumn.fromJSONObject(windowedColumn);
        }
    }

    private WindowedExpression getWindowedExpression() {
        AbstractExpression abstractSE = m_windowedSchemaColumn.getExpression();
        assert(abstractSE instanceof WindowedExpression);
        return (WindowedExpression)abstractSE;
    }

    public AbstractExpression getPartitionByExpression(int idx) {
        WindowedExpression we = getWindowedExpression();
        return we.getPartitionByExpressions().get(idx);
    }

    public int numberPartitionByExpressions() {
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

    public void addWindowedColumn(SchemaColumn col) {
        m_windowedSchemaColumn = col;
    }

    public SchemaColumn getWindowedColumns() {
        return m_windowedSchemaColumn;
    }

    public void setWindowedColumn(SchemaColumn col) {
        m_windowedSchemaColumn = col;
    }
    SchemaColumn       m_windowedSchemaColumn;
}
