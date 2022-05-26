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
import java.util.List;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;

/**
 * This type of plan node wraps a subquery expression for queries like this
 *
 *  SELECT * FROM T WHERE (T.f, T.f) > (SELECT R.f R.g FROM R LIMIT 1);
 *
 */
public class TupleScanPlanNode extends AbstractScanPlanNode {

    public enum Members {
        PARAM_IDX;
    }

    private List<AbstractExpression> m_columnList =
            new ArrayList<>();

    public TupleScanPlanNode() {
        super();
        m_isSubQuery = true;
        m_hasSignificantOutputSchema = true;
    }

    /*
     * @param
     * @param
     */
    public TupleScanPlanNode(String subqueryName, List<AbstractExpression> columnExprs) {
        super(subqueryName, subqueryName);
        m_isSubQuery = true;
        m_hasSignificantOutputSchema = true;
        // copy columns
        for (AbstractExpression columnExpr : columnExprs) {
            m_columnList.add(columnExpr.clone());
        }
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.TUPLESCAN;
    }

    @Override
    public void generateOutputSchema(Database db) {
        if (m_tableSchema == null) {
            m_tableSchema = new NodeSchema();
            int columnIdx = 1;
            for (AbstractExpression colExpr : m_columnList) {
                assert(colExpr instanceof ParameterValueExpression);
                ParameterValueExpression pve = (ParameterValueExpression) colExpr;
                // must produce a tuple value expression for this column.
                String columnName = "C" + Integer.toString(columnIdx);
                TupleValueExpression tve = new TupleValueExpression(
                        m_targetTableName, m_targetTableAlias,
                        columnName, columnName,
                        pve, columnIdx);
                m_tableSchema.addColumn(
                        m_targetTableName, m_targetTableAlias,
                        columnName, columnName,
                        tve);
                ++columnIdx;
            }
            m_outputSchema = m_tableSchema;
            m_hasSignificantOutputSchema = true;
        }
    }

    @Override
    public void resolveColumnIndexes() {
        // output columns
        for (SchemaColumn col : m_outputSchema) {
            AbstractExpression colExpr = col.getExpression();
            // At this point, they'd better all be TVEs.
            assert(colExpr instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) colExpr;
            tve.setColumnIndexUsingSchema(m_tableSchema);
        }
        m_outputSchema.sortByTveIndex();
    }

    @Override
    public String explainPlanForNode(String indent) {
        StringBuilder result = new StringBuilder("(");
        String connector = "";
        for (AbstractExpression arg : m_columnList) {
            result.append(connector).append(arg.explain(indent));
            connector = ", ";
        }
        result.append(")");
        return result.toString();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        // Output the correlated parameter ids
        stringer.key(Members.PARAM_IDX.name()).array();
        for (AbstractExpression colExpr : m_columnList) {
            assert(colExpr instanceof ParameterValueExpression);
            ParameterValueExpression pve = (ParameterValueExpression) colExpr;
            stringer.value(pve.getParameterIndex());
         }
        stringer.endArray();
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        if (jobj.has(Members.PARAM_IDX.name())) {
            JSONArray paramIdxArray = jobj.getJSONArray(Members.PARAM_IDX.name());
            int paramSize = paramIdxArray.length();
            assert(m_outputSchema != null && paramSize == m_outputSchema.size());
            for (int i = 0; i < paramSize; ++i) {
                int paramIdx = paramIdxArray.getInt(i);
                ParameterValueExpression pve = new ParameterValueExpression();
                pve.setParameterIndex(paramIdx);
                AbstractExpression expr = m_outputSchema.getColumn(i).getExpression();
                pve.setValueSize(expr.getValueSize());
                pve.setValueType(expr.getValueType());
                m_columnList.add(pve);
            }
        }
    }

}
