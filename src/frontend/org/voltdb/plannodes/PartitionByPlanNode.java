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

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

/**
 * This plan node represents windowed aggregate computations.
 * The only one we implement now is windowed RANK.  But more
 * could be possible.
 */
public class PartitionByPlanNode extends HashAggregatePlanNode {
    public enum Members {
        SORT_COLUMNS,
        SORT_EXPRESSION,
        SORT_DIRECTION
    };

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.PARTITIONBY;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        for (AbstractExpression expr : m_sortExpressions) {
            expr.validate();
        }
    }

    @Override
    public void resolveColumnIndexes() {
        super.resolveColumnIndexes();
        assert(m_children.size() > 0);
        m_children.get(0).resolveColumnIndexes();
        NodeSchema input_schema = m_children.get(0).getOutputSchema();
        // get all the TVEs in the output columns
        List<TupleValueExpression> sort_tves =
            new ArrayList<TupleValueExpression>();
        for (AbstractExpression expr : m_sortExpressions) {
            sort_tves.addAll(ExpressionUtil.getTupleValueExpressions(expr));
        }
        // and update their indexes against the table schema
        for (TupleValueExpression tve : sort_tves)
        {
            int index = tve.resolveColumnIndexesUsingSchema(input_schema);
            tve.setColumnIndex(index);
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        String optionalTableName = "*NO MATCH -- USE ALL TABLE NAMES*";
        StringBuilder sb = new StringBuilder(" PARTIION PLAN: " + super.explainPlanForNode(indent));
        sb.append(indent).append("SORT BY: \n");
        for (int idx = 0; idx < m_sortExpressions.size(); idx += 1) {
            AbstractExpression ae = m_sortExpressions.get(idx);
            SortDirectionType dir = m_sortDirections.get(idx);
            sb.append(indent)
               .append(ae.explain(optionalTableName))
               .append(": ")
               .append(dir.name())
               .append("\n");
        }
        sb.append(indent).append("PARTITION BY:\n");
        return sb.toString();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        assert (m_sortExpressions.size() == m_sortDirections.size());
        /*
         * Serialize the sort columns.
         */
        stringer.key(Members.SORT_COLUMNS.name()).array();
        for (int ii = 0; ii < m_sortExpressions.size(); ii++) {
            stringer.object();
            stringer.key(Members.SORT_EXPRESSION.name());
            stringer.object();
            m_sortExpressions.get(ii).toJSONString(stringer);
            stringer.endObject();
            stringer.key(Members.SORT_DIRECTION.name()).value(m_sortDirections.get(ii).toString());
            stringer.endObject();
        }
        stringer.endArray();
    }

    @Override
    public void loadFromJSONObject(JSONObject jobj, Database db) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        m_sortExpressions.clear();
        m_sortDirections.clear();

        /*
         * Unfortunately we cannot use AbstractExpression.loadFromJSONArrayChild here,
         * as we need to get a sort expression and a sort order for each column.
         */
        if (jobj.has(Members.SORT_COLUMNS.name())) {
            JSONArray jarray = jobj.getJSONArray(Members.SORT_COLUMNS.name());
            int size = jarray.length();
            for (int ii = 0; ii < size; ii += 1) {
                JSONObject tempObj = jarray.getJSONObject(ii);
                m_sortDirections.add( SortDirectionType.get(tempObj.getString( Members.SORT_DIRECTION.name())) );
                m_sortExpressions.add( AbstractExpression.fromJSONChild(tempObj, Members.SORT_EXPRESSION.name()) );
            }
        }
    }

    public void addSortExpression(AbstractExpression ae,
                                  SortDirectionType  dir) {
        m_sortExpressions.add(ae);
        m_sortDirections.add(dir);
    }

    public AbstractExpression getSortExpression(int idx) {
        return m_sortExpressions.get(idx);
    }

    public SortDirectionType getSortDirection(int idx) {
        return m_sortDirections.get(idx);
    }

    public int numberSortExpressions() {
        return m_sortExpressions.size();
    }
    private List<AbstractExpression> m_sortExpressions = new ArrayList<AbstractExpression>();
    private List<SortDirectionType>  m_sortDirections = new ArrayList<SortDirectionType>();
}
