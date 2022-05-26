/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package org.voltdb.plannodes;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.PlanNodeType;

public class MockPlanNode extends AbstractPlanNode {
    String m_tableName;
    String[] m_columnNames;
    boolean m_isOrderDeterministic = false;

    MockPlanNode(String tableName, String[] columnNames) {
        super();
        m_nondeterminismDetail = "no ordering was asserted for Mock Plan Node";
        m_tableName = tableName;
        m_columnNames = columnNames;
    }

    @Override
    public void generateOutputSchema(Database db) {
        m_outputSchema = new NodeSchema();
        m_hasSignificantOutputSchema = true;
        for (int i = 0; i < m_columnNames.length; ++i) {
            String colName = m_columnNames[i];
            TupleValueExpression tve = new TupleValueExpression(m_tableName,
                                                                m_tableName,
                                                                colName,
                                                                colName,
                                                                i);
            m_outputSchema.addColumn(m_tableName, m_tableName,
                    colName, colName,
                    tve);
        }
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return null;
    }

    @Override
    public void resolveColumnIndexes() {

    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "MOCK";
    }

    /**
     * Accessor for flag marking the plan as guaranteeing an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     * @return previously cached value.
     */
    @Override
    public boolean isOrderDeterministic() {
        return m_isOrderDeterministic;
    }

    /**
     * Write accessor for order determinism flag and optional description.
     * Also ensures consistency of content determinism flag (true -> true).
     */
    public void setOrderDeterminism(boolean b, String explanation) {
        m_isOrderDeterministic = b;
        if (m_isOrderDeterministic) {
            m_nondeterminismDetail = null;
        }
        else {
            m_nondeterminismDetail = explanation;
        }
    }

    @Override
    protected void loadFromJSONObject(JSONObject jobj, Database db)
            throws JSONException {
        helpLoadFromJSONObject(jobj, db);
    }

}
