/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.voltdb.plannodes;

import java.util.Arrays;

import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.json_voltpatches.JSONTokener;
import org.voltdb.MockVoltDB;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.WindowedExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.SortDirectionType;

import junit.framework.TestCase;

public class TestPartitionByNode extends TestCase {
    static private final String TABLE1 = "TABLE1";
    static private final String[] COLS = { "COL0", "COL1", "COL2", "COL3", "COL4" };
    static private final VoltType[] COLTYPES = { VoltType.INTEGER, VoltType.TINYINT,
                                         VoltType.TIMESTAMP, VoltType.FLOAT,
                                         VoltType.BIGINT };

    private MockVoltDB m_voltdb;

    @Override
    protected void setUp() {
        m_voltdb = new MockVoltDB();
        m_voltdb.addTable(TABLE1, false);
        for (int i = 0; i < COLS.length; ++i)
        {
            m_voltdb.addColumnToTable(TABLE1, COLS[i], COLTYPES[i], false, "",
                                      COLTYPES[i]);
        }
    }

    @Override
    protected void tearDown() throws Exception {
        m_voltdb.shutdown(null);
    }

    public void testJSON() throws Exception {
        WindowedExpression we = makeWindowedExpression();
        SchemaColumn col = new SchemaColumn("TMP", "TMP", "TC", "TC", we);
        PartitionByPlanNode pn = new PartitionByPlanNode(col);
        pn.setWindowedColumn(col);
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        try {
            pn.toJSONString(stringer);
            stringer.endObject();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        String json = stringer.toString();
        // Enable this to debug the JSON.
        System.out.printf("JSON: %s\n", json);
        JSONObject jobj = new JSONObject(new JSONTokener(json));
        PartitionByPlanNode pn2 = new PartitionByPlanNode(null);
        pn2.loadFromJSONObject(jobj, m_voltdb.getDatabase());
        int oldCount = pn.numberSortExpressions();
        int newCount = pn2.numberSortExpressions();
        assertEquals(oldCount, newCount);
        for (int idx = 0; idx < pn2.numberSortExpressions(); idx += 1) {
            AbstractExpression ae2 = pn2.getSortExpression(idx);
            AbstractExpression ae  = pn.getSortExpression(idx);
            assertTrue(ae2 instanceof TupleValueExpression);
            assertTrue(ae instanceof TupleValueExpression);
            TupleValueExpression tve2 = (TupleValueExpression)ae2;
            TupleValueExpression tve = (TupleValueExpression)ae;
            // Not everything gets serialized.  We don't serialize all
            // the names, for example.  But the metadata and the column
            // indices need to match.
            assertEquals(tve.getValueSize(), tve2.getValueSize());
            assertEquals(tve.getValueType(), tve2.getValueType());
            assertEquals(tve.getColumnIndex(), tve2.getColumnIndex());
            assertEquals(pn.getSortDirection(idx), pn2.getSortDirection(idx));
        }
        assertEquals(SortDirectionType.ASC,  pn.getSortDirection(0));
        assertEquals(SortDirectionType.DESC, pn.getSortDirection(1));
        assertEquals(VoltType.FLOAT,             pn.getSortExpression(0).getValueType());
        assertEquals(8,                          pn.getSortExpression(0).getValueSize());
        assertEquals(ExpressionType.VALUE_TUPLE, pn.getSortExpression(0).getExpressionType());
        assertEquals(VoltType.INTEGER,           pn.getSortExpression(1).getValueType());
        assertEquals(4,                          pn.getSortExpression(1).getValueSize());
        assertEquals(ExpressionType.VALUE_TUPLE, pn.getSortExpression(1).getExpressionType());
    }

    private WindowedExpression makeWindowedExpression() {
        TupleValueExpression tve1 = new TupleValueExpression("TABLE1", "TABLE1", "COL1", "COL1", 1);
        tve1.setValueType(VoltType.FLOAT);
        tve1.setValueSize(8);
        TupleValueExpression tve2 = new TupleValueExpression("TABLE1", "TABLE1", "COL1", "COL1", 2);
        tve2.setValueType(VoltType.INTEGER);
        tve2.setValueSize(4);
        TupleValueExpression tve3 = new TupleValueExpression("TABLE1", "TABLE1", "COL1", "COL1", 1);
        tve3.setValueType(VoltType.FLOAT);
        tve3.setValueSize(8);
        TupleValueExpression tve4 = new TupleValueExpression("TABLE1", "TABLE1", "COL1", "COL1", 2);
        tve4.setValueType(VoltType.INTEGER);
        tve4.setValueSize(4);
        WindowedExpression we = new WindowedExpression(ExpressionType.AGGREGATE_WINDOWED_RANK,
                                                       Arrays.asList(tve1, tve2),
                                                       Arrays.asList(tve3, tve4),
                                                       Arrays.asList(SortDirectionType.ASC, SortDirectionType.DESC),
                                                       m_voltdb.getDatabase(), true, false);
        return we;
    }
}
