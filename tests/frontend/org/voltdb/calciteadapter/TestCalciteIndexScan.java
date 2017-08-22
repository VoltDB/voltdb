/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter;

import org.apache.calcite.schema.SchemaPlus;
import org.voltdb.types.PlannerType;

public class TestCalciteIndexScan extends TestCalciteBase {

    SchemaPlus m_schemaPlus;

    @Override
    public void setUp() throws Exception {
        setupSchema(TestCalciteIndexScan.class.getResource("testcalcite-scan-ddl.sql"),
                "testcalcitescan", false);
        m_schemaPlus = schemaPlusFromDDL();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testIndexScanNoFilter() throws Exception {
        String sql;
        // Calcite produces a SeqScan here
        sql = "select * from RI1";
        comparePlans(sql);
    }

    public void testIndexScanNoFilter10() throws Exception {
        String sql;
        // Calcite produces a SeqScan here
        sql = "select i, bi from RI1";
        comparePlans(sql);
    }

    public void testIndexScan() throws Exception {
        String sql;
        sql = "select bi from RI1 where i > 45 and ti > 3";
        comparePlans(sql);
    }

    public void testIndexScan1() throws Exception {
        String sql;
        sql = "select * from RI2 where i > 5";
        comparePlans(sql);
    }

    public void testIndexScan2() throws Exception {
        String sql;
        sql = "select ti from RI2 where ti > 5 and i + si > 4 limit 3";
        // Node ids differ
        comparePlans(sql);
    }

    public void testExpressionIndexScan() throws Exception {
        String sql;
        sql = "select ti from RI1 where ti + 1 = 3";
        // Index INDEX RI1_IND3_EXPR ON RI1 (ti + 1)
        comparePlans(sql);
    }

    public void testPartialExpressionIndexScan() throws Exception {
        String sql;
        sql = "select ti from RI1 where ti + 1 = 3 and si is NULL";
        // Index INDEX RI1_IND3_EXPR ON RI1 (ti + 1) and si is NULL
        comparePlans(sql);
    }

    public void testNoPartialIndexScan() throws Exception {
        String sql;
        sql = "select ti from RI1 where si > 4 and tI * 3 > 10";
        //INDEX INDEX RI1_IND4_PART ON RI1 (si) WHERE tI * 2 > 10
        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"PREDICATE\":{\"TYPE\":20,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":4}},\"RIGHT\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":3,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":3}},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":10}}},\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testPartialIndexScan() throws Exception {
        String sql;
        sql = "select ti from RI1 where si > 4 and tI * 2 > 10";
        //INDEX INDEX RI1_IND4_PART ON RI1 (si) WHERE tI * 2 > 10
        comparePlans(sql);
    }

    public void testHashIndexScan() throws Exception {
        String sql;
        sql = "select ti from RI2 where ti + i = 10";
        //INDEX INDEX RI2_IND3_HASH ON RI2 (ti + i)
        comparePlans(sql);
    }


}
