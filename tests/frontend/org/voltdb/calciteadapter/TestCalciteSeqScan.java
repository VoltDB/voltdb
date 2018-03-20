/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.SchemaPlus;
import org.voltdb.types.PlannerType;

public class TestCalciteSeqScan extends TestCalciteBase {

    SchemaPlus m_schemaPlus;

    @Override
    public void setUp() throws Exception {
        setupSchema(TestCalciteSeqScan.class.getResource("testcalcite-scan-ddl.sql"),
                "testcalcitescan", false);
        m_schemaPlus = schemaPlusFromDDL();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSeqScan() throws Exception {
        String sql;
        sql = "select * from R1";
        comparePlans(sql);
    }

    public void testSeqScanWithProjection() throws Exception {
        String sql;
        sql = "select i, si from R1";
        comparePlans(sql);
    }

    public void testSeqScanWithProjectionExpr() throws Exception {
        String sql;
        sql = "select i * 5 from R1";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        ignores.put("\"TYPE\":3,\"VALUE_TYPE\":5", "\"TYPE\":3,\"VALUE_TYPE\":6");
        comparePlans(sql, ignores);
    }

    public void testSeqScanWithFilter() throws Exception {
        String sql;
        sql = "select i from R1 where i = 5";
        comparePlans(sql);
    }

    public void testSeqScanWithFilterParam() throws Exception {
        String sql;
        sql = "select i from R1 where i = ? and v = ?";
        comparePlans(sql);
    }

    public void testSeqScanWithStringFilter() throws Exception {
        String sql;
        sql = "select i from R1 where v = 'FOO1'";
        comparePlans(sql);
    }

    public void testSeqScanWithFilterWithTypeConversion() throws Exception {
        String sql;
        sql = "select i from R1 where si = 5";
        // calcite adds a CAST expression to cast SMALLINT to INT
        Map<String, String> ignores = new HashMap<>();
        ignores.put("\"TYPE\":7,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}",
                "\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1");

        comparePlans(sql, ignores);
    }

    public void testSeqScanPartitioned() throws Exception {
        String sql;
        sql = "select * from P1";
        comparePlans(sql);
    }

    public void testSeqScanPartitioned1() throws Exception {
        String sql;
        sql = "select i from P1";
        comparePlans(sql);
    }

    public void testSeqScanPartitioned2() throws Exception {
        String sql;
        sql = "select cast(i as bigint) * 5 from P1";
        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":0}}]}]}{\"PLAN_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[4]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":3,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":5}}}]}],\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testSeqScanPartitioned3() throws Exception {
        String sql;
        sql = "select i * 5 from P1";
        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}]}{\"PLAN_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[4]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":3,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":5}}}]}],\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);

    }

    public void testSeqScanWithFilterPartitioned() throws Exception {
        String sql;
        sql = "select i from P1 where si = 5";
        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}]}{\"PLAN_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[4]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":7,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":5}},\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);

    }

}
