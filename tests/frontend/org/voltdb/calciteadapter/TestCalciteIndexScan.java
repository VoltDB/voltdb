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
        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testIndexScanNoFilter10() throws Exception {
        String sql;
        // Calcite produces a SeqScan here
        sql = "select i, bi from RI1";
        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}]}],\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testIndexScan() throws Exception {
        String sql;
        sql = "select bi from RI1 where i > 45 and ti > 3";
        comparePlans(sql);
    }

    public void testIndexScan1() throws Exception {
        String sql;
        sql = "select * from RI2 where i > 5";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("\"SORT_DIRECTION\":\"ASC\"", "\"SORT_DIRECTION\":\"INVALID\"");
        comparePlans(sql, ignores);
    }

    public void testIndexScan2() throws Exception {
        String sql;
        sql = "select ti from RI2 where ti > 5 and i + si > 4 limit 3";
        // Node ids differ
        Map<String, String> ignores = new HashMap<>();
        ignores.put("\"INLINE_NODES\":[{\"ID\":4", "\"INLINE_NODES\":[{\"ID\":3");
        ignores.put("\"ID\":3,\"PLAN_NODE_TYPE\":\"LIMIT\"", "\"ID\":4,\"PLAN_NODE_TYPE\":\"LIMIT\"");
        ignores.put("\"TYPE\":1,\"VALUE_TYPE\":5", "\"TYPE\":1,\"VALUE_TYPE\":6");
        comparePlans(sql, ignores);
    }

    public void testIndexScan3() throws Exception {
        String sql;
        sql = "select RI2.si from RI2  where 5 = RI2.si and RI2.I > 3;";

        //comparePlans(sql);
        // NLJ with inner IndexScan
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}]}],\"PREDICATE\":{\"TYPE\":20,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":5},\"RIGHT\":{\"TYPE\":7,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}},\"RIGHT\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":3}}},\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"GT\",\"SORT_DIRECTION\":\"INVALID\",\"TARGET_INDEX_NAME\":\"RI2_IND2\",\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":3}],\"COMPARE_NOTDISTINCT\":[false],\"SKIP_NULL_PREDICATE\":{\"TYPE\":9,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);

    }

    public void testExpressionIndexScan() throws Exception {
        String sql;
        sql = "select ti from RI1 where ti + 1 = 3";
        // Index INDEX RI1_IND3_EXPR ON RI1 (ti + 1)
        Map<String, String> ignores = new HashMap<>();
        ignores.put("\"TYPE\":1,\"VALUE_TYPE\":5", "\"TYPE\":1,\"VALUE_TYPE\":6");

        comparePlans(sql, ignores);
    }

    public void testPartialExpressionIndexScan() throws Exception {
        String sql;
        sql = "select ti from RI1 where ti + 1 = 3 and si is NULL";
        // Index INDEX RI1_IND3_EXPR ON RI1 (ti + 1) and si is NULL
        Map<String, String> ignores = new HashMap<>();
        ignores.put("\"TYPE\":1,\"VALUE_TYPE\":5", "\"TYPE\":1,\"VALUE_TYPE\":6");
        comparePlans(sql, ignores);

    }

    public void testNoPartialIndexScan() throws Exception {
        String sql;
        sql = "select ti from RI1 where si > 4 and tI * 3 > 10";
        //INDEX INDEX RI1_IND4_PART ON RI1 (si) WHERE tI * 2 > 10
        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"PREDICATE\":{\"TYPE\":20,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":4}},\"RIGHT\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":3,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":3}},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":10}}},\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\"}]}";
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
        sql = "select i from RI4 where i = 10";
        //INDEX INDEX RI4_IND1_HASH ON RI4 (i) is preferred over a scan one  RI4_IND2 ON RI4 (i)
        comparePlans(sql);
    }

    public void testIndexScanPartitioned1() throws Exception {
        String sql;
        sql = "select I from PI1 where I > 0";
        comparePlans(sql);
    }

    public void testIndexScanPartitioned2() throws Exception {
        String sql;
        sql = "select i * 5 from PI1 where I > 0";
        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}]}{\"PLAN_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[4]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":3,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":5}}}]}],\"TARGET_TABLE_NAME\":\"PI1\",\"TARGET_TABLE_ALIAS\":\"PI1\",\"LOOKUP_TYPE\":\"GT\",\"SORT_DIRECTION\":\"INVALID\",\"TARGET_INDEX_NAME\":\"VOLTDB_AUTOGEN_IDX_PK_PI1_I\",\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":0}],\"COMPARE_NOTDISTINCT\":[false],\"SKIP_NULL_PREDICATE\":{\"TYPE\":9,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);

    }

    public void testIndexScanPartitioned3() throws Exception {
        String sql;
        sql = "select I from PI1 where I = 0";
        comparePlans(sql);
    }

    public void testIndexScanPartitioned31() throws Exception {
        String sql;
        sql = "select I from PI1 where cast(I as integer) = 0";
        comparePlans(sql);
    }

    public void testIndexScanPartitioned4() throws Exception {
        String sql;
        // Index on non-partition column
        sql = "select I from PI1 where II > 0";
        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}]}{\"PLAN_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[4]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"PI1\",\"TARGET_TABLE_ALIAS\":\"PI1\",\"LOOKUP_TYPE\":\"GT\",\"SORT_DIRECTION\":\"INVALID\",\"TARGET_INDEX_NAME\":\"PI1_IND1\",\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":0}],\"COMPARE_NOTDISTINCT\":[false],\"SKIP_NULL_PREDICATE\":{\"TYPE\":9,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);

    }

}
