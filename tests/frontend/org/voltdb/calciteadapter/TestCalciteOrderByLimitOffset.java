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

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.SchemaPlus;
import org.voltdb.types.PlannerType;

public class TestCalciteOrderByLimitOffset extends TestCalciteBase {

    SchemaPlus m_schemaPlus;

    @Override
    public void setUp() throws Exception {
        setupSchema(TestCalciteOrderByLimitOffset.class.getResource("testcalcite-scan-ddl.sql"),
                "testcalcitescan", false);
        m_schemaPlus = schemaPlusFromDDL();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSeqScanWithLimit() throws Exception {
        String sql;
        sql = "select i from R1 limit 5";

        Map<String, String> ignores = new HashMap<>();
        // Inline nodes ids are swapped
        String calciteProj = "\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        String voltProj = "\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        ignores.put(calciteProj, voltProj);

        String calciteLimit = "\"ID\":3,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        String voltLimit = "\"ID\":4,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        ignores.put(calciteLimit, voltLimit);

        comparePlans(sql, ignores);
    }

    public void testSeqScanWithLimitParam() throws Exception {
        String sql;
        sql = "select i from R1 limit ?";

        Map<String, String> ignores = new HashMap<>();
        // Inline nodes ids are swapped
        String calciteProj = "\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        String voltProj = "\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        ignores.put(calciteProj, voltProj);

        String calciteLimit = "\"ID\":3,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        String voltLimit = "\"ID\":4,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        ignores.put(calciteLimit, voltLimit);

        comparePlans(sql, ignores);
    }

    public void testSeqScanWithFilterAndLimit() throws Exception {
        String sql;
        sql = "select i from R1 where si > 3 limit 5";

        Map<String, String> ignores = new HashMap<>();
        // Inline nodes ids are swapped
        String calciteProj = "\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        String voltProj = "\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        ignores.put(calciteProj, voltProj);

        String calciteLimit = "\"ID\":3,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        String voltLimit = "\"ID\":4,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        ignores.put(calciteLimit, voltLimit);

        // @TODO Need rule to swap Sort and Filter
        comparePlans(sql, ignores);
    }

    public void testSeqScanWithOffset() throws Exception {
        String sql;
        sql = "select i from R1 offset 1";

        Map<String, String> ignores = new HashMap<>();
        // Inline nodes ids are swapped
        String calciteProj = "\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        String voltProj = "\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        ignores.put(calciteProj, voltProj);

        String calciteLimit = "\"ID\":3,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        String voltLimit = "\"ID\":4,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        ignores.put(calciteLimit, voltLimit);

        comparePlans(sql, ignores);
    }

    public void testSeqScanWithLimitOffset() throws Exception {
        String sql;
        sql = "select i from R1 limit 5 offset 1";

        Map<String, String> ignores = new HashMap<>();
        // Inline nodes ids are swapped
        String calciteProj = "\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        String voltProj = "\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        ignores.put(calciteProj, voltProj);

        String calciteLimit = "\"ID\":3,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        String voltLimit = "\"ID\":4,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        ignores.put(calciteLimit, voltLimit);

        comparePlans(sql, ignores);
    }

    public void testSeqScanWithOrderBy() throws Exception {
        String sql;
        sql = "select bi, i, si from R1 order by i, si desc";

        // Calcite and VoltDB plans differ significantly
        // Calcite plan is SeqScan -> OrderBy -> Send
        // VoltDB plan is SeqScan -> OrderBy -> Projection -> Send
        String expectedCalcitePlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"ORDERBY\",\"CHILDREN_IDS\":[3],\"SORT_COLUMNS\":[{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1},\"SORT_DIRECTION\":\"ASC\"},{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":2},\"SORT_DIRECTION\":\"DESC\"}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedCalcitePlan, calcitePlan);
    }

    public void testSeqScanWithOrderByExpr() throws Exception {
        String sql;
        sql = "select bi, i, si from R1 order by i, si + 1 desc";

        // The si + 1 expression is added as a fourth output column to the R1 scan node
        String expectedCalcitePlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"ORDERBY\",\"CHILDREN_IDS\":[4],\"SORT_COLUMNS\":[{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1},\"SORT_DIRECTION\":\"ASC\"},{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":3},\"SORT_DIRECTION\":\"DESC\"}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"EXPR$3\",\"EXPRESSION\":{\"TYPE\":1,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":1}}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedCalcitePlan, calcitePlan);
    }

    public void testSeqScanWithOrderByAndLimit() throws Exception {
        String sql;
        sql = "select bi, i, si from R1 order by i limit 5";

        // SeqScan (Inline Projection) -> OrderBy (Inline Limit) -> Send
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        String expectedCalcitePlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"ORDERBY\",\"INLINE_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"LIMIT\",\"OFFSET\":0,\"LIMIT\":5,\"OFFSET_PARAM_IDX\":-1,\"LIMIT_PARAM_IDX\":-1,\"LIMIT_EXPRESSION\":null}],\"CHILDREN_IDS\":[4],\"SORT_COLUMNS\":[{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1},\"SORT_DIRECTION\":\"ASC\"}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        assertEquals(expectedCalcitePlan, calcitePlan);
    }

}
