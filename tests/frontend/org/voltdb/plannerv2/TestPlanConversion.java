/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.plannerv2;

import org.voltdb.types.PlannerType;

import java.util.HashMap;
import java.util.Map;

public class TestPlanConversion extends CalcitePlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSimpleSeqScan() {
        comparePlans("select si from R1");
    }

    public void testSeqScan() {
        comparePlans("select * from R1");
    }

    public void testSeqScanWithProjection() {
        comparePlans("select i, si from R1");
    }

    public void testSeqScanWithProjectionExpr() {
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        // in volt planner: int*int -> bigint; while in calcite: int*int -> int
        ignores.put("\"TYPE\":3,\"VALUE_TYPE\":5", "\"TYPE\":3,\"VALUE_TYPE\":6");
        comparePlans("select i * 5 from R1", ignores);
    }

    public void testSeqScanWithFilter() {
        comparePlans("select i from R1 where i = 5");
    }

    public void testSeqScanWithFilterParam() {
        comparePlans("select i from R1 where i = ? and v = ?");
    }

    public void testSeqScanWithStringFilter() {
        comparePlans("select i from R1 where v = 'FOO1'");
    }

    public void testSeqScanWithFilterWithTypeConversion() {
        // calcite adds a CAST expression to cast SMALLINT to INT
        Map<String, String> ignores = new HashMap<>();
        ignores.put("\"TYPE\":7,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}",
                "\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1");

        comparePlans("select i from R1 where si = 5", ignores);
    }

    public void testSeqScanWithLimit() {
        comparePlans("select i from R1 limit 5");
    }

    public void testSeqScanWithLimitParam() {
        Map<String, String> ignores = new HashMap<>();
        // Inline nodes ids are swapped
        String calciteProj = "\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        String voltProj = "\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        ignores.put(calciteProj, voltProj);

        String calciteLimit = "\"ID\":3,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        String voltLimit = "\"ID\":4,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        ignores.put(calciteLimit, voltLimit);

        // TODO: limit / offset as expressions or parameters
//        comparePlans("select i from R1 limit ?", ignores);
    }

    public void testSeqScanWithFilterAndLimit() {
        comparePlans("select i from R1 where si > 3 limit 5");
    }

    public void testSeqScanWithOffset() {
        comparePlans("select i from R1 offset 1");
    }

    public void testSeqScanWithLimitOffset() {
        comparePlans("select i from R1 limit 5 offset 1");
    }

    public void testSeqScanWithLimitOffsetSort() {
        comparePlans("select i from R1 order by bi limit 5 offset 1");
    }

    public void testSeqScanWithOrderByAndFilter() {
        comparePlans("select * from R1 where si > 3 order by i");

        // TODO: Calcite and VoltDB plans differ significantly
        // Calcite plan is SeqScan -> OrderBy -> Send
        // VoltDB plan is SeqScan -> OrderBy -> Projection -> Send
//        comparePlans("select i, bi, si from R1 where si > 3 order by i");
    }

    public void testSeqScanWithOrderBy1() {
        String sql = "select si from R1 order by i, si desc";

        // the inlined projection column order diffs
        // in volt it is I, SI
        // in Calcite it is SI, I
//        comparePlans(sql);
        String expectedCalcitePlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"ORDERBY\",\"CHILDREN_IDS\":[4],\"SORT_COLUMNS\":[{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1},\"SORT_DIRECTION\":\"ASC\"},{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0},\"SORT_DIRECTION\":\"DESC\"}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = toJSONPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedCalcitePlan, calcitePlan);
    }

    public void testSeqScanWithOrderBy2() {
        String sql = "select bi, i, si from R1 order by i, si desc";

        // Calcite and VoltDB plans differ significantly
        // Calcite plan is SeqScan -> OrderBy -> Send
        // VoltDB plan is SeqScan -> OrderBy -> Projection -> Send
//        comparePlans(sql);
        String expectedCalcitePlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"ORDERBY\",\"CHILDREN_IDS\":[3],\"SORT_COLUMNS\":[{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1},\"SORT_DIRECTION\":\"ASC\"},{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":2},\"SORT_DIRECTION\":\"DESC\"}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = toJSONPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedCalcitePlan, calcitePlan);
    }

    public void testSeqScanWithOrderByExpr() {
        String sql = "select bi, i, si from R1 order by i, si + 1 desc";

        // The si + 1 expression is added as a fourth output column to the R1 scan node
//        comparePlans(sql);
        String expectedCalcitePlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":2}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"ORDERBY\",\"CHILDREN_IDS\":[4],\"SORT_COLUMNS\":[{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1},\"SORT_DIRECTION\":\"ASC\"},{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":3},\"SORT_DIRECTION\":\"DESC\"}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"EXPR$3\",\"EXPRESSION\":{\"TYPE\":1,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":1}}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = toJSONPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedCalcitePlan, calcitePlan);
    }

    public void testSeqScanWithOrderByAndLimit() {
        String sql = "select bi, i, si from R1 order by i limit 5";

//        comparePlans(sql);
        // VoltDB SeqScan (Inline Projection) -> OrderBy (Inline Limit) -> Projection -> Send
        // Calcite SeqScan (Inline Projection) -> OrderBy (Inline Limit) -> Send
        // Calcite doesn't have the last project because it is exactly same with the Inline Projection in SeqScan
        String calcitePlan = toJSONPlan(sql, PlannerType.CALCITE);
        String expectedCalcitePlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"ORDERBY\",\"INLINE_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"LIMIT\",\"OFFSET\":0,\"LIMIT\":5,\"OFFSET_PARAM_IDX\":-1,\"LIMIT_PARAM_IDX\":-1,\"LIMIT_EXPRESSION\":null}],\"CHILDREN_IDS\":[4],\"SORT_COLUMNS\":[{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1},\"SORT_DIRECTION\":\"ASC\"}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        assertEquals(expectedCalcitePlan, calcitePlan);
    }

    public void testTypes() {
        // VARBINARY
        comparePlans("select vb from RTYPES");

        // VARCHAR
        comparePlans("select vc from RTYPES");

        // TMESTAMP
        comparePlans("select ts from RTYPES");

        // DECIMAL
        comparePlans("select d from RTYPES");

        // FLOAT
        comparePlans("select f from RTYPES");

        // BIGINT
        comparePlans("select bi from RTYPES");

        // SMALLINT
        comparePlans("select si from RTYPES");

        // TINYINT
        comparePlans("select ti from RTYPES");
    }

    public void testConstBinaryIntExpr() {
        String sql = "select 5 + 5 from R1";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        // Volt evaluates 5 + 5 into a single 10
        String calciteExpr = "\"TYPE\":1,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":5},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":5}";
        String voltExpr = "\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":10";
        ignores.put(calciteExpr, voltExpr);

        comparePlans(sql, ignores);
    }

    public void testBinaryIntExpr() {
        String sql = "select 5 + i from R1";
        Map<String, String> ignores = new HashMap<>();
        // different column name of expression
        ignores.put("EXPR$0", "C1");
        // No default NUMERIC type widening
        ignores.put("\"TYPE\":1,\"VALUE_TYPE\":5", "\"TYPE\":1,\"VALUE_TYPE\":6");

        comparePlans(sql, ignores);
    }

}
