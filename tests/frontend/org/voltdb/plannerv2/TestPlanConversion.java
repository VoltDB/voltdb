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
        String sql;
        sql = "select i from R1 order by bi limit 5 offset 1";

        Map<String, String> ignores = new HashMap<>();
        // Inline nodes ids are swapped
        String calciteProj = "\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        String voltProj = "\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        ignores.put(calciteProj, voltProj);

        String calciteLimit = "\"ID\":3,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        String voltLimit = "\"ID\":4,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        ignores.put(calciteLimit, voltLimit);

        comparePlans(sql);
    }
}
