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

public class TestCalciteExpressions extends TestCalciteBase {

    SchemaPlus m_schemaPlus;

    @Override
    public void setUp() throws Exception {
        setupSchema(TestCalciteExpressions.class.getResource("testcalcite-scan-ddl.sql"),
                "testcalcitescan", false);
        m_schemaPlus = schemaPlusFromDDL();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testConstBinaryIntExpr() throws Exception {
        String sql;
        sql = "select 5 + 5 from R1";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        // HSQL evaluates 5 + 5 into a single 10
        String calciteExpr = "\"TYPE\":1,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":5},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":5}";
        String voltExpr = "\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":10";
        ignores.put(calciteExpr, voltExpr);

        comparePlans(sql, ignores);
    }

    public void testConstIntExpr() throws Exception {
        String sql;
        sql = "select 5 from R1";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

        comparePlans(sql, ignores);
    }

    public void testConstRealExpr() throws Exception {
        String sql;
        sql = "select 5.1 from R1";
        // VoltDB does not support NUMERIC consts in display const expressions
        testPlan(sql, PlannerType.CALCITE);
    }

    // Strings
    public void testConstStringExpr() throws Exception {
        String sql;
        sql = "select 'FOO' from R1";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

        comparePlans(sql, ignores);
    }

    public void testFailConstBinaryStringExpr() throws Exception {
        String sql;
        sql = "select '55' + '22' from R1";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

        failToCompile(PlannerType.CALCITE, sql, "Cannot apply '+' to arguments of type '<CHAR(2)> + <CHAR(2)>'");
        testPlan(sql, PlannerType.VOLTDB);
    }

    public void testFailMismatchStringExpr() throws Exception {
        String sql;
        sql = "select '55' + 22 from R1";

        failToCompile(PlannerType.CALCITE, sql, "Cannot apply '+' to arguments of type '<CHAR(2)> + <INTEGER>'");
    }

    public void testConcatConstStringExpr() throws Exception {
        String sql;
        sql = "select '55' || '22' from R1";
        testPlan(sql, PlannerType.CALCITE);
        testPlan(sql, PlannerType.VOLTDB);
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        // HSQL concatenates the strings while parcing
        String calciteExpr = "\"TYPE\":100,\"VALUE_TYPE\":9,\"VALUE_SIZE\":1048576,\"ARGS\":[{\"TYPE\":30,\"VALUE_TYPE\":9,\"VALUE_SIZE\":1048576,\"ISNULL\":false,\"VALUE\":\"55\"},{\"TYPE\":30,\"VALUE_TYPE\":9,\"VALUE_SIZE\":1048576,\"ISNULL\":false,\"VALUE\":\"22\"}],\"NAME\":\"concat\",\"FUNCTION_ID\":124";
        String voltDBExpr = "\"TYPE\":30,\"VALUE_TYPE\":9,\"VALUE_SIZE\":1048576,\"ISNULL\":false,\"VALUE\":\"5522\"";
        ignores.put(calciteExpr, voltDBExpr);

        comparePlans(sql, ignores);
    }

    public void testConcatStringExpr() throws Exception {
        String sql;
        sql = "select v || '22' from R1";
        testPlan(sql, PlannerType.CALCITE);
        testPlan(sql, PlannerType.VOLTDB);
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

        comparePlans(sql, ignores);
    }

}
