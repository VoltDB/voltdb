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

    // VARBINARY
    public void testVarbinaryExpr() throws Exception {
        String sql;
        sql = "select vb from RTYPES";
        comparePlans(sql);
    }

    // VARCHAR
    public void testVarcharExpr() throws Exception {
        String sql;
        sql = "select vc from RTYPES";
        comparePlans(sql);
    }

    // TMESTAMP
    public void testTimestampExpr() throws Exception {
        String sql;
        sql = "select ts from RTYPES";
        comparePlans(sql);
    }

    // DECIMAL
    public void testDecimalExpr() throws Exception {
        String sql;
        sql = "select d from RTYPES";
        comparePlans(sql);
    }

    // FLOAT
    public void testFloatExpr() throws Exception {
        String sql;
        sql = "select f from RTYPES";
        comparePlans(sql);
    }

    // BIGINT
    public void testBigintExpr() throws Exception {
        String sql;
        sql = "select bi from RTYPES";
        comparePlans(sql);
    }

    // BIGINT
    public void testSmallintExpr() throws Exception {
        String sql;
        sql = "select si from RTYPES";
        comparePlans(sql);
    }

    // TINYINT
    public void testTinyintExpr() throws Exception {
        String sql;
        sql = "select ti from RTYPES";
        comparePlans(sql);
    }

    // INTEGER
    public void testIntegerExpr() throws Exception {
        String sql;
        sql = "select i from RTYPES";
        comparePlans(sql);
    }

    public void testConstBinaryIntExpr() throws Exception {
        String sql;
        sql = "select 5 + 5 from R1";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        // HSQL evaluates 5 + 5 into a single 10
        String calciteExpr = "\"TYPE\":1,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":5},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":5}";
        String voltExpr = "\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":10";
        ignores.put(calciteExpr, voltExpr);

        comparePlans(sql, ignores);
    }

    public void testBinaryIntExpr() throws Exception {
        String sql;
        sql = "select 5 + i from R1";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        // No default NUMERIC type widening
        ignores.put("\"TYPE\":1,\"VALUE_TYPE\":5", "\"TYPE\":1,\"VALUE_TYPE\":6");

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
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

        comparePlans(sql, ignores);
    }

    // Arithmetic Expressions
    public void testPlusExpr() throws Exception {
        String sql;
        sql = "select i + si from R1";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        // No default NUMERIC type widening
        ignores.put("\"TYPE\":1,\"VALUE_TYPE\":5", "\"TYPE\":1,\"VALUE_TYPE\":6");

        comparePlans(sql, ignores);
    }
    public void testMinusExpr() throws Exception {
        String sql;
        sql = "select i - si from R1";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        // No default NUMERIC type widening
        ignores.put("\"TYPE\":2,\"VALUE_TYPE\":5", "\"TYPE\":2,\"VALUE_TYPE\":6");

        comparePlans(sql, ignores);
    }
    public void testMultExpr() throws Exception {
        String sql;
        sql = "select i * si from R1";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        // No default NUMERIC type widening
        ignores.put("\"TYPE\":3,\"VALUE_TYPE\":5", "\"TYPE\":3,\"VALUE_TYPE\":6");

        comparePlans(sql, ignores);
    }
    public void testDivExpr() throws Exception {
        String sql;
        sql = "select i / si from R1";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        // No default NUMERIC type widening
        ignores.put("\"TYPE\":4,\"VALUE_TYPE\":5", "\"TYPE\":4,\"VALUE_TYPE\":6");

        comparePlans(sql, ignores);
    }

    // TIMESTAMP
    public void testInvalidDatetimeExpr() throws Exception {
        String sql;
        sql = "select ts + 10 from RTYPES";
        failToCompile(PlannerType.CALCITE, sql, "Cannot apply '+' to arguments of type '<TIMESTAMP(0)> + <INTEGER>'");
    }
    public void testDatetimeExpr1() throws Exception {
        String sql;
        sql = "select ts from RTYPES";
        comparePlans(sql);
    }
    public void testDatetimeConstExpr() throws Exception {
        String sql;
        sql = "select TIMESTAMP '1969-07-20 20:17:40' from RTYPES";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        comparePlans(sql, ignores);
    }
    public void testDatetimeConstMinusExpr() throws Exception {
        String sql;
        // Can not compare plans directly since HSQL simplifies the expression into a CVE during parsing
        sql = "select TIMESTAMP '1969-07-20 20:17:40' - INTERVAL '1' DAY from RTYPES";

        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        String calcitePlanJSON = testPlan(sql, PlannerType.CALCITE);

        // Equivalent VoltDB query
        sql = "select TO_TIMESTAMP(Microsecond, SINCE_EPOCH(Microsecond,TIMESTAMP '1969-07-20 20:17:40') - 86400000000) from rtypes";
        String voltdbPlanJSON = testPlan(sql, PlannerType.VOLTDB);
        compareJSONPlans(calcitePlanJSON, voltdbPlanJSON, ignores);
    }
    public void testDatetimeMinusExpr() throws Exception {
        String sql;
        // Can not compare plans directly since HSQL simplifies the expression into a CVE during parsing
        sql = "select ts - INTERVAL '1' DAY from RTYPES";

        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        String calcitePlanJSON = testPlan(sql, PlannerType.CALCITE);

        // Equivalent VoltDB query
        sql = "select TO_TIMESTAMP(Microsecond, SINCE_EPOCH(Microsecond,ts) - 86400000000) from rtypes";
        String voltdbPlanJSON = testPlan(sql, PlannerType.VOLTDB);
        compareJSONPlans(calcitePlanJSON, voltdbPlanJSON, ignores);
    }

    public void testDatetimeConstPlusExpr() throws Exception {
        String sql;
        // Can not compare plans directly since HSQL simplifies the expression into a CVE during parsing
        sql = "select TIMESTAMP '1969-07-20 20:17:40' + INTERVAL '1' DAY from RTYPES";

        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        String calcitePlanJSON = testPlan(sql, PlannerType.CALCITE);

        // Equivalent VoltDB query
        sql = "select TO_TIMESTAMP(Microsecond, SINCE_EPOCH(Microsecond,TIMESTAMP '1969-07-20 20:17:40') + 86400000000) from rtypes";
        String voltdbPlanJSON = testPlan(sql, PlannerType.VOLTDB);
        compareJSONPlans(calcitePlanJSON, voltdbPlanJSON, ignores);
    }
    public void testDatetimePlusExpr() throws Exception {
        String sql;
        // Can not compare plans directly since HSQL simplifies the expression into a CVE during parsing
        sql = "select ts + INTERVAL '1' DAY from RTYPES";

        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        String calcitePlanJSON = testPlan(sql, PlannerType.CALCITE);

        // Equivalent VoltDB query
        sql = "select TO_TIMESTAMP(Microsecond, SINCE_EPOCH(Microsecond, ts) + 86400000000) from rtypes";
        String voltdbPlanJSON = testPlan(sql, PlannerType.VOLTDB);
        compareJSONPlans(calcitePlanJSON, voltdbPlanJSON, ignores);
    }

    public void testDatetimePlusParamExpr() throws Exception {
        String sql;
        // Can not compare plans directly since HSQL simplifies the expression into a CVE during parsing
        sql = "select ts + INTERVAL ? DAY from RTYPES";

        failToCompile(PlannerType.CALCITE, sql, "Encountered \"+ INTERVAL ?\"");

        // Equivalent VoltDB query
        sql = "select TO_TIMESTAMP(Microsecond, SINCE_EPOCH(Microsecond, ts) + ?) from rtypes";
        String voltdbPlanJSON = testPlan(sql, PlannerType.VOLTDB);
//        compareJSONPlans(calcitePlanJSON, voltdbPlanJSON, ignores);
    }

    public void testSinceEpochExpr() throws Exception {
        String sql;
        sql = "select SINCE_EPOCH(i) from RTYPES";
        String calcitePlanJSON = testPlan(sql, PlannerType.CALCITE);

//        final String viewSql = "select \"empid\" as EMPLOYEE_ID,\n"
//                + "  \"name\" || ' ' || \"name\" as EMPLOYEE_NAME,\n"
//                + "  \"salary\" as EMPLOYEE_SALARY,\n"
//                + "  POST.MY_INCREMENT(\"empid\", 10) as INCREMENTED_SALARY\n"
//                + "from \"hr\".\"emps\"";
//        testPlan(viewSql, PlannerType.CALCITE);

//        String voltdbPlanJSON = testPlan(sql, PlannerType.VOLTDB);
//        sql = "select TO_TIMESTAMP('1969-07-20' , 'YYYY-MM-DD') from rtypes";
//        testPlan(sql, PlannerType.CALCITE);
    }

    public void testToTimestampExpr() throws Exception {
        String sql;
        sql = "select TO_TIMESTAMP(Second, SINCE_EPOCH(Second,ts) + 3600) from rtypes";
        testPlan(sql, PlannerType.VOLTDB);
//        sql = "select TO_TIMESTAMP('1969-07-20' , 'YYYY-MM-DD') from rtypes";
//        testPlan(sql, PlannerType.CALCITE);
    }

   public void testDatetimeExpr() throws Exception {
        String sql;
        sql = "select ts + INTERVAL '45' DAY from rtypes";
        testPlan(sql, PlannerType.CALCITE);
    }

    public void testFailMismatchOperandExpr() throws Exception {
        String sql;
        sql = "select vb + i from RTYPES";

        failToCompile(PlannerType.CALCITE, sql, "Cannot apply '+' to arguments of type '<VARBINARY(1024)> + <INTEGER>'");
    }

    // Conjunction
    public void testConjunctionAndExpr() throws Exception {
        String sql;
        sql = "select 1 from RTYPES where i = 1 and vc = 'foo'";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

        comparePlans(sql, ignores);
    }
    public void testConjunctionOrExpr() throws Exception {
        String sql;
        sql = "select 1 from RTYPES where i = 1 or i = 4";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

        comparePlans(sql, ignores);
    }

    // Compare
    public void testCompareInExpr1() throws Exception {
        String sql;
        sql = "select 1 from RTYPES where i IN (1, ?, 3)";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

        // Calcite transforms the IN expression into ORs during the transformation stage
        comparePlans(sql, ignores);
    }
    public void testCompareInExpr2() throws Exception {
        String sql;
        sql = "select 1 from RTYPES where i IN (?)"; // Calcite Regular EQUAL
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

        // Calcite transforms the IN expression with a single value into EQUAL expression
        comparePlans(sql, ignores);
    }
    public void testCompareInExpr3() throws Exception {
        String sql;
        sql = "select 1 from RTYPES where i IN ?"; // Calcite Regular EQUAL
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

        // Calcite transforms the IN expression with a single value into EQUAL expression
        comparePlans(sql, ignores);
    }
    public void testCompareInExpr4() throws Exception {
        String sql;
        sql = "select 1 from RTYPES where i IN (1, 2)"; // Calcite Regular OR
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

        // Calcite transforms the IN expression into ORs during the transformation stage
        comparePlans(sql, ignores);
    }

    public void testCompareLikeExpr1() throws Exception {
        String sql;
        sql = "select 1 from RTYPES where vc LIKE 'ab%c'";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

        // HSQL transfroms the LIKE 'ab%c' expression into
        // (((VC LIKE 'ab%c') AND (VC >= 'ab')) AND (VC <= 'ab�'))
        comparePlans(sql, ignores);
    }
    public void testCompareLikeExprWithParam() throws Exception {
        String sql;
        sql = "select 1 from RTYPES where vc LIKE ?";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

        comparePlans(sql, ignores);
    }

    public void testAbsExpr() throws Exception {
        String sql;
        sql = "select abs(i) from RTYPES";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

 //       testPlan(sql, PlannerType.VOLTDB);
        comparePlans(sql, ignores);
    }

    public void testDecodeExpr() throws Exception {
        String sql;
        sql = "select DECODE(i, -1, 0, i) from RTYPES";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");

 //       testPlan(sql, PlannerType.VOLTDB);
        comparePlans(sql, ignores);
    }

}
