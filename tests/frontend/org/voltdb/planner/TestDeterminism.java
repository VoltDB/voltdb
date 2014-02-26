/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.planner;

import org.voltdb.compiler.DeterminismMode;

public class TestDeterminism extends PlannerTestCase {

    @Override
    protected void setUp() throws Exception {
        final boolean planForSinglePartitionFalse = false;
        setupSchema(TestDeterminism.class.getResource("testplans-determinism-ddl.sql"), "testdeterminism", planForSinglePartitionFalse);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private void assertPlanDeterminism(String sql, boolean order, boolean content,
                                       DeterminismMode detMode)
    {
        CompiledPlan cp = compileAdHocPlan(sql, detMode);
        if (order != cp.isOrderDeterministic()) {
            System.out.println((order ? "EXPECTED ORDER: " : "UNEXPECTED ORDER: ") + sql);
        }
        assertEquals(order, cp.isOrderDeterministic());
        assertEquals(content, cp.isOrderDeterministic() || ! cp.hasLimitOrOffset());
        assertTrue(cp.isOrderDeterministic() || (null != cp.nondeterminismDetail()));
    }

    private void assertPlanDeterminism(String sql, boolean order, boolean content,
                                       boolean alsoTryWithLimit, DeterminismMode detMode)
    {
        assertPlanDeterminism(sql, order, content, detMode);
        if (alsoTryWithLimit) {
            String limitedStatement = sql.replaceAll(";", " LIMIT 2;");
            assertPlanDeterminism(limitedStatement, order, order, detMode);
        }
    }

    /**
     * Checks determinism of statement against expected results, with or without factoring in order effects
     * @param sql
     * @param order expected statement determinism, including order effects
     */
    private void assertPlanDeterminism(String sql, boolean order)
    {
        assertPlanDeterminism(sql, order, CONSISTENT, true, DeterminismMode.SAFER);
    }

    private void assertPlanNeedsSaferDeterminism(String sql)
    {
        assertPlanDeterminism(sql, UNORDERED, CONSISTENT, DeterminismMode.FASTER);
        String limitedStatement = sql.replaceAll(";", " LIMIT 2;");
        assertPlanDeterminism(limitedStatement, UNORDERED, INCONSISTENT, DeterminismMode.FASTER);
        // These cases should be deterministic in the safer mode.
        assertPlanDeterminism(sql, ORDERED);
    }

    static final boolean UNORDERED = false;
    static final boolean ORDERED = true;
    static final boolean LATER_TO_BE_ORDERED = UNORDERED;
    static final boolean INCONSISTENT = false;
    static final boolean CONSISTENT = true;

    public void testDeterminismOfSelectStar() {
        assertPlanDeterminism("select * from ttree;", UNORDERED);
        // if a table has a unique index... it can be used to scan in a r/w transaction
        assertPlanNeedsSaferDeterminism("select * from tunique;");
        assertPlanNeedsSaferDeterminism("select * from tuniqcombo;");
        assertPlanNeedsSaferDeterminism("select * from tpk;");
    }

    public void testDeterminismOfJoin() {
        assertPlanDeterminism(
            "select X.a, X.z, Y.z from tuniqcombo X, tunique Y order by X.a, X.c, X.b, Y.a;",
            ORDERED);
        assertPlanDeterminism(
            "select X.a, X.z, Y.z from tuniqcombo X, tunique Y order by X.b, X.a, Y.a, X.c;",
            ORDERED);
        assertPlanDeterminism(
            "select X.a, X.z, Y.z from tuniqcombo X, tunique Y order by X.z, X.a, X.c, X.b, Y.a;",
            ORDERED);
        assertPlanDeterminism(
            "select X.a, X.z, Y.z, X.z + Y.z from tuniqcombo X, tunique Y order by 4, X.z, X.a, X.c, X.b, Y.a;",
            ORDERED);
    }

    public void testNonDeterminismOfJoin() {
        assertPlanDeterminism(
                "select X.a, X.z, Y.z from ttree X, tunique Y order by X.a, X.c, X.b, Y.a;",
                UNORDERED);
        assertPlanDeterminism(
                "select X.a, X.z, Y.z from ttree X, tunique Y order by X.b, X.a, Y.a, X.c;",
                UNORDERED);
        assertPlanDeterminism(
                "select X.a, X.z, Y.z from ttree X, tunique Y order by X.z, X.a, X.c, X.b, Y.a;",
                UNORDERED);
        assertPlanDeterminism(
                "select X.a, X.z, Y.z, X.z + Y.z from ttree X, tunique Y order by 4, X.z, X.a, X.c, X.b, Y.a;",
                UNORDERED);
        assertPlanNeedsSaferDeterminism(
                "select X.a, X.z, Y.z from tuniqcombo X, tunique Y where X.a = Y.a;");
        assertPlanNeedsSaferDeterminism(
            "select X.a, X.z, Y.z from tuniqcombo X, tunique Y order by X.a, X.b, Y.a;");
        assertPlanNeedsSaferDeterminism(
            "select X.a, X.z, Y.z from tuniqcombo X, tunique Y order by X.a, X.c + X.b, X.b, Y.a;");
    }

    public void testDeterminismOfSelectStarOrderOne() {
        assertPlanDeterminism("select * from ttree order by a;", UNORDERED); // insufficient ordering
        assertPlanDeterminism("select * from tunique order by a;", ORDERED);
        assertPlanDeterminism("select * from tpk order by a;", ORDERED);
        assertPlanDeterminism("select * from ttree where a > 1 order by a;", UNORDERED); // insufficient ordering
        assertPlanDeterminism("select * from tunique where a > 1 order by a;", ORDERED);
        assertPlanDeterminism("select * from tpk where a > 1 order by a;", ORDERED);
    }

    public void testDeterminismOfSelectStarOrderNoisy() {
        assertPlanDeterminism("select * from tunique order by z, a, c;", ORDERED);
        assertPlanDeterminism("select * from tpk order by z, a;", ORDERED);
        assertPlanDeterminism("select * from ttree where a > 1 order by a, a+z;", UNORDERED); // insufficient ordering
        assertPlanDeterminism("select * from tunique where a > 1 order by a;", ORDERED);
        assertPlanDeterminism("select * from tpk where a > 1 order by a;", ORDERED);
    }

    public void testDeterminismOfSelectOrderGroupKeys() {
        assertPlanDeterminism("select z, max(a)    from ttree group by z    order by z   ;", ORDERED);
        assertPlanDeterminism("select    max(a)    from ttree group by z    order by z   ;", ORDERED);

        assertPlanDeterminism("select z, max(a), b from ttree group by z, b order by z, b;", ORDERED);
        assertPlanDeterminism("select z, max(a)    from ttree group by z, b order by z, b;", ORDERED);
        assertPlanDeterminism("select    max(a)    from ttree group by z, b order by z, b;", ORDERED);

        assertPlanDeterminism("select z, max(a), b from ttree group by z, b order by z   ;", UNORDERED);
        assertPlanDeterminism("select z, max(a)    from ttree group by z, b order by z   ;", UNORDERED);
        assertPlanDeterminism("select    max(a)    from ttree group by z, b order by z   ;", UNORDERED);

        // Odd edge cases of needlessly long ORDER BY clause
        assertPlanDeterminism("select z, max(a), max(b) from ttree group by z order by z, max(b);", ORDERED);
        assertPlanDeterminism("select z, max(a), max(b) from ttree group by z order by z, 3;", ORDERED);
        assertPlanDeterminism("select    max(a), max(b) from ttree group by z order by z, max(b);", ORDERED);
        // not yet supported by planner */ assertPlanDeterminism("select z, max(a)         from ttree group by z order by z, max(b);", ORDERED);
        // not yet supported by planner */ assertPlanDeterminism("select    max(a)         from ttree group by z order by z, max(b);", ORDERED);

        assertPlanDeterminism("select z, max(a), max(b) from ttree group by z order by max(b), z;", ORDERED);
        // not yet supported by planner */ assertPlanDeterminism("select    max(a)         from ttree group by z order by max(b), z;", ORDERED);
        assertPlanDeterminism("select z, max(a), max(b) from ttree group by z order by max(b), z;", ORDERED);
        // not yet supported by planner */ assertPlanDeterminism("select    max(a)         from ttree group by z order by max(b), z;", ORDERED);
    }

    public void testDeterminismOfSelectOrderAll() {
        assertPlanDeterminism("select z from ttree order by z;", ORDERED);
        assertPlanDeterminism("select a, b, z from ttree order by 1, 2, 3;", ORDERED);
        assertPlanDeterminism("select a, b, z from ttree order by b, a, z;", ORDERED);
        assertPlanDeterminism("select a, b, z from ttree order by 3, 2, 1;", ORDERED);
    }

    public void testDeterminismOfJoinOrderAll() {
        assertPlanDeterminism("select X.a, X.z, Y.z from ttree X, tunique Y where X.a = Y.a order by 1, 2, 3;", ORDERED);
        assertPlanDeterminism("select X.a, X.z, Y.z from ttree X, tunique Y where X.a = Y.a order by Y.z, X.a, X.z;", ORDERED);
        assertPlanDeterminism("select X.a, X.z, Y.z from ttree X, tunique Y where X.a = Y.a order by 3, 2, 1;", ORDERED);
    }

    public void testDeterminismOfSelectIndexKeysOnly() {
        assertPlanDeterminism("select a, b from ttree;", LATER_TO_BE_ORDERED);
        assertPlanDeterminism("select a, b, c from ttree;", LATER_TO_BE_ORDERED);
        // non-prefix keys don't help
        assertPlanDeterminism("select b, c from ttree;", UNORDERED);
        // if a table has a unique index... it can be used to scan in a r/w transaction
        assertPlanNeedsSaferDeterminism("select a from tunique;");
        assertPlanNeedsSaferDeterminism("select a from tpk;");
        // hashes don't help, here
        assertPlanDeterminism("select a, b from thash;", UNORDERED);
        assertPlanDeterminism("select a, b, c from thash;", UNORDERED);
    }

    public void testDeterminismOfSelectOneKeyValue() {
        assertPlanDeterminism("select a, z from ttree where a = 1;", UNORDERED);
        assertPlanDeterminism("select a, z from ttree where a = 1 and b < 10;", UNORDERED);
        assertPlanDeterminism("select a, z from tunique where a = 1;", ORDERED);
        assertPlanDeterminism("select a, z from tunique where a = 1 and b < 10;", ORDERED);
        assertPlanDeterminism("select a, z from tpk where a = 1;", ORDERED);
        assertPlanDeterminism("select a, z from tpk where a = 1 and b < 10;", ORDERED);
    }

    private void assertDMLPlanDeterminism(String sql)
    {
        assertPlanDeterminism(sql, ORDERED, CONSISTENT, DeterminismMode.SAFER);
    }

    public void testDeterminismOfWrites() {
        // "LIMIT" not currently supported for DML.
        assertDMLPlanDeterminism("insert into ttree values (1,2,3,4);");
        assertDMLPlanDeterminism("insert into tunique values (1,2,3,4);");
        assertDMLPlanDeterminism("insert into tpk values (1,2,3,4);");
        assertDMLPlanDeterminism("delete from ttree;");
        assertDMLPlanDeterminism("delete from tunique;");
        assertDMLPlanDeterminism("delete from tpk;");
        assertDMLPlanDeterminism("update ttree set z = 5 where a < 2;");
        assertDMLPlanDeterminism("update tunique set z = 5 where a < 2;");
        assertDMLPlanDeterminism("update tpk set z = 5 where a < 2;");
    }

    public void testOrderByWithoutIndex() {
        assertPlanDeterminism("SELECT * FROM eng4155 ORDER BY ts DESC, id;", ORDERED);
        assertPlanDeterminism("SELECT * FROM eng4155 ORDER BY ts DESC;", UNORDERED);
        assertPlanDeterminism("SELECT ts FROM eng4155 ORDER BY ts DESC;", ORDERED);
    }
}
