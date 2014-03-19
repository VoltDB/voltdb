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

    private final static boolean m_staticRetryForDebugOnFailure = /**/ false; //*/ = true;//to debug

    private void assertPlanDeterminismCore(String sql, boolean order, boolean content,
            DeterminismMode detMode)
    {
        CompiledPlan cp = compileAdHocPlan(sql, detMode);
        if (order != cp.isOrderDeterministic()) {
            System.out.println((order ? "EXPECTED ORDER: " : "UNEXPECTED ORDER: ") + sql);
            if (m_staticRetryForDebugOnFailure) {
                // retry failed case for debugging
                cp = compileAdHocPlan(sql, detMode);
            }
        }
        assertEquals(order, cp.isOrderDeterministic());
        if (content != (cp.isOrderDeterministic() || ! cp.hasLimitOrOffset())) {
            System.out.println((content ? "EXPECTED CONSISTENT CONTENT: " : "UNEXPECTED CONSISTENT CONTENT: ") + sql);
            // retry failed case for debugging
            if (m_staticRetryForDebugOnFailure) {
                // retry failed case for debugging
                cp = compileAdHocPlan(sql, detMode);
            }
        }
        assertEquals(content, cp.isOrderDeterministic() || ! cp.hasLimitOrOffset());
        assertTrue(cp.isOrderDeterministic() || (null != cp.nondeterminismDetail()));
    }

    // This is a weakened version of assertPlanDeterminismCore that only complains to the system output
    // instead of failing asserts.
    private void ascertainPlanDeterminismCore(String sql, boolean order, boolean content,
            DeterminismMode detMode)
    {
        CompiledPlan cp = compileAdHocPlan(sql, detMode);
        if (order != cp.isOrderDeterministic()) {
            System.out.println("WEAKENED ASSERT WOULD HAVE FAILED ON " +
                    (order ? "EXPECTED ORDER: " : "UNEXPECTED ORDER: ") + sql);
            if (m_staticRetryForDebugOnFailure) {
                // retry failed case for debugging
                cp = compileAdHocPlan(sql, detMode);
            }
        }
        if (content != (cp.isOrderDeterministic() || ! cp.hasLimitOrOffset())) {
            System.out.println("WEAKENED ASSERT WOULD HAVE FAILED ON " +
                    (content ? "EXPECTED CONSISTENT CONTENT: " : "UNEXPECTED CONSISTENT CONTENT: ") + sql);
            if (m_staticRetryForDebugOnFailure) {
                // retry failed case for debugging
                cp = compileAdHocPlan(sql, detMode);
            }
        }
    }

    // Check a number of variants of a core query for expected determinism effects.
    // The variants include the original query, the query with added sorting, the query with a row limit added,
    // the query nested within a trivial "select * " parent query, and various permutations of these changes.
    // The provided "order by" strings are expected to be sufficient to make the original query deterministic.
    private void assertPlanDeterminismCombo(String sql, boolean order, boolean content,
            String tryOrderBy, String tryPostSubOrderBy, DeterminismMode detMode)
    {
        assertPlanDeterminismCore(sql, order, content, detMode);
        assertPlanDeterminismCore("select * from (" + sql + ") sqy", order, content, detMode);
        if (tryOrderBy != null) {
            String orderedStmt = sql + " " + tryOrderBy;
            assertPlanDeterminismCore(orderedStmt, true, true, detMode);
            assertPlanDeterminismCore("select * from (" + orderedStmt + ") sqy", true, true, detMode);

            String limitedOrderStatement = orderedStmt + " LIMIT 2";
            assertPlanDeterminismCore(limitedOrderStatement, true, true, detMode);
            assertPlanDeterminismCore("select * from (" + limitedOrderStatement + ") sqy", true, true, detMode);
            assertPlanDeterminismCore("select * from (" + orderedStmt + ") sqy LIMIT 2", true, true, detMode);

            if (tryPostSubOrderBy != null) {
                String postOrderedParent = "select * from (" + sql + ") sqy " + tryPostSubOrderBy;
                ascertainPlanDeterminismCore(postOrderedParent, true, true, detMode);

                String limitedPostOrderStmt =
                        "select * from (" + orderedStmt + " LIMIT 2) sqy " + tryPostSubOrderBy;
                assertPlanDeterminismCore(limitedPostOrderStmt, true, true, detMode);
                String postLimitedPostOrdered = postOrderedParent + " LIMIT 2";
                ascertainPlanDeterminismCore(postLimitedPostOrdered, true, true, detMode);
            }
        }

        String limitedStatement = sql + " LIMIT 2";
        assertPlanDeterminismCore(limitedStatement, order, order, detMode);
        assertPlanDeterminismCore("select * from (" + limitedStatement + ") sqy", order, order, detMode);
    }

    private void assertPlanNeedsSaferDeterminismCombo(String sql)
    {
        assertPlanDeterminismCore(sql, UNORDERED, CONSISTENT, DeterminismMode.FASTER);
        String limitedStatement = sql + " LIMIT 2";
        assertPlanDeterminismCore(limitedStatement, UNORDERED, INCONSISTENT, DeterminismMode.FASTER);
        // These cases should be deterministic in the safer mode.
        assertPlanDeterminism(sql, null);
    }

    private void assertPlanNeedsSaferDeterminismOrOrderCombo(String sql, String tryOrderBy) {
        assertPlanNeedsSaferDeterminismCombo(sql);
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy);
    }

    private void assertPlanDeterminismNeedsOrdering(String sql, String tryOrderBy)
    {
        assertPlanDeterminismCombo(sql, UNORDERED, CONSISTENT, tryOrderBy, tryOrderBy, DeterminismMode.FASTER);
    }

    private void assertPlanDeterminismNeedsOrdering(String sql, String tryOrderBy, String tryPostSubOrderBy)
    {
        assertPlanDeterminismCombo(sql, UNORDERED, CONSISTENT, tryOrderBy, tryPostSubOrderBy, DeterminismMode.FASTER);
    }

    /**
     * Checks non-determinism of statements that are not salvageable by order by.
     * @param sql
     */
    private void assertPlanDeterminismWrongOrder(String sql)
    {
        assertPlanDeterminismCombo(sql, UNORDERED, CONSISTENT,
                /*tryOrderBy*/null, /*tryPostSubOrderBy*/null, DeterminismMode.SAFER);
    }

    /**
     * Checks determinism of statement against expected results, with or without factoring in order effects
     * @param sql
     */
    private void assertPlanDeterminism(String sql, String tryOrderBy)
    {
        assertPlanDeterminismCombo(sql, ORDERED, CONSISTENT, tryOrderBy, null, DeterminismMode.SAFER);
    }

    static final boolean UNORDERED = false;
    static final boolean ORDERED = true;
    static final boolean LATER_TO_BE_ORDERED = UNORDERED;
    static final boolean INCONSISTENT = false;
    static final boolean CONSISTENT = true;

    public void testDeterminismOfSelectStar() {
        assertPlanDeterminismNeedsOrdering("select * from ttree", "order by 1, 2, 3, 4");
        // if a table has a unique index... it can be used to scan in a r/w transaction
        // even without ordering when planned in "safe" determinism mode.
        assertPlanNeedsSaferDeterminismOrOrderCombo("select * from tunique",    "order by a");
        assertPlanNeedsSaferDeterminismOrOrderCombo("select * from tuniqcombo", "order by b, c, a");
        assertPlanNeedsSaferDeterminismOrOrderCombo("select * from tpk",        "order by a");

        // test sufficiency of minimal orderings
        assertPlanDeterminismNeedsOrdering("select * from ttree order by a",
                ", b, c, z", "order by a, b, c, z");
        assertPlanDeterminismNeedsOrdering("select * from ttree where a > 1 order by a",
                ", b, z, c", "order by a, b, z, c");
        assertPlanDeterminismNeedsOrdering("select * from ttree where a > 1 order by a, b",
                ", c, z", "order by a, b, c, z");
        assertPlanDeterminism("select * from tunique where a > 1", "order by a");
        assertPlanDeterminism("select * from tpk where a > 1", "order by a");


        // test effects of sufficient but overly crowded or redundant order by
        assertPlanDeterminism("select * from tunique", "order by z, a, c");
        assertPlanDeterminism("select * from tpk", "order by z, a");
        assertPlanDeterminismNeedsOrdering("select * from ttree where a > 1 order by a, a+z",
                ", z, c, b", "order by a, a+z, z, c, b");
        assertPlanDeterminism("select * from tunique where a > 1", "order by a");
        assertPlanDeterminism("select * from tpk where a > 1", "order by a");
    }

    public void testDeterminismOfJoin() {
        assertPlanDeterminism(
            "select X.a, X.z, Y.z from tuniqcombo X, tunique Y", "order by X.a, X.c, X.b, Y.a");
        assertPlanDeterminism(
            "select X.a, X.z, Y.z from tuniqcombo X, tunique Y", "order by X.b, X.a, Y.a, X.c");
        assertPlanDeterminism(
            "select X.a, X.z, Y.z from tuniqcombo X, tunique Y", "order by X.z, X.a, X.c, X.b, Y.a");
        assertPlanDeterminism(
            "select X.a, X.z, Y.z, X.z + Y.z from tuniqcombo X, tunique Y", "order by 4, X.z, X.a, X.c, X.b, Y.a");
    }

    public void testNonDeterminismOfJoin() {
        assertPlanDeterminismNeedsOrdering(
                "select X.a l, X.z m, Y.z n from ttree X, tunique Y order by X.a, X.c, X.b, Y.a",
                ", X.z, Y.z", "order by l, m, n");
        assertPlanDeterminismNeedsOrdering(
                "select X.a l, X.z m, Y.z n from ttree X, tunique Y order by X.b, X.a, Y.a, X.c",
                ", X.z, Y.z", "order by l, m, n");
        assertPlanDeterminismNeedsOrdering(
                "select X.a l, X.z m, Y.z n from ttree X, tunique Y order by X.z, X.a, X.c, X.b, Y.a",
                ", X.z, Y.z", "order by l, m, n");
        assertPlanDeterminismNeedsOrdering(
                "select X.a, X.z, Y.z, X.z + Y.z from ttree X, tunique Y order by 4, X.z, X.a, X.c, X.b, Y.a",
                ", Y.z", "order by 1, 2, 3, 4");
        assertPlanNeedsSaferDeterminismCombo(
                "select X.a, X.z, Y.z from tuniqcombo X, tunique Y where X.a = Y.a");
        assertPlanNeedsSaferDeterminismCombo(
            "select X.a, X.z, Y.z from tuniqcombo X, tunique Y order by X.a, X.b, Y.a");
        assertPlanNeedsSaferDeterminismCombo(
            "select X.a, X.z, Y.z from tuniqcombo X, tunique Y order by X.a, X.c + X.b, X.b, Y.a");
    }

    public void testDeterminismOfSelectOrderGroupKeys() {
        assertPlanDeterminismNeedsOrdering("select z, max(a)    from ttree group by z   ",
                "order by z   ");
        assertPlanDeterminismNeedsOrdering("select    max(a)    from ttree group by z   ",
                "order by z   ", "order by 1");

        assertPlanDeterminismNeedsOrdering("select z, max(a), b from ttree group by z, b",
                "order by z, b");
        assertPlanDeterminismNeedsOrdering("select z, max(a)    from ttree group by z, b",
                "order by z, b", "order by 1, 2");
        assertPlanDeterminismNeedsOrdering("select    max(a)    from ttree group by z, b",
                "order by z, b", "order by 1");

        assertPlanDeterminismNeedsOrdering(
                "select z, max(a)      , b from ttree group by z, b order by z   ",
                ", b", "order by 1, 3");
        assertPlanDeterminismNeedsOrdering(
                "select z, max(a) max_a, b from ttree group by z, b order by z   ",
                ", b", "order by 1, 2");
        assertPlanDeterminismNeedsOrdering(
                "select    max(a)    from ttree group by z, b order by z   ",
                ", b", "order by 1");




        // Odd edge cases of needlessly long ORDER BY clause
        assertPlanDeterminismNeedsOrdering("select z, max(a), max(b) from ttree group by z",
                "order by z, max(b)", "order by z, 3");
        assertPlanDeterminismNeedsOrdering("select z, max(a), max(b) from ttree group by z",
                "order by z, 3     ", "order by z, 3");
        assertPlanDeterminismNeedsOrdering("select    max(a), max(b) from ttree group by z",
                "order by z, max(b)", "order by 1, 2");
        assertPlanDeterminismNeedsOrdering("select z, max(a), max(b) from ttree group by z order by max(b)",
                ", z", "order by 3, 1");
        assertPlanDeterminismNeedsOrdering("select z, max(a), max(b) from ttree group by z",
                "order by z, max(a)", "order by 1, 3");
        // not yet supported by planner */ assertPlanDeterminismNeedsOrdering("select z, max(a)         from ttree group by z",
        // not yet supported by planner */         "order by z, max(b)", "order by 1, 2");
        // not yet supported by planner */ assertPlanDeterminismNeedsOrdering("select    max(a)         from ttree group by z",
        // not yet supported by planner */         "order by z, max(b)", "order by 1");
    }

    public void testDeterminismOfSelectOrderAll() {
        assertPlanDeterminismNeedsOrdering("select z from ttree",       "order by z");
        assertPlanDeterminismNeedsOrdering("select a, b, z from ttree", "order by 1, 2, 3");
        assertPlanDeterminismNeedsOrdering("select a, b, z from ttree", "order by b, a, z");
        assertPlanDeterminismNeedsOrdering("select a, b, z from ttree", "order by 3, 2, 1");
    }

    public void testDeterminismOfJoinOrderAll() {
        assertPlanDeterminismNeedsOrdering("select X.a, X.z, Y.z from ttree X, tunique Y where X.a = Y.a",
                "order by 1, 2, 3");
        assertPlanDeterminismNeedsOrdering("select X.a l, X.z m, Y.z n from ttree X, tunique Y where X.a = Y.a",
                "order by Y.z, X.a, X.z", "order by l, m, n");
        assertPlanDeterminismNeedsOrdering("select X.a, X.z, Y.z from ttree X, tunique Y where X.a = Y.a",
                "order by 3, 2, 1");
    }

    public void testDeterminismOfSelectIndexKeysOnly() {
        assertPlanDeterminismNeedsOrdering("select a, b from ttree", "order by a, b");
        assertPlanDeterminismNeedsOrdering("select a, b, c from ttree", "order by a, b, c");
        // non-prefix keys don't help
        assertPlanDeterminismNeedsOrdering("select b, c from ttree", "order by b, c");
        // if a table has a unique index... it can be used to scan in a r/w transaction
        assertPlanNeedsSaferDeterminismCombo("select a from tunique");
        assertPlanNeedsSaferDeterminismCombo("select a from tpk");
        // hashes don't help, here
        assertPlanDeterminismNeedsOrdering("select a, b from thash", "order by a, b");
        assertPlanDeterminismNeedsOrdering("select a, b, c from thash", "order by a, b, c");
    }

    public void testDeterminismOfSelectOneKeyValue() {
        assertPlanDeterminismNeedsOrdering("select a, z from ttree where a = 1", "order by a, z");
        assertPlanDeterminismNeedsOrdering("select a, z from ttree where a = 1 and b < 10", "order by a, z");
        assertPlanDeterminism("select a, z from tunique where a = 1", "order by z, a");
        assertPlanDeterminism("select a, z from tunique where a = 1 and b < 10", "order by z, a");
        assertPlanDeterminism("select a, z from tpk where a = 1", "order by z, a");
        assertPlanDeterminism("select a, z from tpk where a = 1 and b < 10", "order by z, a");
    }

    private void assertDMLPlanDeterminism(String sql)
    {
        assertPlanDeterminismCore(sql, ORDERED, CONSISTENT, DeterminismMode.SAFER);
    }

    public void testDeterminismOfWrites() {
        // "LIMIT" not currently supported for DML.
        assertDMLPlanDeterminism("insert into ttree values (1,2,3,4)");
        assertDMLPlanDeterminism("insert into tunique values (1,2,3,4)");
        assertDMLPlanDeterminism("insert into tpk values (1,2,3,4)");
        assertDMLPlanDeterminism("delete from ttree");
        assertDMLPlanDeterminism("delete from tunique");
        assertDMLPlanDeterminism("delete from tpk");
        assertDMLPlanDeterminism("update ttree set z = 5 where a < 2");
        assertDMLPlanDeterminism("update tunique set z = 5 where a < 2");
        assertDMLPlanDeterminism("update tpk set z = 5 where a < 2");
    }

    public void testOrderByWithoutIndex() {
        assertPlanDeterminism("SELECT * FROM eng4155", "order by ts DESC, id");
        assertPlanDeterminismNeedsOrdering("SELECT * FROM eng4155 ORDER BY ts DESC",
                ", id", "order by ts DESC, id");
        assertPlanDeterminism("SELECT ts FROM eng4155", "order by ts DESC");
    }
}
