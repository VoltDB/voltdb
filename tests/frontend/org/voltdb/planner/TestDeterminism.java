/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

    /**
     * This is the older interface to the determinism test function. It just
     * asserts that the sql argument passes the tests and expects the results to
     * be inherently deterministic. See
     * {@link assertPlanDeterminismFullCore(String sql, boolean order, boolean
     * orderLimitContent, boolean inherentContent, DeterminismMode detMode)}.
     *
     * @param sql
     *            SQL text to test.
     * @param order
     *            Do we expect the sql to be order deterministic.
     * @param orderLimitContent
     *            to we expect the sql to be order-limit determistic.
     * @param detMode
     *            FASTER or SAFER.
     */
    private void assertPlanDeterminismCore(String sql,
                                           boolean order,
                                           boolean orderLimitContent,
                                           DeterminismMode detMode) {
        assertPlanDeterminismFullCore(sql, order, orderLimitContent, true, detMode);
    }

    /**
     * This is the core test for determinism. We plan the sql and test that the
     * order determinism, orderLimit content determinism and inherent
     * determinism are as we expect.
     *
     * @param sql
     * @param order
     * @param orderLimitContent
     * @param inherentContent
     * @param detMode
     */
    private void assertPlanDeterminismFullCore(String sql,
                                               boolean order,
                                               boolean orderLimitContent,
                                               boolean inherentContent,
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
        if (orderLimitContent != (cp.isOrderDeterministic() || !cp.hasLimitOrOffset())) {
            System.out.println((orderLimitContent ? "EXPECTED CONSISTENT CONTENT: " : "UNEXPECTED CONSISTENT CONTENT: ")
                               + sql);
            // retry failed case for debugging
            if (m_staticRetryForDebugOnFailure) {
                // retry failed case for debugging
                cp = compileAdHocPlan(sql, detMode);
            }
        }
        assertEquals(orderLimitContent, cp.isOrderDeterministic() || !cp.hasLimitOrOffset());
        assertTrue(cp.isOrderDeterministic() || (null != cp.nondeterminismDetail()));
        if (inherentContent != cp.isContentDeterministic()) {
            System.out.println((inherentContent ? "EXPECTED CONSISTENT CONTENT: " : "UNEXPECTED CONSISTENT CONTENT: ")
                               + sql);
            // retry failed case for debugging
            if (m_staticRetryForDebugOnFailure) {
                // retry failed case for debugging
                cp = compileAdHocPlan(sql, detMode);
            }
        }
        assertEquals(cp.isContentDeterministic(), inherentContent);
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
                "select X.a, X.z, Y.z from tuniqcombo X, tunique Y",
                "order by X.a, X.c, X.b, Y.a");
        assertPlanDeterminism(
                "select X.a, X.z, Y.z from tuniqcombo X, tunique Y",
                "order by X.b, X.a, Y.a, X.c");
        assertPlanDeterminism(
                "select X.a, X.z, Y.z from tuniqcombo X, tunique Y",
                "order by X.z, X.a, X.c, X.b, Y.a");
        assertPlanDeterminism(
                "select X.a, X.z, Y.z, X.z + Y.z from tuniqcombo X, tunique Y",
                "order by 4, X.z, X.a, X.c, X.b, Y.a");
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
        // not yet supported by planner */ assertPlanDeterminismNeedsOrdering("select z, max(a) from ttree group by z",
        // not yet supported by planner */         "order by z, max(b)", "order by 1, 2");
        // not yet supported by planner */ assertPlanDeterminismNeedsOrdering("select    max(a) from ttree group by z",
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
        // "LIMIT" not currently supported for some DML.
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

    // MP section repeats tests on partitioned tables -- need to bypass subquerification for now.
    public void testMPDeterminismOfSelectStar() {
        assertMPPlanDeterminismNeedsOrdering("select * from ptree", "order by 1, 2, 3, 4");
        // if a table has a unique index... it can be used to scan in a r/w transaction
        // even without ordering when planned in "safe" determinism mode.
        assertMPPlanNeedsSaferDeterminismOrOrderCombo("select * from punique",    "order by a");
        assertMPPlanNeedsSaferDeterminismOrOrderCombo("select * from puniqcombo", "order by b, c, a");
        assertMPPlanNeedsSaferDeterminismOrOrderCombo("select * from ppk",        "order by a");

        // test sufficiency of minimal orderings
        assertMPPlanDeterminismNeedsOrdering("select * from ptree order by a",
                ", b, c, z", "order by a, b, c, z");
        assertMPPlanDeterminismNeedsOrdering("select * from ptree where a > 1 order by a",
                ", b, z, c", "order by a, b, z, c");
        assertMPPlanDeterminismNeedsOrdering("select * from ptree where a > 1 order by a, b",
                ", c, z", "order by a, b, c, z");
        assertMPPlanDeterminismNeedsOrdering("select * from punique where a > 1", "order by a");
        assertMPPlanDeterminismNeedsOrdering("select * from ppk where a > 1", "order by a");


        // test effects of sufficient but overly crowded or redundant order by
        assertMPPlanDeterminismNeedsOrdering("select * from punique", "order by z, a, c");
        assertMPPlanDeterminismNeedsOrdering("select * from ppk", "order by z, a");
        assertMPPlanDeterminismNeedsOrdering("select * from ptree where a > 1 order by a, a+z",
                ", z, c, b", "order by a, a+z, z, c, b");
        assertMPPlanDeterminismNeedsOrdering("select * from punique where a > 1", "order by a");
        assertMPPlanDeterminismNeedsOrdering("select * from ppk where a > 1", "order by a");
    }

    public void testMPDeterminismOfJoin() {
        assertMPPlanDeterminismNeedsOrdering(
                "select X.a, X.z, Y.z from puniqcombo X, punique Y where X.a = Y.a",
                "order by X.a, X.c, X.b, Y.a");
        assertMPPlanDeterminismNeedsOrdering(
                "select X.a, X.z, Y.z from puniqcombo X, punique Y where X.a = Y.a",
                "order by X.b, X.a, Y.a, X.c");
        assertMPPlanDeterminismNeedsOrdering(
                "select X.a, X.z, Y.z from puniqcombo X, punique Y where X.a = Y.a",
                "order by X.z, X.a, X.c, X.b, Y.a");
        assertMPPlanDeterminismNeedsOrdering(
                "select X.a, X.z, Y.z, X.z + Y.z from puniqcombo X, punique Y where X.a = Y.a",
                "order by 4, X.z, X.a, X.c, X.b, Y.a");
        assertMPPlanDeterminismNeedsOrdering(
                "select X.a l, X.z m, Y.z n from ptree X, punique Y where X.a = Y.a order by X.a, X.c, X.b, Y.a",
                ", X.z, Y.z", "order by l, m, n");
        assertMPPlanDeterminismNeedsOrdering(
                "select X.a l, X.z m, Y.z n from ptree X, punique Y where X.a = Y.a order by X.b, X.a, Y.a, X.c",
                ", X.z, Y.z", "order by l, m, n");
        assertMPPlanDeterminismNeedsOrdering(
                "select X.a l, X.z m, Y.z n from ptree X, punique Y where X.a = Y.a order by X.z, X.a, X.c, X.b, Y.a",
                ", X.z, Y.z", "order by l, m, n");
        assertMPPlanDeterminismNeedsOrdering(
                "select X.a, X.z, Y.z, X.z + Y.z from ptree X, punique Y where X.a = Y.a order by 4, X.z, X.a, X.c, X.b, Y.a",
                ", Y.z", "order by 1, 2, 3, 4");
        assertMPPlanNeedsSaferDeterminismCombo(
                "select X.a, X.z, Y.z from puniqcombo X, punique Y where X.a = Y.a");
        assertMPPlanNeedsSaferDeterminismCombo(
            "select X.a, X.z, Y.z from puniqcombo X, punique Y where X.a = Y.a order by X.a, X.b, Y.a");
        assertMPPlanNeedsSaferDeterminismCombo(
            "select X.a, X.z, Y.z from puniqcombo X, punique Y where X.a = Y.a order by X.a, X.c + X.b, X.b, Y.a");
    }

    public void testMPDeterminismOfSelectOrderGroupKeys() {
        assertMPPlanDeterminismNeedsOrdering("select z, max(a)    from ptree group by z   ",
                "order by z   ");
        assertMPPlanDeterminismNeedsOrdering("select    max(a)    from ptree group by z   ",
                "order by z   ", "order by 1");

        assertMPPlanDeterminismNeedsOrdering("select z, max(a), b from ptree group by z, b",
                "order by z, b");
        assertMPPlanDeterminismNeedsOrdering("select z, max(a)    from ptree group by z, b",
                "order by z, b", "order by 1, 2");
        assertMPPlanDeterminismNeedsOrdering("select    max(a)    from ptree group by z, b",
                "order by z, b", "order by 1");

        assertMPPlanDeterminismNeedsOrdering(
                "select z, max(a)      , b from ptree group by z, b order by z   ",
                ", b", "order by 1, 3");
        assertMPPlanDeterminismNeedsOrdering(
                "select z, max(a) max_a, b from ptree group by z, b order by z   ",
                ", b", "order by 1, 2");
        assertMPPlanDeterminismNeedsOrdering(
                "select    max(a)    from ptree group by z, b order by z   ",
                ", b", "order by 1");

        // Odd edge cases of needlessly long ORDER BY clause
        assertMPPlanDeterminismNeedsOrdering("select z, max(a), max(b) from ptree group by z",
                "order by z, max(b)", "order by z, 3");
        assertMPPlanDeterminismNeedsOrdering("select z, max(a), max(b) from ptree group by z",
                "order by z, 3     ", "order by z, 3");
        assertMPPlanDeterminismNeedsOrdering("select    max(a), max(b) from ptree group by z",
                "order by z, max(b)", "order by 1, 2");
        assertMPPlanDeterminismNeedsOrdering("select z, max(a), max(b) from ptree group by z order by max(b)",
                ", z", "order by 3, 1");
        assertMPPlanDeterminismNeedsOrdering("select z, max(a), max(b) from ptree group by z",
                "order by z, max(a)", "order by 1, 3");
        // not yet supported by planner */ assertMPPlanDeterminismNeedsOrdering("select z, max(a) from ptree group by z",
        // not yet supported by planner */         "order by z, max(b)", "order by 1, 2");
        // not yet supported by planner */ assertMPPlanDeterminismNeedsOrdering("select    max(a) from ptree group by z",
        // not yet supported by planner */         "order by z, max(b)", "order by 1");
    }

    public void testMPDeterminismOfSelectOrderAll() {
        assertMPPlanDeterminismNeedsOrdering("select z from ptree",       "order by z");
        assertMPPlanDeterminismNeedsOrdering("select a, b, z from ptree", "order by 1, 2, 3");
        assertMPPlanDeterminismNeedsOrdering("select a, b, z from ptree", "order by b, a, z");
        assertMPPlanDeterminismNeedsOrdering("select a, b, z from ptree", "order by 3, 2, 1");
    }

    public void testMPDeterminismOfJoinOrderAll() {
        assertMPPlanDeterminismNeedsOrdering("select X.a, X.z, Y.z from ptree X, punique Y where X.a = Y.a",
                "order by 1, 2, 3");
        assertMPPlanDeterminismNeedsOrdering("select X.a l, X.z m, Y.z n from ptree X, punique Y where X.a = Y.a",
                "order by Y.z, X.a, X.z", "order by l, m, n");
        assertMPPlanDeterminismNeedsOrdering("select X.a, X.z, Y.z from ptree X, punique Y where X.a = Y.a",
                "order by 3, 2, 1");
    }

    public void testMPDeterminismOfSelectIndexKeysOnly() {
        assertMPPlanDeterminismNeedsOrdering("select a, b from ptree", "order by a, b");
        assertMPPlanDeterminismNeedsOrdering("select a, b, c from ptree", "order by a, b, c");
        // non-prefix keys don't help
        assertMPPlanDeterminismNeedsOrdering("select b, c from ptree", "order by b, c");
        // if a table has a unique index... it can be used to scan in a r/w transaction
        assertMPPlanNeedsSaferDeterminismCombo("select a from punique");
        assertMPPlanNeedsSaferDeterminismCombo("select a from ppk");
        // hashes don't help, here
        assertMPPlanDeterminismNeedsOrdering("select a, b from phash", "order by a, b");
        assertMPPlanDeterminismNeedsOrdering("select a, b, c from phash", "order by a, b, c");
    }

    public void testMPDeterminismOfSelectOneKeyValue() {
        assertMPPlanDeterminismNeedsOrdering("select a, z from ptree where a = 1", "order by a, z");
        assertMPPlanDeterminismNeedsOrdering("select a, z from ptree where a = 1 and b < 10", "order by a, z");
        assertMPPlanDeterminism("select a, z from punique where a = 1", "order by z, a");
        assertMPPlanDeterminism("select a, z from punique where a = 1 and b < 10", "order by z, a");
        assertMPPlanDeterminism("select a, z from ppk where a = 1", "order by z, a");
        assertMPPlanDeterminism("select a, z from ppk where a = 1 and b < 10", "order by z, a");
    }

    public void testMPDeterminismImpliedByParameter() {
        assertPlanDeterminismCore("select * from ttree_with_key where b = ? order by a, c limit 1;",
                                  true,
                                  true,
                                  DeterminismMode.FASTER);
        assertPlanDeterminismCore("select d, e, id from ttree_with_key order by a, b, c limit 1;",
                                  true,
                                  true,
                                  DeterminismMode.FASTER);
        // We could have ? = 1, ttree_with_key = [(1, 1, 1, 2, 3), (1, -1, 1, 2, 3)].
        // Then the order evaluation has the single value [(1, 1)] and all rows are
        // selected by the where clause.  This means both rows are listed, but
        // we don't know in which order they will be listed.  The limit makes it
        // not content deterministic as well.
        assertPlanDeterminismCore("select * from ttree_with_key where abs(b) = ? order by a, c limit 1;",
                                  false,
                                  false,
                                  DeterminismMode.FASTER);
        // If ENG-8677 and follow on tickets are closed,
        // we can reenable these tests.
        final boolean ENG8677IsFixed = false;
        // Two tables.  Actually, this is a self join as well.  That
        // may be a reason to not allow it here.
        assertPlanDeterminismCore("select tleft.* from ttree_with_key as tleft join ttree_with_key as tright on tleft.id = tright.id where tleft.b = ? order by tleft.a, tleft.c limit 1;",
                                  ENG8677IsFixed,
                                  ENG8677IsFixed,
                                  DeterminismMode.FASTER);
        // Functions of order by expressions should be ok, whether they.
        // are deterministic or not.
        assertPlanDeterminismCore("select abs(a) + abs(b) from ttree_with_key order by a+1, b+1, c+2 limit 1;",
                                  ENG8677IsFixed,
                                  ENG8677IsFixed,
                                  DeterminismMode.FASTER);
        // Since these functions (x->x + 1 and x->x + 2) are one-to-one, these
        // functions are ok.
        assertPlanDeterminismCore("select * from ttree_with_key order by a+1, b+1, c+2 limit 1;",
                                  ENG8677IsFixed,
                                  ENG8677IsFixed,
                                  DeterminismMode.FASTER);
        // It seems like this should be deterministic.  There
        // are two copies of the table ttree_with_key.  The a, b and c
        // columns of both are constrained to be equal.  These form a
        // primary unique key.  If we have two rows:
        //      r1 = <la1, lb1, lc1, ld1, le1, lid1, ra1, rb1, rc1, rd1, re1, rid1>
        //      r2 = <la2, lb2, lc2, ld2, le2, lid2, ra2, rb2, rc2, rd2, re2, rid2>
        // we know from the join conditions:
        //      la1 = ra1, la2 = ra2
        //      lb1 = rb1, lb2 = rb2
        //      lc1 = rc1, lc2 = rc2
        // If the ordering of r1 and r2 is the same, then
        //      la1 = la2, rb1 = rb2, lc1 = lc2
        // So, all the columns are equal: xay = x'ay', xby = x'by' and xcy = x'cy'.
        // Since a, b, c is unique primary key,
        //      ld1 = ld2, le1 = le2, lid1 = lid2
        //      rd1 = rd2, re1 = re2, rid1 = rid2
        // Since the two tables are copies on of the other,
        //      ld1 = rd1, le1 = re1, lid1 = rid1
        //      ld2 = rd2, le2 = re2, lid2 = rid2
        // so everything is equal, and there is really only one column.  In particular,
        // the selected columns are equal.
        // Note: This argument depends on the Determinism Theorem.  See the attachment to
        //       ENG-8677.
        assertPlanDeterminismCore("select r.d, l.e, r.id from ttree_with_key as l join ttree_with_key as r on l.a = r.a and l.b = r.b and l.c = r.c order by l.a, r.b, l.c limit 1;",
                                  ENG8677IsFixed,
                                  ENG8677IsFixed,
                                  DeterminismMode.FASTER);
        // This is probably not possible. Let A be the matrix
        //          | 1  1  1 |
        //     A =  | 1  1 -1 |
        //          | 1 -1  1 |
        // Then det(A) = -3 != 0, and A is invertible.  If A*[a, b, c]' = A*[aa, bb, cc]', then
        // [a, b, c]' = [aa, bb, cc]'.  Since (a, b, c) is a primary key, this means
        // all the values are determined.
        assertPlanDeterminismCore("select * from ttree_with_key order by a + b + c, a + b - c, a - b + c limit 1;",
                                  ENG8677IsFixed,
                                  ENG8677IsFixed,
                                  DeterminismMode.FASTER);
    }

    public void testUnionDeterminism() throws Exception {
        final boolean ENG8790IsFixed = false;
        //
        // These two queries should be nearly identical. However, they are not.  The
        // problem, described in ENG-8790, is that in a union statement we only look
        // at the left-most select statement.
        //
        assertPlanDeterminismCore("(select a, b, c from ttree_with_key order by a, b, c limit 1) union (select a, b, c from ttree_with_key);", !ENG8790IsFixed, true, DeterminismMode.FASTER);
        assertPlanDeterminismCore("(select a, b, c from ttree_with_key) union (select a, b, c from ttree_with_key order by a, b, c limit 1);", ENG8790IsFixed, true, DeterminismMode.FASTER);
        assertPlanDeterminismCore("select a from tonecolumn order by abs(a)", false, true, DeterminismMode.FASTER);
    }

    public void testFloatingAggs() throws Exception {
        assertPlanDeterminismFullCore("select sum(alpha + beta + gamma) as fsum from floataggs order by fsum;",
                                      true,
                                      true,
                                      false,
                                      DeterminismMode.FASTER);
    }

    private void assertMPPlanDeterminismCore(String sql, boolean order, boolean content,
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

    // Check a number of variants of a core query for expected determinism effects.
    // The variants include the original query, the query with added sorting, the query with a row limit added,
    // the query nested within a trivial "select * " parent query, and various permutations of these changes.
    // The provided "order by" strings are expected to be sufficient to make the original query deterministic.
    private void assertMPPlanDeterminismCombo(String sql, boolean order, boolean content,
            String tryOrderBy, String tryPostSubOrderBy, DeterminismMode detMode)
    {
        assertMPPlanDeterminismCore(sql, order, content, detMode);
        if (tryOrderBy != null) {
            String orderedStmt = sql + " " + tryOrderBy;
            assertMPPlanDeterminismCore(orderedStmt, true, true, detMode);
            String limitedOrderStatement = orderedStmt + " LIMIT 2";
            assertMPPlanDeterminismCore(limitedOrderStatement, true, true, detMode);
        }

        String limitedStatement = sql + " LIMIT 2";
        assertMPPlanDeterminismCore(limitedStatement, order, order, detMode);
    }

    private void assertMPPlanNeedsSaferDeterminismCombo(String sql)
    {
        assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, DeterminismMode.FASTER);
        String limitedStatement = sql + " LIMIT 2";
        assertMPPlanDeterminismCore(limitedStatement, UNORDERED, INCONSISTENT, DeterminismMode.FASTER);
        // These MP cases ARE NOT deterministic in the safer mode.
        assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, DeterminismMode.SAFER);
        assertMPPlanDeterminismCore(limitedStatement, UNORDERED, INCONSISTENT, DeterminismMode.SAFER);
    }

    private void assertMPPlanNeedsSaferDeterminismOrOrderCombo(String sql, String tryOrderBy) {
        assertMPPlanNeedsSaferDeterminismCombo(sql);
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);
    }

    private void assertMPPlanDeterminismNeedsOrdering(String sql, String tryOrderBy)
    {
        assertMPPlanDeterminismCombo(sql, UNORDERED, CONSISTENT, tryOrderBy, tryOrderBy, DeterminismMode.FASTER);
    }

    private void assertMPPlanDeterminismNeedsOrdering(String sql, String tryOrderBy, String tryPostSubOrderBy)
    {
        assertMPPlanDeterminismCombo(sql, UNORDERED, CONSISTENT, tryOrderBy, tryPostSubOrderBy, DeterminismMode.FASTER);
    }

    /**
     * Checks determinism of statement against expected results, with or without factoring in order effects
     * @param sql
     */
    private void assertMPPlanDeterminism(String sql, String tryOrderBy)
    {
        assertMPPlanDeterminismCombo(sql, ORDERED, CONSISTENT, tryOrderBy, null, DeterminismMode.SAFER);
    }


}
