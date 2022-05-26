/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLDecoder;

import org.hsqldb_voltpatches.HSQLInterface;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;

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
        assertPlanDeterminismFullCore(sql, order, orderLimitContent,
                true, detMode);
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
                                               DeterminismMode detMode) {
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

    // This is a weakened version of assertPlanDeterminismCore that only
    // complains to the system output instead of failing asserts.
    private void ascertainPlanDeterminismCore(String sql,
            boolean order, boolean content, DeterminismMode detMode) {
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

    // Check variants of a core query for expected determinism effects.
    // The variants include the original query, the query with added sorting,
    // the query with a row limit added, the query nested within a trivial
    // "select * " parent query, and various permutations of these changes.
    // The provided "order by" strings are expected to be sufficient to make
    // the original query deterministic.
    private void assertPlanDeterminismCombo(String sql, boolean order,
            boolean content, boolean trySubqueryOrderBy, String tryOrderBy,
            String tryPostSubOrderBy, DeterminismMode detMode) {
        assertPlanDeterminismCore(sql, order, content, detMode);
        String wrappedSql;
        wrappedSql = "select * from (" + sql + ") sqy";
        assertPlanDeterminismCore(wrappedSql, order, content, detMode);
        if (tryOrderBy != null) {
            String orderedStmt = sql + " " + tryOrderBy;
            assertPlanDeterminismCore(orderedStmt, true, true, detMode);
            if (trySubqueryOrderBy) {
                wrappedSql = "select * from (" + orderedStmt + ") sqy";
                assertPlanDeterminismCore(wrappedSql, true, true, detMode);
            }

            String limitedOrderStatement = orderedStmt + " LIMIT 2";
            assertPlanDeterminismCore(limitedOrderStatement, true, true, detMode);
            wrappedSql = "select * from (" + limitedOrderStatement + ") sqy";
            assertPlanDeterminismCore(wrappedSql, true, true, detMode);
            if (trySubqueryOrderBy) {
                wrappedSql = "select * from (" + orderedStmt + ") sqy LIMIT 2";
                assertPlanDeterminismCore(wrappedSql, true, true, detMode);
            }

            if (tryPostSubOrderBy != null) {
                String postOrderedParent =
                        "select * from (" + sql + ") sqy " + tryPostSubOrderBy;
                ascertainPlanDeterminismCore(postOrderedParent, true, true, detMode);

                String limitedPostOrderStmt =
                        "select * from (" + orderedStmt + " LIMIT 2) sqy " +
                        tryPostSubOrderBy;
                assertPlanDeterminismCore(limitedPostOrderStmt, true, true, detMode);
                String postLimitedPostOrdered = postOrderedParent + " LIMIT 2";
                ascertainPlanDeterminismCore(postLimitedPostOrdered, true, true, detMode);
            }
        }

        String limitedStatement = sql + " LIMIT 2";
        assertPlanDeterminismCore(limitedStatement, order, order, detMode);
        wrappedSql = "select * from (" + limitedStatement + ") sqy";
        assertPlanDeterminismCore(wrappedSql, order, order, detMode);
    }

    private void assertPlanNeedsSaferDeterminismCombo(String sql) {
        assertPlanDeterminismCore(sql, UNORDERED, CONSISTENT, DeterminismMode.FASTER);
        String limitedStatement = sql + " LIMIT 2";
        assertPlanDeterminismCore(limitedStatement, UNORDERED, INCONSISTENT,
                DeterminismMode.FASTER);
        // These cases should be deterministic in the safer mode.
        assertPlanDeterminism(sql, null);
    }

    private void assertPlanNeedsSaferDeterminismOrOrderCombo(String sql,
            String tryOrderBy, boolean trySubqueryWithOrderBy) {
        assertPlanNeedsSaferDeterminismCombo(sql);
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, trySubqueryWithOrderBy);
    }

    private void assertPlanDeterminismNeedsOrdering(String sql, String tryOrderBy)
    {
        assertPlanDeterminismCombo(sql, UNORDERED, CONSISTENT, tryOrderBy, tryOrderBy, DeterminismMode.FASTER);
    }

    private void assertPlanDeterminismNeedsOrdering(String sql,
            String tryOrderBy, boolean trySubqueryWithOrderBy) {
        assertPlanDeterminismCombo(sql, UNORDERED, CONSISTENT, trySubqueryWithOrderBy, tryOrderBy, tryOrderBy, DeterminismMode.FASTER);
    }

    private void assertPlanDeterminismNeedsOrdering(String sql, String tryOrderBy,
            String tryPostSubOrderBy, boolean trySubqueryWithOrderBy) {
        assertPlanDeterminismCombo(sql, UNORDERED, CONSISTENT, trySubqueryWithOrderBy, tryOrderBy, tryPostSubOrderBy, DeterminismMode.FASTER);
    }

    private void assertPlanDeterminismNeedsOrdering(String sql, String tryOrderBy,
            String tryPostSubOrderBy) {
        assertPlanDeterminismCombo(sql, UNORDERED, CONSISTENT, true, tryOrderBy, tryPostSubOrderBy, DeterminismMode.FASTER);
    }

    private void assertPlanDeterminismCombo(String sql, boolean order, boolean content,
            String tryOrderBy, String tryPostSubOrderBy, DeterminismMode detMode) {
        assertPlanDeterminismCombo(sql, order, content, true, tryOrderBy, tryPostSubOrderBy, detMode);
    }

    /**
     * Checks determinism of statement against expected results, with or without factoring in order effects
     * @param sql
     */
    private void assertPlanDeterminism(String sql, String tryOrderBy) {
        assertPlanDeterminismCombo(sql, ORDERED, CONSISTENT, tryOrderBy, null, DeterminismMode.SAFER);
    }

    private static final boolean UNORDERED = false;
    private static final boolean ORDERED = true;
    private static final boolean INCONSISTENT = false;
    private static final boolean CONSISTENT = true;

    public void testDeterminismOfSelectStar() {
        String sql;
        String tryOrderBy;
        String tryPostSubOrderBy;

        sql = "select * from ttree";
        tryOrderBy = "order by 1, 2, 3, 4";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, false);

        // if a table has a unique index...
        // it can be used to scan in a r/w transaction
        // even without ordering when planned in "safe" determinism mode.
        sql = "select * from tunique";
        tryOrderBy = "order by a";
        assertPlanNeedsSaferDeterminismOrOrderCombo(sql, tryOrderBy, false);

        sql = "select * from tuniqcombo";
        tryOrderBy = "order by b, c, a";
        assertPlanNeedsSaferDeterminismOrOrderCombo(sql, tryOrderBy, false);

        sql = "select * from tpk";
        tryOrderBy = "order by a";
        assertPlanNeedsSaferDeterminismOrOrderCombo(sql, tryOrderBy, false);

        // test sufficiency of minimal orderings
        sql = "select * from ttree order by a";
        tryOrderBy = ", b, c, z";
        tryPostSubOrderBy = "order by a, b, c, z";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy, false);

        sql = "select * from ttree where a > 1 order by a";
        tryOrderBy = ", b, z, c";
        tryPostSubOrderBy = "order by a, b, z, c";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy, false);

        sql = "select * from ttree where a > 1 order by a, b";
        tryOrderBy = ", c, z";
        tryPostSubOrderBy = "order by a, b, c, z";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy, false);

        sql = "select * from tunique where a > 1";
        tryOrderBy = "order by a";
        assertPlanDeterminism(sql, tryOrderBy);

        sql = "select * from tpk where a > 1";
        tryOrderBy = "order by a";
        assertPlanDeterminism(sql, tryOrderBy);

        // test effects of sufficient but overly crowded or redundant order by
        sql = "select * from tunique";
        tryOrderBy = "order by z, a, c";
        assertPlanDeterminism(sql, tryOrderBy);

        sql = "select * from tpk";
        tryOrderBy = "order by z, a";
        assertPlanDeterminism(sql, tryOrderBy);

        sql = "select * from ttree where a > 1 order by a, a+z";
        tryOrderBy = ", z, c, b";
        tryPostSubOrderBy = "order by a, a+z, z, c, b";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy, false);

        sql = "select * from tunique where a > 1";
        tryOrderBy = "order by a";
        assertPlanDeterminism(sql, tryOrderBy);

        sql = "select * from tpk where a > 1";
        tryOrderBy = "order by a";
        assertPlanDeterminism(sql, tryOrderBy);
    }

    public void testDeterminismOfJoin() {
        String sql;
        String tryOrderBy;
        String tryPostSubOrderBy;

        sql = "select X.a, X.z as xz, Y.z as yz from tuniqcombo X, tunique Y";
        tryOrderBy = "order by X.a, X.c, X.b, Y.a";
        assertPlanDeterminism(sql, tryOrderBy);

        sql = "select X.a, X.z as xz, Y.z as yz from tuniqcombo X, tunique Y";
        tryOrderBy = "order by X.b, X.a, Y.a, X.c";
        assertPlanDeterminism(sql, tryOrderBy);

        sql = "select X.a, X.z as xz, Y.z as yz from tuniqcombo X, tunique Y";
        tryOrderBy = "order by X.z, X.a, X.c, X.b, Y.a";
        assertPlanDeterminism(sql, tryOrderBy);

        sql = "select X.a, X.z as xz, Y.z as yz, X.z + Y.z from tuniqcombo X, tunique Y";
        tryOrderBy = "order by 4, X.z, X.a, X.c, X.b, Y.a";
        assertPlanDeterminism(sql, tryOrderBy);

        sql = "select X.a l, X.z m, Y.z n from ttree X, tunique Y order by X.a, X.c, X.b, Y.a";
        tryOrderBy = ", X.z, Y.z";
        tryPostSubOrderBy = "order by l, m, n";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select X.a l, X.z m, Y.z n from ttree X, tunique Y order by X.b, X.a, Y.a, X.c";
        tryOrderBy = ", X.z, Y.z";
        tryPostSubOrderBy = "order by l, m, n";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select X.a l, X.z m, Y.z n from ttree X, tunique Y order by X.z, X.a, X.c, X.b, Y.a";
        tryOrderBy = ", X.z, Y.z";
        tryPostSubOrderBy = "order by l, m, n";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select X.a, X.z as xz, Y.z as yz, X.z + Y.z from ttree X, tunique Y order by 4, X.z, X.a, X.c, X.b, Y.a";
        tryOrderBy = ", Y.z";
        tryPostSubOrderBy = "order by 1, 2, 3, 4";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select X.a, X.z as xz, Y.z as yz from tuniqcombo X, tunique Y where X.a = Y.a";
        assertPlanNeedsSaferDeterminismCombo(sql);

        sql = "select X.a, X.z as xz, Y.z as yz from tuniqcombo X, tunique Y order by X.a, X.b, Y.a";
        assertPlanNeedsSaferDeterminismCombo(sql);

        sql = "select X.a, X.z as xz, Y.z as yz from tuniqcombo X, tunique Y order by X.a, X.c + X.b, X.b, Y.a";
        assertPlanNeedsSaferDeterminismCombo(sql);
    }

    public void testDeterminismOfSelectOrderGroupKeys() {
        String sql;
        String tryOrderBy;
        String tryPostSubOrderBy;

        sql = "select z, max(a)    from ttree group by z   ";
        tryOrderBy = "order by z";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select    max(a)    from ttree group by z   ";
        tryOrderBy = "order by z";
        tryPostSubOrderBy = "order by 1";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a), b from ttree group by z, b";
        tryOrderBy = "order by z, b";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select z, max(a)    from ttree group by z, b";
        tryOrderBy = "order by z, b";
        tryPostSubOrderBy = "order by 1, 2";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select    max(a)    from ttree group by z, b";
        tryOrderBy = "order by z, b";
        tryPostSubOrderBy = "order by 1";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a)      , b from ttree group by z, b order by z   ";
        tryOrderBy = ", b";
        tryPostSubOrderBy = "order by 1, 3";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a) max_a, b from ttree group by z, b order by z   ";
        tryOrderBy = ", b";
        tryPostSubOrderBy = "order by 1, 2";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select    max(a)    from ttree group by z, b order by z   ";
        tryOrderBy = ", b";
        tryPostSubOrderBy = "order by 1";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        // Odd edge cases of needlessly long ORDER BY clause
        sql = "select z, max(a), max(b) from ttree group by z";
        tryOrderBy = "order by z, max(b)";
        tryPostSubOrderBy = "order by z, 3";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a), max(b) from ttree group by z";
        tryOrderBy = "order by z, 3     ";
        tryPostSubOrderBy = "order by z, 3";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select    max(a), max(b) from ttree group by z";
        tryOrderBy = "order by z, max(b)";
        tryPostSubOrderBy = "order by 1, 2";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a), max(b) from ttree group by z order by max(b)";
        tryOrderBy = ", z";
        tryPostSubOrderBy = "order by 3, 1";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a), max(b) from ttree group by z";
        tryOrderBy = "order by z, max(a)";
        tryPostSubOrderBy = "order by 1, 2";
        /* not yet supported by planner? */ assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a) from ttree group by z";
        tryOrderBy = "order by z, max(b)";
        tryPostSubOrderBy = "order by 1, 2";
        //* not yet supported by planner? */ assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select    max(a) from ttree group by z";
        tryOrderBy = "order by z, max(b)";
        tryPostSubOrderBy = "order by 1, 3";
        //* not yet supported by planner? */ assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);
    }

    public void testDeterminismOfSelectOrderAll() {
        String sql;
        String tryOrderBy;

        sql = "select z from ttree";
        tryOrderBy = "order by z";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, false);

        sql = "select a, b, z from ttree";
        tryOrderBy = "order by 1, 2, 3";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, false);

        sql = "select a, b, z from ttree";
        tryOrderBy = "order by b, a, z";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, false);

        sql = "select a, b, z from ttree";
        tryOrderBy = "order by 3, 2, 1";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, false);
    }

    public void testDeterminismOfJoinOrderAll() {
        String sql;
        String tryOrderBy;
        String tryPostSubOrderBy;

        sql = "select X.a, X.z as xz, Y.z as yz from ttree X, tunique Y where X.a = Y.a";
        tryOrderBy = "order by 1, 2, 3";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select X.a l, X.z m, Y.z n from ttree X, tunique Y where X.a = Y.a";
        tryOrderBy = "order by Y.z, X.a, X.z";
        tryPostSubOrderBy = "order by l, m, n";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select X.a, X.z as xz, Y.z as yz from ttree X, tunique Y where X.a = Y.a";
        tryOrderBy = "order by 3, 2, 1";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy);
    }

    public void testDeterminismOfSelectIndexKeysOnly() {
        String sql;
        String tryOrderBy;

        sql = "select a, b from ttree";
        tryOrderBy = "order by a, b";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, false);

        sql = "select a, b, c from ttree";
        tryOrderBy = "order by a, b, c";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, false);

        // non-prefix keys don't help
        sql = "select b, c from ttree";
        tryOrderBy = "order by b, c";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, false);

        // if a table has a unique index... it can be used to scan in a r/w transaction
        sql = "select a from tunique";
        assertPlanNeedsSaferDeterminismCombo(sql);

        sql = "select a from tpk";
        assertPlanNeedsSaferDeterminismCombo(sql);

        // hashes don't help, here
        sql = "select a, b from thash";
        tryOrderBy = "order by a, b";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, false);

        sql = "select a, b, c from thash";
        tryOrderBy = "order by a, b, c";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, false);
    }

    public void testDeterminismOfSelectOneKeyValue() {
        String sql;
        String tryOrderBy;

        sql = "select a, z from ttree where a = 1";
        tryOrderBy = "order by a, z";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, false);

        sql = "select a, z from ttree where a = 1 and b < 10";
        tryOrderBy = "order by a, z";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, false);

        sql = "select a, z from tunique where a = 1";
        tryOrderBy = "order by z, a";
        assertPlanDeterminism(sql, tryOrderBy);

        sql = "select a, z from tunique where a = 1 and b < 10";
        tryOrderBy = "order by z, a";
        assertPlanDeterminism(sql, tryOrderBy);

        sql = "select a, z from tpk where a = 1";
        tryOrderBy = "order by z, a";
        assertPlanDeterminism(sql, tryOrderBy);

        sql = "select a, z from tpk where a = 1 and b < 10";
        tryOrderBy = "order by z, a";
        assertPlanDeterminism(sql, tryOrderBy);
    }

    private void assertDMLPlanDeterminism(String sql) {
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
        String sql;
        String tryOrderBy;
        String tryPostSubOrderBy;

        sql = "SELECT * FROM eng4155";
        tryOrderBy = "order by ts DESC, id";
        assertPlanDeterminism(sql, tryOrderBy);

        sql = "SELECT * FROM eng4155 ORDER BY ts DESC";
        tryOrderBy = ", id";
        tryPostSubOrderBy = "order by ts DESC, id";
        assertPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy, false);

        sql = "SELECT ts FROM eng4155";
        tryOrderBy = "order by ts DESC";
        assertPlanDeterminism(sql, tryOrderBy);
    }

    // MP section repeats tests on partitioned tables -- need to bypass subquerification for now.
    public void testMPDeterminismOfSelectStar() {
        String sql;
        String tryOrderBy;
        String tryPostSubOrderBy;

        // if a table has a unique index... it can be used to scan in a r/w transaction
        // even without ordering when planned in "safe" determinism mode.
        sql = "select * from ptree";
        tryOrderBy = "order by 1, 2, 3, 4";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select * from punique";
        tryOrderBy = "order by a";
        assertMPPlanNeedsSaferDeterminismOrOrderCombo(sql, tryOrderBy);

        sql = "select * from puniqcombo";
        tryOrderBy = "order by b, c, a";
        assertMPPlanNeedsSaferDeterminismOrOrderCombo(sql, tryOrderBy);

        sql = "select * from ppk";
        tryOrderBy = "order by a";
        assertMPPlanNeedsSaferDeterminismOrOrderCombo(sql, tryOrderBy);

        // test sufficiency of minimal orderings
        sql = "select * from ptree order by a";
        tryOrderBy = ", b, c, z";
        tryPostSubOrderBy = "order by a, b, c, z";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select * from ptree where a > 1 order by a";
        tryOrderBy = ", b, z, c";
        tryPostSubOrderBy = "order by a, b, z, c";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select * from ptree where a > 1 order by a, b";
        tryOrderBy = ", c, z";
        tryPostSubOrderBy = "order by a, b, c, z";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select * from punique where a > 1";
        tryOrderBy = "order by a";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select * from ppk where a > 1";
        tryOrderBy = "order by a";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);


        // test effects of sufficient but overly crowded or redundant order by
        sql = "select * from punique";
        tryOrderBy = "order by z, a, c";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select * from ppk";
        tryOrderBy = "order by z, a";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select * from ptree where a > 1 order by a, a+z";
        tryOrderBy = ", z, c, b";
        tryPostSubOrderBy = "order by a, a+z, z, c, b";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select * from punique where a > 1";
        tryOrderBy = "order by a";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select * from ppk where a > 1";
        tryOrderBy = "order by a";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);
    }

    public void testMPDeterminismOfJoin() {
        String sql;
        String tryOrderBy;
        String tryPostSubOrderBy;

        sql = "select X.a, X.z, Y.z from puniqcombo X, punique Y where X.a = Y.a";
        tryOrderBy = "order by X.a, X.c, X.b, Y.a";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select X.a, X.z, Y.z from puniqcombo X, punique Y where X.a = Y.a";
        tryOrderBy = "order by X.b, X.a, Y.a, X.c";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select X.a, X.z, Y.z from puniqcombo X, punique Y where X.a = Y.a";
        tryOrderBy = "order by X.z, X.a, X.c, X.b, Y.a";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select X.a, X.z, Y.z, X.z + Y.z from puniqcombo X, punique Y where X.a = Y.a";
        tryOrderBy = "order by 4, X.z, X.a, X.c, X.b, Y.a";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select X.a l, X.z m, Y.z n from ptree X, punique Y where X.a = Y.a order by X.a, X.c, X.b, Y.a";
        tryOrderBy = ", X.z, Y.z";
        tryPostSubOrderBy = "order by l, m, n";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select X.a l, X.z m, Y.z n from ptree X, punique Y where X.a = Y.a order by X.b, X.a, Y.a, X.c";
        tryOrderBy = ", X.z, Y.z";
        tryPostSubOrderBy = "order by l, m, n";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select X.a l, X.z m, Y.z n from ptree X, punique Y where X.a = Y.a order by X.z, X.a, X.c, X.b, Y.a";
        tryOrderBy = ", X.z, Y.z";
        tryPostSubOrderBy = "order by l, m, n";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select X.a, X.z, Y.z, X.z + Y.z from ptree X, punique Y where X.a = Y.a order by 4, X.z, X.a, X.c, X.b, Y.a";
        tryOrderBy = ", Y.z";
        tryPostSubOrderBy = "order by 1, 2, 3, 4";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select X.a, X.z, Y.z " +
                "from puniqcombo X, punique Y where X.a = Y.a";
        assertMPPlanNeedsSaferDeterminismCombo(sql);

        sql = "select X.a, X.z, Y.z " +
                "from puniqcombo X, punique Y where X.a = Y.a " +
                "order by X.a, X.b, Y.a";
        assertMPPlanNeedsSaferDeterminismCombo(sql);

        sql = "select X.a, X.z, Y.z " +
                "from puniqcombo X, punique Y where X.a = Y.a " +
                "order by X.a, X.c + X.b, X.b, Y.a";
        assertMPPlanNeedsSaferDeterminismCombo(sql);
    }

    public void testMPDeterminismOfSelectOrderGroupKeys() {
        String sql;
        String tryOrderBy;
        String tryPostSubOrderBy;

        sql = "select z, max(a)    from ptree group by z   ";
        tryOrderBy = "order by z   ";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select    max(a)    from ptree group by z   ";
        tryOrderBy = "order by z   ";
        tryPostSubOrderBy = "order by 1";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a), b from ptree group by z, b";
        tryOrderBy = "order by z, b";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select z, max(a)    from ptree group by z, b";
        tryOrderBy = "order by z, b";
        tryPostSubOrderBy = "order by 1, 2";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select    max(a)    from ptree group by z, b";
        tryOrderBy = "order by z, b";
        tryPostSubOrderBy = "order by 1";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a)      , b from ptree group by z, b order by z   ";
        tryOrderBy = ", b";
        tryPostSubOrderBy = "order by 1, 3";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a) max_a, b from ptree group by z, b order by z   ";
        tryOrderBy = ", b";
        tryPostSubOrderBy = "order by 1, 2";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        // Odd edge cases of needlessly long ORDER BY clause
        sql = "select    max(a)          from ptree group by z, b order by z   ";
        tryOrderBy = ", b";
        tryPostSubOrderBy = "order by 1";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a), max(b)  from ptree group by z";
        tryOrderBy = "order by z, max(b)";
        tryPostSubOrderBy = "order by z, 3";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a), max(b)  from ptree group by z";
        tryOrderBy = "order by z, 3     ";
        tryPostSubOrderBy = "order by z, 3";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select    max(a), max(b)  from ptree group by z";
        tryOrderBy = "order by z, max(b)";
        tryPostSubOrderBy = "order by 1, 2";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a), max(b)  from ptree group by z order by max(b)";
        tryOrderBy = ", z";
        tryPostSubOrderBy = "order by 3, 1";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a), max(b)  from ptree group by z";
        tryOrderBy = "order by z, max(a)";
        tryPostSubOrderBy = "order by 1, 2";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select z, max(a)          from ptree group by z";
        tryOrderBy = "order by z, max(b)";
        tryPostSubOrderBy = "order by 1";
        // not yet supported by planner */ assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);

        sql = "select    max(a)          from ptree group by z";
        tryOrderBy = "order by z, max(b)";
        tryPostSubOrderBy = "order by 1";
        // not yet supported by planner */ assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy, tryPostSubOrderBy);
    }

    public void testMPDeterminismOfSelectOrderAll() {
        String sql;
        String tryOrderBy;

        sql = "select z from ptree";
        tryOrderBy = "order by z";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select a, b, z from ptree";
        tryOrderBy = "order by 1, 2, 3";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select a, b, z from ptree";
        tryOrderBy = "order by b, a, z";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select a, b, z from ptree";
        tryOrderBy = "order by 3, 2, 1";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);
    }

    public void testMPDeterminismOfJoinOrderAll() {
        String sql;
        String tryOrderBy;

        sql = "select X.a, X.z, Y.z from ptree X, punique Y where X.a = Y.a";
        tryOrderBy = "order by 1, 2, 3";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select X.a l, X.z m, Y.z n from ptree X, punique Y where X.a = Y.a";
        tryOrderBy = "order by Y.z, X.a, X.z";
        tryOrderBy = "order by l, m, n";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select X.a, X.z, Y.z from ptree X, punique Y where X.a = Y.a";
        tryOrderBy = "order by 3, 2, 1";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);
    }

    public void testMPDeterminismOfSelectIndexKeysOnly() {
        String sql;
        String tryOrderBy;

        sql = "select a, b from ptree";
        tryOrderBy = "order by a, b";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select a, b, c from ptree";
        tryOrderBy = "order by a, b, c";
        // non-prefix keys don't help
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select b, c from ptree";
        tryOrderBy = "order by b, c";
        // if a table has a unique index... it can be used to scan in a r/w transaction
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select a from punique";
        assertMPPlanNeedsSaferDeterminismCombo(sql);

        sql = "select a from ppk";
        assertMPPlanNeedsSaferDeterminismCombo(sql);

        // hashes don't help, here
        sql = "select a, b from phash";
        tryOrderBy = "order by a, b";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select a, b, c from phash";
        tryOrderBy = "order by a, b, c";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);
    }

    public void testMPDeterminismOfSelectOneKeyValue() {
        String sql;
        String tryOrderBy;

        sql = "select a, z from ptree where a = 1";
        tryOrderBy = "order by a, z";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select a, z from ptree where a = 1 and b < 10";
        tryOrderBy = "order by a, z";
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);

        sql = "select a, z from punique where a = 1";
        tryOrderBy = "order by z, a";
        assertMPPlanDeterminism(sql, tryOrderBy);

        sql = "select a, z from punique where a = 1 and b < 10";
        tryOrderBy = "order by z, a";
        assertMPPlanDeterminism(sql, tryOrderBy);

        sql = "select a, z from ppk where a = 1";
        tryOrderBy = "order by z, a";
        assertMPPlanDeterminism(sql, tryOrderBy);

        sql = "select a, z from ppk where a = 1 and b < 10";
        tryOrderBy = "order by z, a";
        assertMPPlanDeterminism(sql, tryOrderBy);
    }

    public void testMPDeterminismImpliedByParameter() {
        String sql;

        sql = "select * from ttree_with_key where b = ? order by a, c limit 1;";
        assertPlanDeterminismCore(sql, true, true, DeterminismMode.FASTER);

        sql = "select d, e, id from ttree_with_key order by a, b, c limit 1;";
        assertPlanDeterminismCore(sql, true, true, DeterminismMode.FASTER);

        // We could have ? = 1, ttree_with_key = [(1, 1, 1, 2, 3), (1, -1, 1, 2, 3)].
        // Then the order evaluation has the single value [(1, 1)] and all rows are
        // selected by the where clause.  This means both rows are listed, but
        // we don't know in which order they will be listed.  The limit makes it
        // not content deterministic as well.
        sql = "select * from ttree_with_key where abs(b) = ? order by a, c limit 1;";
        assertPlanDeterminismCore(sql, false, false, DeterminismMode.FASTER);

        // If ENG-8677 and follow on tickets are closed,
        // we can reenable these tests.
        final boolean ENG8677IsFixed = false;

        // Two tables.  Actually, this is a self join as well.  That
        // may be a reason to not allow it here.
        sql = "select tleft.* from ttree_with_key as tleft join ttree_with_key as tright on tleft.id = tright.id where tleft.b = ? order by tleft.a, tleft.c limit 1;";
        assertPlanDeterminismCore(sql, ENG8677IsFixed, ENG8677IsFixed, DeterminismMode.FASTER);

        // Functions of order by expressions should be ok, whether they.
        // are deterministic or not.
        sql = "select abs(a) + abs(b) from ttree_with_key order by a+1, b+1, c+2 limit 1;";
        assertPlanDeterminismCore(sql, ENG8677IsFixed, ENG8677IsFixed, DeterminismMode.FASTER);

        // Since these functions (x->x + 1 and x->x + 2) are one-to-one, these
        // functions are ok.
        sql = "select * from ttree_with_key order by a+1, b+1, c+2 limit 1;";
        assertPlanDeterminismCore(sql, ENG8677IsFixed, ENG8677IsFixed, DeterminismMode.FASTER);

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
        sql = "select r.d, l.e, r.id " +
                "from ttree_with_key as l join ttree_with_key as r " +
                "on l.a = r.a and l.b = r.b and l.c = r.c " +
                "order by l.a, r.b, l.c limit 1;";
        assertPlanDeterminismCore(sql, ENG8677IsFixed, ENG8677IsFixed, DeterminismMode.FASTER);

        // This is probably not possible. Let A be the matrix
        //          | 1  1  1 |
        //     A =  | 1  1 -1 |
        //          | 1 -1  1 |
        // Then det(A) = -3 != 0, and A is invertible.  If A*[a, b, c]' = A*[aa, bb, cc]', then
        // [a, b, c]' = [aa, bb, cc]'.  Since (a, b, c) is a primary key, this means
        // all the values are determined.
        sql = "select * from ttree_with_key order by a + b + c, a + b - c, a - b + c limit 1;";
        assertPlanDeterminismCore(sql, ENG8677IsFixed, ENG8677IsFixed, DeterminismMode.FASTER);
    }

    public void testUnionDeterminism() throws Exception {
        String sql;

        // LHS of union is not deterministic
        sql = "(select a, b, c from ttree_with_key) " +
                "union (select a, b, c from ttree_with_key order by a, b, c limit 1);";
        assertPlanDeterminismCore(sql, false, true, DeterminismMode.FASTER);

        // LHS of union is not deterministic,
        // but ORDER BY clause determines order of whole statement.
        sql = "(select a, b, c from ttree_with_key) " +
                "union (select a, b, c from ttree_with_key order by a, b, c limit 1) " +
                "order by a, b, c;";
        assertPlanDeterminismCore(sql, true, true, DeterminismMode.FASTER);

        // RHS of union is not deterministic
        sql = "(select a, b, c from ttree_with_key order by a, b, c limit 1) " +
                "union (select a, b, c from ttree_with_key);";
        assertPlanDeterminismCore(sql, false, true, DeterminismMode.FASTER);

        sql = "(select a, b, c from ttree_with_key order by a, b, c limit 1) " +
                "union (select a, b, c from ttree_with_key) " +
                "union (select a, b, c from ttree_with_key);";
        assertPlanDeterminismCore(sql, false, true, DeterminismMode.FASTER);

        // Both sides are deterministic; whole statement is deterministic
        sql = "(select a, b, c from ttree_with_key order by a, b, c) " +
                  "union (select a, b, c from ttree_with_key order by a, b, c limit 1);";
        assertPlanDeterminismCore(sql, true, true, DeterminismMode.FASTER);
    }

    public void testFloatingAggs() throws Exception {
        String sql = "select sum(alpha + beta + gamma) as fsum " +
                "from floataggs order by fsum;";
        assertPlanDeterminismFullCore(sql, true, true, false, DeterminismMode.FASTER);
    }

    public void testDeterminismForUniqueConstraintEquivalence() {
        String sql;

        // equivalence filter in predicate clause using unique key with display list with
        // different column than in where clause predicate.
        sql = "select b from punique where a = ?";
        assertMPPlanDeterminismCore(sql, ORDERED, CONSISTENT, false, DeterminismMode.FASTER);
        assertMPPlanDeterminismCore(sql, ORDERED, CONSISTENT, false, DeterminismMode.SAFER);

        // where clause using equivalence filter on table using all columns
        // forming composite primary
        // key as this select statement can be in insert clause too, test it for
        // safer determinism also
        sql = "select z from ppkcombo where a = 1 AND b = 2 AND c = 3;";
        assertMPPlanDeterminismCore(sql, ORDERED, CONSISTENT, false, DeterminismMode.FASTER);
        assertMPPlanDeterminismCore(sql, ORDERED, CONSISTENT, false, DeterminismMode.SAFER);

        // using parameters
        sql = "select z from ppkcombo where a = ? AND b = ? AND c = ?;";
        assertMPPlanDeterminismCore(sql, ORDERED, CONSISTENT, false, DeterminismMode.FASTER);
        assertMPPlanDeterminismCore(sql, ORDERED, CONSISTENT, false, DeterminismMode.SAFER);

        // predicate containing value-equivalence through transitive behavior that form unique
        // index on table
        sql = "select c, z from ppkcombo where a = ? AND a = b AND c = b;";
        assertMPPlanDeterminismCore(sql, ORDERED, CONSISTENT, false, DeterminismMode.FASTER);
        assertMPPlanDeterminismCore(sql, ORDERED, CONSISTENT, false, DeterminismMode.SAFER);

        sql = "select * from ppkcombo where a = ? AND z = a;";
        assertMPPlanDeterminismCore(sql, ORDERED, CONSISTENT, false, DeterminismMode.FASTER);
        assertMPPlanDeterminismCore(sql, ORDERED, CONSISTENT, false, DeterminismMode.SAFER);

        // predicate clause has equivalence filter but not value-equivalence
        sql = "select c, z from ppkcombo where a = b AND c = b;";
        assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, false, DeterminismMode.FASTER);

        // query does not guarantee determinism if not all columns that form
        // compound index, are present in part of value equivalence
        sql = "select c, z from ppkcombo where a = 1 AND c = 3;";
        assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, false, DeterminismMode.FASTER);

        // predicate clause including all columns of compound key with
        // OR operator, does not guarantee single row output
        sql = "select c, z from ppkcombo where a = 1 OR b = ? AND c = 3;";
        assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, false, DeterminismMode.FASTER);

        // predicate clause including all columns of composite keys of composite but not
        // value equivalence
        sql = "select c, z from ppkcombo where a > 1 AND b = ? AND c = 3;";
        assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, false, DeterminismMode.FASTER);

        // non-equivalence filter on unique filter does not guarantee determinism
        sql = "select a from ppk where a > 1;";
        assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, false, DeterminismMode.FASTER);

        // Two-table scan query with predicate containing value equivalence filters, such that it covers
        // unique indexes defined on both table, output will contain at most one row and is deterministic.
        // Though the logic for unique index equivalence is defined for single table currently, so multi-
        // table statement gets flagged incorrectly as non-deterministic. ENG-10299 is to address this issue.

        // Change the test to expect determinism when ENG-10299 is fixed and execute test with safer
        // determinism mode.
        sql = "select * from punique, ppk where punique.a = ppk.a and ppk.a = ?";
        assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, false, DeterminismMode.FASTER);
        //assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, true, DeterminismMode.SAFER);

        // Change the test to expect determinism when ENG-10299 is fixed and execute test with safer
        // determinism mode.
        sql = "select * from ppkcombo, ppk where ppkcombo.a = ppk.a and ppk.a = ? and ppkcombo.z = ?";
        assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, false, DeterminismMode.FASTER);
        //assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, true, DeterminismMode.SAFER);

        // query on multi-table with predicate only has equivalence but not value equivalence on unique
        // indexes defined on both the tables. At most one row output is not guaranteed.
        sql = "select * from punique, ppk where punique.a = ppk.a";
        assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, false, DeterminismMode.FASTER);
    }

    /*
     * A test that runs some read-only non-deterministic DDLs, check that the output does not contain
     * any ND warnings
     */
    public void testDeterminismReadOnlyNoWarning() throws Exception {
        // Should the members be reused here?
        HSQLInterface hsql = HSQLInterface.loadHsqldb(ParameterizationInfo.getParamStateManager());
        VoltCompiler compiler = new VoltCompiler(false);
        VoltCompiler.DdlProceduresToLoad all_procs = DdlProceduresToLoad.NO_DDL_PROCEDURES;

        URL path = TestDeterminism.class.getResource("testplans-determinism-read-only.sql");
        String pathStr = URLDecoder.decode(path.getPath(), "UTF-8");
        compiler.loadSchema(hsql, all_procs, pathStr);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        compiler.summarizeSuccess(new PrintStream(outputStream), null, "");
        System.out.println(outputStream.toString());
        // There is no warnings generated for read-only ND queries
        String msg = outputStream.toString();
        assertFalse(stringContains(msg, "ND"));
        assertFalse(stringContains(msg, "WARN"));
        assertTrue(stringContains(msg, "Successfully created"));
        assertTrue(stringContains(msg, "Catalog contains"));
        outputStream.close();
    }

    private void assertMPPlanDeterminismCore(String sql, boolean order, boolean content, DeterminismMode detMode) {
        assertMPPlanDeterminismCore(sql, order, content, true, detMode);
        }

    /**
     * Tests MP compiled plan for the specified determinism properties
     *
     * @param sql - SQL statement
     * @param order - Specifies whether the statement should result in order determinism or not
     * @param content - Specifies whether the statement should result in content determinism or not
     * @param inferPartitioning - If set to true uses inferred partitioning to generate the compiled plan;
     *                            if set to false applies force MP to generate the compiled plan.
     * @param detMode - Determinism mode - faster or slower.
     */
    private void assertMPPlanDeterminismCore(String sql, boolean order, boolean content,
            boolean inferPartitioning, DeterminismMode detMode) {

        final boolean forceSP = false;
        CompiledPlan cp = compileAdHocPlan(sql, inferPartitioning, forceSP, detMode);
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
    private void assertMPPlanDeterminismCombo(String sql,
            boolean order,
            boolean content,
            String tryOrderBy,
            String tryPostSubOrderBy,
            DeterminismMode detMode) {
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

    private void assertMPPlanNeedsSaferDeterminismCombo(String sql) {
        assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, DeterminismMode.FASTER);
        String limitedStatement = sql + " LIMIT 2";
        assertMPPlanDeterminismCore(limitedStatement, UNORDERED, INCONSISTENT, DeterminismMode.FASTER);
        // These MP cases ARE NOT deterministic in the safer mode.
        assertMPPlanDeterminismCore(sql, UNORDERED, CONSISTENT, DeterminismMode.SAFER);
        assertMPPlanDeterminismCore(limitedStatement, UNORDERED, INCONSISTENT, DeterminismMode.SAFER);
    }

    private void assertMPPlanNeedsSaferDeterminismOrOrderCombo(String sql,
            String tryOrderBy) {
        assertMPPlanNeedsSaferDeterminismCombo(sql);
        assertMPPlanDeterminismNeedsOrdering(sql, tryOrderBy);
    }

    private void assertMPPlanDeterminismNeedsOrdering(String sql, String tryOrderBy) {
        assertMPPlanDeterminismCombo(sql, UNORDERED, CONSISTENT, tryOrderBy, tryOrderBy, DeterminismMode.FASTER);
    }

    private void assertMPPlanDeterminismNeedsOrdering(String sql,
            String tryOrderBy,
            String tryPostSubOrderBy) {
        assertMPPlanDeterminismCombo(sql, UNORDERED, CONSISTENT, tryOrderBy, tryPostSubOrderBy, DeterminismMode.FASTER);
    }

    /**
     * Checks determinism of statement against expected results, with or without factoring in order effects
     * @param sql
     */
    private void assertMPPlanDeterminism(String sql, String tryOrderBy) {
        assertMPPlanDeterminismCombo(sql, ORDERED, CONSISTENT, tryOrderBy, null, DeterminismMode.SAFER);
    }

    /*
     * Helper function for sub-string regex matching
     */
    private boolean stringContains(String msg, String regex) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(regex);
        return pattern.matcher(msg).find();
    }
}
