/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.PlanNodeType;

public class TestPlansOrderBy extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(TestPlansGroupBy.class.getResource("testplans-orderby-ddl.sql"),
                    "testplansorderby", false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private void validatePlan(String sql, boolean expectIndexScan, boolean expectSeqScan, boolean expectOrderBy, boolean expectHashAggregate)
    {
        AbstractPlanNode pn = compile(sql);
        assertEquals(expectIndexScan, pn.hasAnyNodeOfType(PlanNodeType.INDEXSCAN));
        assertEquals(expectSeqScan, pn.hasAnyNodeOfType(PlanNodeType.SEQSCAN));
        assertEquals(expectOrderBy, pn.hasAnyNodeOfType(PlanNodeType.ORDERBY));
        assertEquals(expectHashAggregate, pn.hasAnyNodeOfType(PlanNodeType.HASHAGGREGATE));
    }

    /// Validate that a plan uses the full bag of tricks
    /// -- that it uses an index scan but no seq scan, order by, or hash aggregate.
    private void validateOptimalPlan(String sql)
    {
        validatePlan(sql, true, false, false, false);
    }

    /// Validate that a plan does not unintentionally use
    /// a completely inapplicable index scan for order.
    private void validateBruteForcePlan(String sql)
    {
        validatePlan(sql, false, true, true, false);
    }

    /// Validate that a plan does not unintentionally use
    /// a completely inapplicable index scan for order,
    /// when it still uses an index scan for non-determinism.
    /// This arguably should not happen for plans where the final
    /// sort already confers determinism (yet it currently does)?.
    private void validateIndexedBruteForcePlan(String sql)
    {
        validatePlan(sql, true, false, true, false);
    }

    public void testOrderByOneOfTwoIndexKeys()
    {
        validateOptimalPlan("SELECT * from T ORDER BY T_D0");
        validateOptimalPlan("SELECT * from T WHERE T_D0 = 1 ORDER BY T_D1");
    }

    public void testOrderByTwo() {
        validatePlan("SELECT * from T ORDER BY T_D0, T_D1", true, false, false, false);
    }

    public void testOrderByTwoDesc() {
        validatePlan("SELECT * from T ORDER BY T_D0 DESC, T_D1 DESC", true, false, false, false);
    }

    public void testOrderByTwoAscDesc() {
        validateIndexedBruteForcePlan("SELECT * from T ORDER BY T_D0, T_D1 DESC");
        validateBruteForcePlan("SELECT * from Tnokey ORDER BY T_D0, T_D1 DESC");
    }

    public void testOrderByTwoOfThreeIndexKeys()
    {
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D0 = ? ORDER BY T_D1, T_D2");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D1 = ? ORDER BY T_D0, T_D2");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D2 = ? ORDER BY T_D0, T_D1");
        validateOptimalPlan("SELECT * from Tmanykeys                ORDER BY T_D0, T_D1");
    }

    public void testOrderByOneOfThreeIndexKeys()
    {
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D0 = ? AND T_D1 = ? ORDER BY T_D2");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D1 = ? AND T_D2 = ? ORDER BY T_D0");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D2 = ? AND T_D0 = ? ORDER BY T_D1");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D0 = ?              ORDER BY T_D1");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D1 = ?              ORDER BY T_D0");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D2 = ?              ORDER BY T_D0");
        validateOptimalPlan("SELECT * from Tmanykeys                             ORDER BY T_D0");
    }

    public void testOrderByWrongPermutation()
    {
        validateIndexedBruteForcePlan("SELECT * from Tmanykeys ORDER BY T_D2, T_D1, T_D0");
        validateIndexedBruteForcePlan("SELECT * from Tmanykeys ORDER BY T_D2, T_D0, T_D1");
        validateIndexedBruteForcePlan("SELECT * from Tmanykeys ORDER BY T_D1, T_D0, T_D2");
        validateIndexedBruteForcePlan("SELECT * from Tmanykeys ORDER BY T_D1, T_D2, T_D0");
        validateIndexedBruteForcePlan("SELECT * from Tmanykeys WHERE T_D0 = ? ORDER BY T_D2, T_D1");
        validateIndexedBruteForcePlan("SELECT * from Tmanykeys WHERE T_D1 = ? ORDER BY T_D2, T_D0");
        validateIndexedBruteForcePlan("SELECT * from Tmanykeys ORDER BY T_D0, T_D2, T_D1");
    }

    public void testOrderByTooManyToIndex()
    {
        validateIndexedBruteForcePlan("SELECT * from T ORDER BY T_D0, T_D1, T_D2");
        validateBruteForcePlan("SELECT * from Tnokey ORDER BY T_D0, T_D1, T_D2");
    }

    public void testOrderByTooMany()
    {
        validateBruteForcePlan("SELECT * from Tnokey ORDER BY T_D0, T_D1, T_D2");
        validateIndexedBruteForcePlan("SELECT * from T ORDER BY T_D0, T_D1, T_D2");
    }

    public void testNoIndexToOrderBy() {
        validateIndexedBruteForcePlan("SELECT * FROM T ORDER BY T_D2");
        validateBruteForcePlan("SELECT * FROM Tnokey ORDER BY T_D2");
        validateIndexedBruteForcePlan("SELECT * FROM Tmanykeys ORDER BY T_D2");
    }

    public void testOrderByNLIJ()
    {
        validatePlan("SELECT Tnokey.T_D1, T.T_D0, T.T_D1 from Tnokey, T " +
                     "where Tnokey.T_D2 = 2 AND T.T_D0 = Tnokey.T_D0 " +
                     "ORDER BY T.T_D0, T.T_D1", true, true, true, false);
    }

    //TODO: This test actually validates that we generate a sub-optimal plan for this query
    //-- but we're keeping the test because, well, at least the query compiles to SOME kind of plan?
    //When ENG-4096 is addressed, the validation will be quite different.
    public void testOrderByCountStar() {
        validatePlan("SELECT T_D0, COUNT(*) AS FOO FROM T GROUP BY T_D0 ORDER BY FOO", true, false, true, true);
        validatePlan("SELECT T_D0, COUNT(*) AS FOO FROM Tnokey GROUP BY T_D0 ORDER BY FOO", false, true, true, true);
        //Expected ENG-4096 effect:
        //validatePlan("SELECT T_D0, COUNT(*) AS FOO FROM T GROUP BY T_D0 ORDER BY FOO");
    }

    public void testEng450()
    {
        // This used to not compile. It does now. That's all we care about.
        compile("select T.T_D0, " +
                     "sum(T.T_D1) " +
                     "from T " +
                     "group by T.T_D0 " +
                     "order by T.T_D0;");
    }

    public void testOrderDescWithEquality() {
        validatePlan("SELECT * FROM T WHERE T_D0 = 2 ORDER BY T_D1 DESC", true, false, true, false);
    }
}
