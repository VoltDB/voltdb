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
        forceReplication();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    void validatePlan(String sql, boolean expectIndexScan, boolean expectSeqScan, boolean expectOrderBy, boolean expectHashAggregate)
    {
        AbstractPlanNode pn = compile(sql);
        assertEquals(expectIndexScan, pn.hasAnyNodeOfType(PlanNodeType.INDEXSCAN));
        assertEquals(expectSeqScan, pn.hasAnyNodeOfType(PlanNodeType.SEQSCAN));
        assertEquals(expectOrderBy, pn.hasAnyNodeOfType(PlanNodeType.ORDERBY));
        assertEquals(expectHashAggregate, pn.hasAnyNodeOfType(PlanNodeType.HASHAGGREGATE));
    }

    public void testOrderByOne() {
        validatePlan("SELECT * from T ORDER BY T_PKEY", true, false, false, false);
    }

    public void testOrderByTwo() {
        validatePlan("SELECT * from T ORDER BY T_PKEY, T_D1", true, false, false, false);
    }

    public void testOrderByTwoDesc() {
        validatePlan("SELECT * from T ORDER BY T_PKEY DESC, T_D1 DESC", true, false, false, false);
    }

    public void testOrderByTwoAscDesc() {
        validatePlan("SELECT * from T ORDER BY T_PKEY, T_D1 DESC", false, true, true, false);
    }

    public void testOrderByThree() {
        validatePlan("SELECT * from T ORDER BY T_PKEY, T_D1, T_D2", false, true, true, false);
    }

    public void testNoOrderBy() {
        validatePlan("SELECT * FROM T ORDER BY T_D2", false, true, true, false);
    }

    //TODO: This test actually validates that we generate a sub-optimal plan for this query
    //-- but we're keeping the test because, well, at least the query compiles to SOME kind of plan?
    //When ENG-4096 is addressed, the validation will be quite different.
    public void testOrderByCountStar() {
        validatePlan("SELECT T_PKEY, COUNT(*) AS FOO FROM T GROUP BY T_PKEY ORDER BY FOO", false, true, true, true);
        //Expected ENG-4096 effect:
        //validatePlan("SELECT T_PKEY, COUNT(*) AS FOO FROM T GROUP BY T_PKEY ORDER BY FOO", true, false, true, false);
    }

    public void testEng450()
    {
        // This used to not compile. It does now. That's all we care about.
        compile("select T.T_PKEY, " +
                     "sum(T.T_D1) " +
                     "from T " +
                     "group by T.T_PKEY " +
                     "order by T.T_PKEY;");
    }

    public void testOrderDescWithEquality() {
        validatePlan("SELECT * FROM T WHERE T_PKEY = 2 ORDER BY T_PKEY DESC, T_D1 DESC", true, false, true, false);
    }
}
