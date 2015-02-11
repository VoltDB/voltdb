/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.List;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexCountPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.TableCountPlanNode;
import org.voltdb.types.PlanNodeType;

public class TestReplaceWithIndexCounter extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(getClass().getResource("testplans-indexcounter-ddl.sql"),
                    "testindexcounter", false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    // DOES NOT support the cases down below right now

    // This is treated as new TABLE COUNT plan for replicated table
    public void testCountStar000() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T1");
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof TableCountPlanNode);
    }
    // This is treated as new TABLE COUNT plan for partitioned table
    public void testCountStar001() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from P1");
        AbstractPlanNode p = pn.get(0).getChild(0);
        // AGGREGATE_SUM
        assertTrue(p instanceof AggregatePlanNode);
        p = pn.get(1).getChild(0);
        assertTrue(p instanceof TableCountPlanNode);
    }

    public void testCountStar002() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT POINTS, count(*) from T1 Group by POINTS");
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.AGGREGATE));
    }


    // Test subquery temp table count
    public void testCountStar003() {
        List<AbstractPlanNode> pn = compileToFragments("select count(*) from (SELECT count(*) from T1) Temp");
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof TableCountPlanNode);
        p = p.getChild(0);
        assertTrue(p instanceof TableCountPlanNode);
    }

    public void testCountStar004() {
        List<AbstractPlanNode> pn = compileToFragments("select count(*) from (SELECT count(*) from P1) Temp");
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof TableCountPlanNode);
        p = p.getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        p = pn.get(1).getChild(0);
        assertTrue(p instanceof TableCountPlanNode);
    }

    // This is generated as an IndexScan which can't be converted into an index count,
    // rather than as the same table count as "SELECT count(*) from T1".
    // The meaningless "order by" here fools the planner.
    // We should fix this later by generally ignoring the "order by" clause on non-grouped aggregate queries.
    public void testCountStar01() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T1 ORDER BY POINTS ASC");
        checkIndexCounter(pn, false);
    }

    public void testCountStar02() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT P1.ID, T3.T3_ID from P1, T3 where P1.ID >= T3.T3_ID order by P1.ID, T3.T3_ID limit 10");
        checkIndexCounter(pn, false);
    }

    public void testCountStar04() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T1 WHERE POINTS = ?");
        checkIndexCounter(pn, true);
    }

    public void testCountStar05() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T1 WHERE POINTS < ? ORDER BY ID DESC");
        checkIndexCounter(pn, true);
    }

    public void testCountStar06() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T1 WHERE POINTS >= 3 AND AGE = ?");
        checkIndexCounter(pn, false);
    }

    public void testCountStar07() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND AGE = 3 AND POINTS < ?");
        checkIndexCounter(pn, false);
    }

    public void testCountStar08() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T2 WHERE USERNAME >'XIN' AND POINTS = ?");
        checkIndexCounter(pn, false);
    }

    public void testCountStar10() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS > ?");
        checkIndexCounter(pn, true);
    }

    // Down below are cases that we can replace
    public void testCountStar11() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T1 WHERE POINTS < 4");
        checkIndexCounter(pn, true);
    }

    public void testCountStar12() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T1 WHERE POINTS < ?");
        checkIndexCounter(pn, true);
    }

    public void testCountStar13() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T1 WHERE POINTS >= 3");
        checkIndexCounter(pn, true);
    }

    public void testCountStar14() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T1 WHERE POINTS > 3 AND POINTS <= 6");
        checkIndexCounter(pn, true);
    }

    public void testCountStar15() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T1 WHERE POINTS > ? AND POINTS < ?");
        checkIndexCounter(pn, true);
    }

    public void testCountStar16() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS >= ? AND POINTS <= ?");
        checkIndexCounter(pn, true);
    }

    public void testCountStar17() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS < ?");
        checkIndexCounter(pn, true);
    }

    public void testCountStar18() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T2 WHERE USERNAME ='XIN'");
        checkIndexCounter(pn, false);
    }

    // Planner bug with Constant value overflow
    public void testCountStar19() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS >= 3 AND POINTS <= 600000000000000000000000000");
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue((p instanceof IndexCountPlanNode) == false);
    }
    // test with group by with Replicated table
    public void testCountStar20() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT AGE, count(*) from T2 WHERE USERNAME ='XIN' AND POINTS < 1 Group by AGE");
        for ( AbstractPlanNode nd : pn)
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
        assertTrue(p.getInlinePlanNode(PlanNodeType.AGGREGATE) != null ||
                p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
    }

    // test with group by with Partitioned table
    public void testCountStar21() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT RATIO, count(*) from P1 WHERE NUM < 1 Group by RATIO");
        for ( AbstractPlanNode nd : pn)
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        p = pn.get(1).getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
        assertTrue(p.getInlinePlanNode(PlanNodeType.AGGREGATE) != null ||
                p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
    }

    // Test counting index feature with partitioned table
    public void testCountStar22() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from P1 WHERE NUM < ?");
        for ( AbstractPlanNode nd : pn)
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        p = pn.get(1).getChild(0);
        assertTrue(p instanceof IndexCountPlanNode);
    }

    public void testCountStar23() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T1 WHERE POINTS < 4 ORDER BY POINTS DESC");
        checkIndexCounter(pn, true);
    }

    public void testCountStar24() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T2 WHERE ID = 1 AND USERNAME > 'JOHN' ");
        checkIndexCounter(pn, false);
    }

    public void testCountStar25() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS >= ?");
        checkIndexCounter(pn, true);
    }

    // test with FLOAT type
    public void testCountStar26() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from P1 WHERE NUM = 1 AND RATIO >= ?");
        for ( AbstractPlanNode nd : pn)
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        p = pn.get(1).getChild(0);
        assertTrue(p instanceof IndexCountPlanNode);
    }

    // should not replace
    public void testCountStar27() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT COUNT(*) FROM T2 WHERE USERNAME >= 'XIN' AND POINTS = ?");
        checkIndexCounter(pn, false);
    }

    public void testCountStar28() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT COUNT(*) FROM T1 WHERE AGE = 18 AND ID + POINTS > ?");
        checkIndexCounter(pn, true);
    }

    public void testCountStar29() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT COUNT(*) FROM T1 WHERE AGE / 2 = 10 AND ABS(AGE) > ?");
        checkIndexCounter(pn, true);
    }

    // should not replace
    public void testCountStar30() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT COUNT(*) FROM T1 WHERE AGE / 2 <= ?");
        checkIndexCounter(pn, false);
    }

    // Not padding maximum value case.
    public void testCountStar32() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT COUNT(*) FROM T2 WHERE ID = 1 AND USERNAME > ?");
        checkIndexCounter(pn, false);
    }

    // Padding maximum value case.
    public void testCountStar31() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(*) from T3 WHERE T3_NUM1 =1 AND T3_NUM2 >= ?");
        checkIndexCounter(pn, true);
    }


    /**
     * Check Whether or not the original plan is replaced with CountingIndexPlanNode.
     *
     * @param pn
     *            The generated plan
     * @param isReplaceable
     *            Whether or not the original plan is replaced with CountingIndexPlanNode
     */
    private void checkIndexCounter(List<AbstractPlanNode> pn, boolean isReplaceable) {
        assertTrue(pn.size() > 0);

        for ( AbstractPlanNode nd : pn) {
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
        }
        AbstractPlanNode p = pn.get(0).getChild(0);
        if (isReplaceable)
            assertTrue(p instanceof IndexCountPlanNode);
        else
            assertTrue((p instanceof IndexCountPlanNode) == false);

    }
}
