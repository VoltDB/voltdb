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

import java.util.ArrayList;
import java.util.List;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.DistinctPlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.ExpressionType;

public class TestPlansGroupBy extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(TestPlansGroupBy.class.getResource("testplans-groupby-ddl.sql"),
                    "testplansgroupby", false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    List<AbstractPlanNode> pns = new ArrayList<AbstractPlanNode>();

    public void testGroupByA1() {
        pns = compileToFragments("SELECT A1 from T1 group by A1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    public void testCountA1() {
        pns = compileToFragments("SELECT count(A1) from T1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    public void testCountStar()
    {
        pns = compileToFragments("SELECT count(*) from T1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    public void testCountDistinctA1() {
        pns = compileToFragments("SELECT count(distinct A1) from T1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    public void testDistinctA1() {
        pns = compileToFragments("SELECT DISTINCT A1 FROM T1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    public void testEdgeComplexRelatedCases() {
        // Make sure that this query will compile correctly
        pns = compileToFragments("select PKEY+A1 from T1 Order by PKEY+A1");
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0) instanceof OrderByPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof ReceivePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);

        // Make it to false when we fix ENG-4397
        // ENG-4937 - As a developer, I want to ignore the "order by" clause on non-grouped aggregate queries.
        pns = compileToFragments("SELECT count(*)  FROM P1 order by PKEY");
        checkHasComplexAgg(pns);

        // Make sure it compile correctly
        pns = compileToFragments("SELECT A1, count(*) as tag FROM P1 group by A1 order by tag, A1 limit 1");
        p = pns.get(0).getChild(0);

        assertTrue(p instanceof LimitPlanNode);
        assertTrue(p.getChild(0) instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof OrderByPlanNode);
        assertTrue(p.getChild(0).getChild(0).getChild(0) instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        assertTrue(p.getChild(0) instanceof AbstractScanPlanNode);
    }

    public void testComplexAggwithLimit() {
        pns = compileToFragments("SELECT A1, sum(A1), sum(A1)+11 FROM P1 GROUP BY A1 ORDER BY A1 LIMIT 2");
        checkHasComplexAgg(pns);

        // Test limit push down
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0) instanceof LimitPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof OrderByPlanNode);
        assertTrue(p.getChild(0).getChild(0).getChild(0) instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof LimitPlanNode);
        assertTrue(p.getChild(0) instanceof OrderByPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof AggregatePlanNode);
    }

    public void testComplexAggwithDistinct() {
        pns = compileToFragments("SELECT A1, sum(A1), sum(distinct A1)+11 FROM P1 GROUP BY A1 ORDER BY A1");
        checkHasComplexAgg(pns);

        // Test aggregation node not push down with distinct
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0) instanceof OrderByPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
    }

    public void testComplexAggwithLimitDistinct() {
        pns = compileToFragments("SELECT A1, sum(A1), sum(distinct A1)+11 FROM P1 GROUP BY A1 ORDER BY A1 LIMIT 2");
        checkHasComplexAgg(pns);

        // Test no limit push down
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0) instanceof LimitPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof OrderByPlanNode);
        assertTrue(p.getChild(0).getChild(0).getChild(0) instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
    }

    public void testComplexAggCase() {
        pns = compileToFragments("SELECT A1, sum(A1), sum(A1)+11 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);

        pns = compileToFragments("SELECT A1, SUM(PKEY) as A2, (SUM(PKEY) / 888) as A3, (SUM(PKEY) + 1) as A4 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);

        pns = compileToFragments("SELECT A1, SUM(PKEY), COUNT(PKEY), (AVG(PKEY) + 1) as A4 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);
    }

    public void testComplexGroupby() {
        pns = compileToFragments("SELECT A1, ABS(A1), ABS(A1)+1, sum(B1) FROM P1 GROUP BY A1, ABS(A1)");
        checkHasComplexAgg(pns);

        // Check it can compile
        pns = compileToFragments("SELECT ABS(A1), sum(B1) FROM P1 GROUP BY ABS(A1)");
        AbstractPlanNode p = pns.get(0).getChild(0);
        //
        assertTrue(p instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        assertTrue(p.getChild(0) instanceof AbstractScanPlanNode);

        pns = compileToFragments("SELECT A1+PKEY, avg(B1) as tag FROM P1 GROUP BY A1+PKEY ORDER BY ABS(tag), A1+PKEY");
        checkHasComplexAgg(pns);
    }


    public void testUnOptimizedAVG() {
        pns = compileToFragments("SELECT AVG(A1) FROM R1");
        checkOptimizedAgg(pns, false);

        pns = compileToFragments("SELECT A1, AVG(PKEY) FROM R1 GROUP BY A1");
        checkOptimizedAgg(pns, false);

        pns = compileToFragments("SELECT A1, AVG(PKEY)+1 FROM R1 GROUP BY A1");
        checkHasComplexAgg(pns);
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0) instanceof AggregatePlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof AbstractScanPlanNode);
    }

    public void testOptimizedAVG() {
        pns = compileToFragments("SELECT AVG(A1) FROM P1");
        checkHasComplexAgg(pns);
        checkOptimizedAgg(pns, true);

        pns = compileToFragments("SELECT A1, AVG(PKEY) FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);
        // Test avg pushed down by replacing it with sum, count
        checkOptimizedAgg(pns, true);

        pns = compileToFragments("SELECT A1, AVG(PKEY)+1 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);
        // Test avg pushed down by replacing it with sum, count
        checkOptimizedAgg(pns, true);
    }

    public void testGroupbyColsNotInDisplayCols() {
        pns = compileToFragments("SELECT sum(PKEY) FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);

        pns = compileToFragments("SELECT sum(PKEY), sum(PKEY) FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);
    }

    public void testMultiPartitionMVBasedQuery_NoAggQuery() {
//        CREATE VIEW V_P1 (V_A1, V_B1, V_CNT, V_SUM_C1, V_SUM_D1)
//        AS SELECT A1, B1, COUNT(*), SUM(C1), COUNT(D1)
//        FROM P1  GROUP BY A1, B1;

        String[] tbs = {"V_P1", "V_P1_ABS"};
        for (String tb: tbs) {
            pns = compileToFragments("SELECT * FROM " + tb);
            checkMVFix_NoTopAgg(pns, 2, 3);

            pns = compileToFragments("SELECT * FROM " + tb + " order by V_A1");
            checkMVFix_NoTopAgg(pns, 2, 3);

            pns = compileToFragments("SELECT * FROM " + tb + " order by V_A1, V_B1");
            checkMVFix_NoTopAgg(pns, 2, 3);

            pns = compileToFragments("SELECT * FROM " + tb + " order by V_SUM_D1");
            checkMVFix_NoTopAgg(pns, 2, 3);

            pns = compileToFragments("SELECT * FROM " + tb + " limit 1");
            checkMVFix_NoTopAgg(pns, 2, 3);

            pns = compileToFragments("SELECT * FROM " + tb + " order by V_A1, V_B1 limit 1");
            checkMVFix_NoTopAgg(pns, 2, 3);

            pns = compileToFragments("SELECT v_sum_c1 FROM " + tb + "");
            checkMVFix_NoTopAgg(pns, 2, 1);

            pns = compileToFragments("SELECT v_sum_c1 FROM " + tb + " order by v_sum_c1");
            checkMVFix_NoTopAgg(pns, 2, 1);

            pns = compileToFragments("SELECT v_sum_c1 FROM " + tb + " order by v_sum_d1");
            checkMVFix_NoTopAgg(pns, 2, 2);

            pns = compileToFragments("SELECT v_sum_c1 FROM " + tb + " limit 1");
            checkMVFix_NoTopAgg(pns, 2, 1);

            pns = compileToFragments("SELECT v_sum_c1 FROM " + tb + " order by v_sum_c1 limit 1");
            checkMVFix_NoTopAgg(pns, 2, 1);

            // test distinct down.
            pns = compileToFragments("SELECT distinct v_sum_c1 FROM " + tb + " limit 1");
            checkMVFix_NoTopAgg(pns, 2, 1, true);
        }
    }


    public void testMultiPartitionMVBasedQuery_AggQueryEdge() {
        try {
            pns = compileToFragments("SELECT count(V_SUM_C1) FROM V_P1");
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("group by query or aggregation on materialized table"));
        }

    }

    public void notestMultiPartitionMVBasedQuery_AggQuery() {
//      CREATE VIEW V_P1 (V_A1, V_B1, V_CNT, V_SUM_C1, V_SUM_D1)
//      AS SELECT A1, B1, COUNT(*), SUM(C1), COUNT(D1)
//      FROM P1  GROUP BY A1, B1;

        String[] tbs = {"V_P1", "V_P1_ABS"};

        for (String tb: tbs) {
            // Test set (1)
            pns = compileToFragments("SELECT V_SUM_C1 FROM " + tb +
                    " GROUP by V_SUM_C1");
            checkMVFix_TopAgg_ReAgg(pns, 1, 0, 2, 1);

            pns = compileToFragments("SELECT V_SUM_C1 FROM " + tb +
                    " GROUP by V_SUM_C1 ORDER BY V_SUM_C1");
            checkMVFix_TopAgg_ReAgg(pns, 1, 0, 2, 1);

            pns = compileToFragments("SELECT V_SUM_C1 FROM " + tb + " GROUP by V_SUM_C1 " +
                    "ORDER BY V_SUM_C1 LIMIT 5");
            checkMVFix_TopAgg_ReAgg(pns, 1, 0, 2, 1);

            pns = compileToFragments("SELECT V_SUM_C1 FROM " + tb +
                    " GROUP by V_SUM_C1 LIMIT 5");
            checkMVFix_TopAgg_ReAgg(pns, 1, 0, 2, 1);

            pns = compileToFragments("SELECT distinct V_SUM_C1 FROM " + tb +
                    " GROUP by V_SUM_C1 LIMIT 5");
            checkMVFix_TopAgg_ReAgg(pns, 1, 0, 2, 1);

            // Test set (2)
            pns = compileToFragments("SELECT V_SUM_C1, sum(V_CNT) FROM " + tb +
                    " GROUP by V_SUM_C1");
            checkMVFix_TopAgg_ReAgg(pns, 1, 1, 2, 2);

            pns = compileToFragments("SELECT V_SUM_C1, sum(V_CNT) FROM " + tb +
                    " GROUP by V_SUM_C1 ORDER BY V_SUM_C1");
            checkMVFix_TopAgg_ReAgg(pns, 1, 1, 2, 2);

            pns = compileToFragments("SELECT V_SUM_C1, sum(V_CNT) FROM " + tb +
                    " GROUP by V_SUM_C1 ORDER BY V_SUM_C1 limit 2");
            checkMVFix_TopAgg_ReAgg(pns, 1, 1, 2, 2);

            // Distinct: No aggregation push down.
            pns = compileToFragments("SELECT V_SUM_C1, sum(distinct V_CNT) " +
                    "FROM " + tb + " GROUP by V_SUM_C1 ORDER BY V_SUM_C1");
            checkMVFix_TopAgg_ReAgg(pns, 1, 1, 2, 2);


            // Test set (3)
            pns = compileToFragments("SELECT V_A1,V_B1, V_SUM_C1, sum(V_SUM_D1) FROM " + tb +
                    " GROUP BY V_A1,V_B1, V_SUM_C1");
            checkMVFix_TopAgg_ReAgg(pns, 1, 1, 2, 2);

            pns = compileToFragments("SELECT V_A1,V_B1, V_SUM_C1, sum(V_SUM_D1) FROM " + tb +
                    " GROUP BY V_A1,V_B1, V_SUM_C1 ORDER BY V_A1,V_B1, V_SUM_C1");
            checkMVFix_TopAgg_ReAgg(pns, 1, 1, 2, 2);

            pns = compileToFragments("SELECT V_A1,V_B1, V_SUM_C1, sum(V_SUM_D1) FROM " + tb +
                    " GROUP BY V_A1,V_B1, V_SUM_C1 ORDER BY V_A1,V_B1, V_SUM_C1 LIMIT 5");
            checkMVFix_TopAgg_ReAgg(pns, 1, 1, 2, 2);

            pns = compileToFragments("SELECT V_A1,V_B1, V_SUM_C1, sum(V_SUM_D1) FROM " + tb +
                    " GROUP BY V_A1,V_B1, V_SUM_C1 ORDER BY V_A1, V_SUM_C1 LIMIT 5");
            checkMVFix_TopAgg_ReAgg(pns, 1, 1, 2, 2);

            // Distinct: No aggregation push down.
            pns = compileToFragments("SELECT V_A1,V_B1, V_SUM_C1, sum( distinct V_SUM_D1) FROM " +
                    tb + " GROUP BY V_A1,V_B1, V_SUM_C1 ORDER BY V_A1, V_SUM_C1 LIMIT 5");
            checkMVFix_TopAgg_ReAgg(pns, 1, 1, 2, 2);
        }
    }

    public void testMultiPartitionMVBasedQuery_NoFix() {
        // Normal select queries
        pns = compileToFragments("SELECT * FROM V_P1_TEST1");
        checkMVNoFix_NoAgg(pns, false);

        pns = compileToFragments("SELECT V_SUM_C1 FROM V_P1_TEST1 ORDER BY V_A1");
        checkMVNoFix_NoAgg(pns, false);

        pns = compileToFragments("SELECT V_SUM_C1 FROM V_P1_TEST1 LIMIT 1");
        checkMVNoFix_NoAgg(pns, false);

        pns = compileToFragments("SELECT DISTINCT V_SUM_C1 FROM V_P1_TEST1");
        checkMVNoFix_NoAgg(pns, true);

        // Distributed group by query
        pns = compileToFragments("SELECT V_SUM_C1 FROM V_P1_TEST1 GROUP by V_SUM_C1");
        checkMVNoFix_Agg(pns, 1, 0, false);

        pns = compileToFragments("SELECT V_SUM_C1, sum(V_CNT) FROM V_P1_TEST1 " +
                "GROUP by V_SUM_C1");
        checkMVNoFix_Agg(pns, 1, 1, false);
    }

    public void testMultiPartitionMVBasedQuery_Where() {
        try {
            pns = compileToFragments("SELECT * FROM V_P1 where v_a1 = 1");
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("has filter on the table"));
        }
    }

    private void checkMVNoFix_NoAgg(List<AbstractPlanNode> pns,
            boolean distinctPushdown) {
        // the first '-1' indicates that there is no top aggregation node.

        checkMVReaggreateFeature(pns, -1, -1, -1, -1,
                distinctPushdown, false, false, true);
    }

    private void checkMVNoFix_Agg(List<AbstractPlanNode> pns,
            int numGroupbyOfTopAggNode, int numAggsOfTopAggNode,
            boolean distinctPushdown) {
        // the first '-1' indicates that there is no top aggregation node.
        checkMVReaggreateFeature(pns,
                numGroupbyOfTopAggNode, numAggsOfTopAggNode,
                -1, -1,
                distinctPushdown, false, true, false);
    }

    private void checkMVFix_NoTopAgg(List<AbstractPlanNode> pns,
            int numGroupbyOfReaggNode, int numAggsOfReaggNode) {
        // the first '-1' indicates that there is no top aggregation node.
        checkMVReaggreateFeature(pns,
                -1, -1,
                numGroupbyOfReaggNode, numAggsOfReaggNode,
                false, true, false, true);
    }

    private void checkMVFix_NoTopAgg(List<AbstractPlanNode> pns,
            int numGroupbyOfReaggNode, int numAggsOfReaggNode, boolean distinct) {
        // the first '-1' indicates that there is no top aggregation node.
        checkMVReaggreateFeature(pns,
                -1, -1,
                numGroupbyOfReaggNode, numAggsOfReaggNode,
                distinct, true, false, true);
    }

    private void checkMVFix_TopAgg_ReAgg(List<AbstractPlanNode> pns,
            int numGroupbyOfTopAggNode, int numAggsOfTopAggNode,
            int numGroupbyOfReaggNode, int numAggsOfReaggNode) {

        checkMVReaggreateFeature(pns,
                numGroupbyOfTopAggNode, numAggsOfTopAggNode,
                numGroupbyOfReaggNode, numAggsOfReaggNode,
                false, true, false, true);
    }

    // topNode, reAggNode
    private void checkMVReaggreateFeature(List<AbstractPlanNode> pns,
            int numGroupbyOfTopAggNode, int numAggsOfTopAggNode,
            int numGroupbyOfReaggNode, int numAggsOfReaggNode,
            boolean distinctPushdown, boolean needFix,
            boolean aggPushdown, boolean projectionNode ) {

        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
        assertTrue(pns.size() == 2);
        AbstractPlanNode p = pns.get(0);
        assertTrue(p instanceof SendPlanNode);
        p = p.getChild(0);

        if (p instanceof LimitPlanNode) {
            // No limit pushed down.
            p = p.getChild(0);
        }
        if (projectionNode) {
            assertTrue(p instanceof ProjectionPlanNode);
            p = p.getChild(0);
        }

        if (p instanceof OrderByPlanNode) {
            p = p.getChild(0);
        }
        if (p instanceof DistinctPlanNode) {
            p = p.getChild(0);
        }

        HashAggregatePlanNode reAggNode = null;

        // Indicates that there is no top aggregation node.
        if (numGroupbyOfTopAggNode == -1 ) {
            if (needFix) {
                assertTrue(p instanceof HashAggregatePlanNode);
                reAggNode = (HashAggregatePlanNode) p;

                assertEquals(numGroupbyOfReaggNode, reAggNode.getGroExpressionTypes().size());
                assertEquals(numAggsOfReaggNode, reAggNode.getAggregateTypes().size());

                for (ExpressionType type: reAggNode.getAggregateTypes()) {
                    assertEquals(ExpressionType.AGGREGATE_SUM, type);
                }
                p = p.getChild(0);
            }
            assertTrue(p instanceof ReceivePlanNode);

            p = pns.get(1);
            assertTrue(p instanceof SendPlanNode);
            p = p.getChild(0);

            if (distinctPushdown) {
                assertTrue(p instanceof DistinctPlanNode);
                p = p.getChild(0);
            }
            assertTrue(p instanceof AbstractScanPlanNode);
        } else {
            assertTrue(p instanceof AggregatePlanNode);
            AggregatePlanNode topAggNode = (AggregatePlanNode) p;

            assertEquals(numGroupbyOfTopAggNode, topAggNode.getGroExpressionTypes().size());
            assertEquals(numAggsOfTopAggNode, topAggNode.getAggregateTypes().size());
            p = p.getChild(0);

            if (needFix) {
                assertTrue(p instanceof HashAggregatePlanNode);
                reAggNode = (HashAggregatePlanNode) p;

                assertEquals(numGroupbyOfReaggNode, reAggNode.getGroExpressionTypes().size());
                assertEquals(numAggsOfReaggNode, reAggNode.getAggregateTypes().size());

                for (ExpressionType type: reAggNode.getAggregateTypes()) {
                    assertEquals(ExpressionType.AGGREGATE_SUM, type);
                }
                p = p.getChild(0);
            }
            assertTrue(p instanceof ReceivePlanNode);

            // Test the second part
            p = pns.get(1);
            assertTrue(p instanceof SendPlanNode);
            p = p.getChild(0);

            if (aggPushdown) {
                assertTrue(!needFix);
                assertTrue(p instanceof AggregatePlanNode);
                p = p.getChild(0);
            }

            assertTrue(p instanceof AbstractScanPlanNode);
        }
    }


    private void checkHasComplexAgg(List<AbstractPlanNode> pns) {
        assertTrue(pns.size() > 0);
        boolean isDistributed = pns.size() > 1 ? true: false;

        for ( AbstractPlanNode nd : pns) {
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
        }

        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        while ( p.getChildCount() > 0) {
            p = p.getChild(0);
            assertFalse(p instanceof ProjectionPlanNode);
        }

        if (isDistributed) {
            p = pns.get(1).getChild(0);
            assertFalse(p instanceof ProjectionPlanNode);
        }
    }

    private void checkOptimizedAgg (List<AbstractPlanNode> pns, boolean optimized) {
        AbstractPlanNode p = pns.get(0).getChild(0);
        if (optimized) {
            assertTrue(p instanceof ProjectionPlanNode);
            assertTrue(p.getChild(0) instanceof AggregatePlanNode);

            p = pns.get(1).getChild(0);
            // push down for optimization
            assertTrue(p instanceof AggregatePlanNode);
            assertTrue(p.getChild(0) instanceof AbstractScanPlanNode);
        } else {
            assertTrue(pns.size() == 1);
            assertTrue(p instanceof AggregatePlanNode);
            assertTrue(p.getChild(0) instanceof AbstractScanPlanNode);
        }
    }
}
