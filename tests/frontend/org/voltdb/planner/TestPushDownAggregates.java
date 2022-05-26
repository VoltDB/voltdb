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

import java.util.List;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.TableCountPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

public class TestPushDownAggregates extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(getClass().getResource("testplans-groupby-ddl.sql"),
                    "testpushdownaggregates", false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testCountOnPartitionedTable() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(A1) from T1");
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT},
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM});
    }

    public void testSumOnPartitionedTable() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT sum(A1) from T1");
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM},
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM});
    }

    public void testMinOnPartitionedTable() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(A1) from T1");
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_MIN},
                        new ExpressionType[] {ExpressionType.AGGREGATE_MIN});
    }

    public void testMaxOnPartitionedTable() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MAX(A1) from T1");
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_MAX},
                        new ExpressionType[] {ExpressionType.AGGREGATE_MAX});
    }

    public void testAvgOnPartitionedTable() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT AVG(A1) from T1");
        for (AbstractPlanNode apn: pn) {
            System.out.println(apn.toExplainPlanString());
        }
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_COUNT},
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_SUM},
                        true);
    }

    public void testCountStarWithGroupBy() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT A1, count(*) FROM T1 GROUP BY A1");
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT_STAR},
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM});
    }

    public void testDistinct() {
        // On partitioned table
        String sql1,sql2;
        sql1 = "SELECT DISTINCT A1 from T1";
        sql2 = "SELECT A1 from T1 group by A1";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // On replicated table
        sql1 = "SELECT DISTINCT D1_NAME from D1";
        sql2 = "SELECT D1_NAME from D1 group by D1_NAME";
        checkQueriesPlansAreTheSame(sql1, sql2);
    }

   public void testAllPushDownAggregates() {
        List<AbstractPlanNode> pn =
                compileToFragments("SELECT A1, count(*), count(PKEY), sum(PKEY), min(PKEY), max(PKEY)" +
                    " FROM T1 GROUP BY A1");
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT_STAR,
                                              ExpressionType.AGGREGATE_COUNT,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_MIN,
                                              ExpressionType.AGGREGATE_MAX},
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_MIN,
                                              ExpressionType.AGGREGATE_MAX});
    }

    //ENG-4980
    public void testAggregatesOnDistinctPKey() {
        List<AbstractPlanNode> pn =
                compileToFragments("SELECT count(distinct PKEY), sum(distinct PKEY), min(distinct PKEY), max(distinct PKEY), avg(distinct PKEY)" +
                    " FROM T1;");
        for (AbstractPlanNode apn: pn) {
            System.out.println(apn.toExplainPlanString());
        }
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_MIN,
                                              ExpressionType.AGGREGATE_MAX,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_COUNT},
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_MIN,
                                              ExpressionType.AGGREGATE_MAX,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_SUM},
                                              true);

        // Test count(distinct pkey) with other compatible aggs like non-distinct aggs of other columns.
        pn = compileToFragments("SELECT count(distinct PKEY), count(A1), min(distinct PKEY), max(distinct A1), sum(A1)" +
                    " FROM T1;");
        for (AbstractPlanNode apn: pn) {
            System.out.println(apn.toExplainPlanString());
        }
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT,
                                              ExpressionType.AGGREGATE_COUNT,
                                              ExpressionType.AGGREGATE_MIN,
                                              ExpressionType.AGGREGATE_MAX,
                                              ExpressionType.AGGREGATE_SUM},
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_MIN,
                                              ExpressionType.AGGREGATE_MAX,
                                              ExpressionType.AGGREGATE_SUM});

        // Negative test case: count(distinct pkey) and count(distinct other) to show that
        // it only takes one other non-trivial distinct on another column to disable the pushdown.
        pn = compileToFragments("SELECT count(distinct PKEY), count(distinct A1) FROM T1;");
        assertTrue(pn.size() == 2);
        assertTrue(pn.get(1).getChild(0) instanceof AbstractScanPlanNode);
        AbstractScanPlanNode asp = (AbstractScanPlanNode)pn.get(1).getChild(0);
        assertTrue(asp.getInlinePlanNode(PlanNodeType.AGGREGATE) == null);
    }

    public void testAllAggregates() {
        List<AbstractPlanNode> pn =
            compileToFragments("SELECT count(*), count(PKEY), sum(PKEY), min(PKEY), max(PKEY), avg(PKEY) FROM T1");
        for (AbstractPlanNode apn: pn) {
            System.out.println(apn.toExplainPlanString());
        }
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT_STAR,
                                              ExpressionType.AGGREGATE_COUNT,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_MIN,
                                              ExpressionType.AGGREGATE_MAX},
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_MIN,
                                              ExpressionType.AGGREGATE_MAX},
                        true);
    }

    public void testAllAggregatesAVG1() {
        List<AbstractPlanNode> pn =
            compileToFragments("SELECT count(*), count(PKEY), min(PKEY), max(PKEY), avg(PKEY) FROM T1");
        for (AbstractPlanNode apn: pn) {
            System.out.println(apn.toExplainPlanString());
        }
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT_STAR,
                                              ExpressionType.AGGREGATE_COUNT,
                                              ExpressionType.AGGREGATE_MIN,
                                              ExpressionType.AGGREGATE_MAX,
                                              ExpressionType.AGGREGATE_SUM},
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_MIN,
                                              ExpressionType.AGGREGATE_MAX,
                                              ExpressionType.AGGREGATE_SUM},
                        true);
    }

    public void testAllAggregatesAVG2() {
        List<AbstractPlanNode> pn =
            compileToFragments("SELECT count(*), min(PKEY), max(PKEY), avg(PKEY) FROM T1");
        for (AbstractPlanNode apn: pn) {
            System.out.println(apn.toExplainPlanString());
        }
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT_STAR,
                                              ExpressionType.AGGREGATE_MIN,
                                              ExpressionType.AGGREGATE_MAX,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_COUNT},
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_MIN,
                                              ExpressionType.AGGREGATE_MAX,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_SUM},
                        true);
    }

    public void testGroupByNotInDisplayColumn() {
        List<AbstractPlanNode> pn = compileToFragments ("SELECT count(A1) FROM T1 GROUP BY A1");
        checkPushedDown(pn, true,
                new ExpressionType[] {ExpressionType.AGGREGATE_COUNT},
                new ExpressionType[] {ExpressionType.AGGREGATE_SUM}, true);

    }

    public void testGroupByWithoutAggregates() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT A1 FROM T1 GROUP BY A1");
        assertFalse(pn.get(0).findAllNodesOfType(PlanNodeType.HASHAGGREGATE).isEmpty());
        // We used to be careful to send raw rows to the coordinator instead of first
        // de-duping them into groups -- solely because there were no aggregation functions
        // requested. What did that have to do with anything? Assert that we're past that phase.
        assertFalse(pn.get(1).findAllNodesOfType(PlanNodeType.HASHAGGREGATE).isEmpty());
    }

    public void testCountDistinct() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT count(distinct A1) from T1");
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT},
                        null);
    }

    public void testSumDistinct() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT sum(distinct A1) from T1");
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM},
                        null);
    }

    //TODO: Not sure what this has to do with PushDownAggregates -- move this test case?
    public void testSinglePartOffset()
    {
        AbstractPlanNode pn = compile("select PKEY from T1 order by PKEY limit 5 offset 1");
        assertTrue(pn.toExplainPlanString().contains("LIMIT"));
    }

    public void testMultiPartOffset() {
        List<AbstractPlanNode> pn = compileToFragments("select PKEY from T1 order by PKEY limit 5 offset 1");
        assertEquals(2, pn.size());
        assertTrue(pn.get(0).toExplainPlanString().contains("LIMIT"));
        assertTrue(pn.get(1).toExplainPlanString().contains("LIMIT"));
    }

    public void testLimit() {
        List<AbstractPlanNode> pn = compileToFragments("select PKEY from T1 order by PKEY limit 5");
        PlanNodeList pnl = new PlanNodeList(pn.get(0), false);
        System.out.println(pnl.toDOTString("FRAG0"));
        pnl = new PlanNodeList(pn.get(1), false);
        System.out.println(pnl.toDOTString("FRAG1"));
    }

    public void testMultiPartLimitPushdown() {
        List<AbstractPlanNode> pns;
        // push down the limit because of the order by
        pns = compileToFragments("select A1, count(*) as tag from T1 group by A1 order by A1 limit 1");
        checkLimitPushedDown(pns, true);

        // order by does not cover all group by columns
        pns = compileToFragments("select B3, C3, count(*) as tag from T3 group by B3, C3 order by B3 limit 1");
        checkLimitPushedDown(pns, false);


        // T1 is partitioned on PKEY column
        pns = compileToFragments("select A1, count(*) as tag from T1 group by A1 order by tag limit 1");
        checkLimitPushedDown(pns, false);

        pns = compileToFragments("select A1, count(*) as tag from T1 group by A1 order by tag+1 limit 1");
        checkLimitPushedDown(pns, false);

        pns = compileToFragments("select A1, count(*) as tag from T1 group by A1 order by count(*)+1 limit 1");
        checkLimitPushedDown(pns, false);

        pns = compileToFragments("select A1, count(*) as tag from T1 group by A1 order by A1, count(*)+1 limit 1");
        checkLimitPushedDown(pns, false);

        pns = compileToFragments("select A1, count(*) as tag from T1 group by A1 order by A1, ABS(count(*)+1) limit 1");
        checkLimitPushedDown(pns, false);

        //
        // T3 is partitioned on A3 column: group by partition column is another story
        //
        pns = compileToFragments("select A3, count(*) as tag from T3 group by A3 order by tag limit 1");
        checkLimitPushedDown(pns, true);

        // function on partition column
        pns = compileToFragments("select ABS(A3), count(*) as tag from T3 group by ABS(A3) order by tag limit 1");
        checkLimitPushedDown(pns, false);


        // Add a replicate table to test
        pns = compileToFragments("select A1, count(*) as tag from R1 group by A1 order by tag limit 1");
        assertEquals(1, pns.size());
        assertTrue(pns.get(0).toExplainPlanString().contains("LIMIT"));

        //
        // Partition table join, limit push down
        //
        pns = compileToFragments("select A3, B4, count(A3) as tag from T3, T4 WHERE A3 = A4 " +
                "group by A3, B4 order by tag desc limit 10");
        checkLimitPushedDown(pns, true);

        pns = compileToFragments("select A3, B4, count(A3) as tag from T3, T4 WHERE A3 = A4 " +
                "group by A3, B4 order by tag+1 desc limit 10");
        checkLimitPushedDown(pns, true);

        // One of the partition table is from sub-query
        pns = compileToFragments("select A3, B4, count(CT) as tag " +
                " from (select A3, count(*) CT from T3 GROUP BY A3) TEMP, T4 WHERE A3 = A4 " +
                "group by A3, B4 order by tag desc limit 10");
        checkLimitPushedDown(pns, true);

    }

    private void checkLimitPushedDown(List<AbstractPlanNode> pn, boolean pushdown) {
        assertEquals(2, pn.size());
        // inline limit with order by node
        assertTrue(pn.get(0).toExplainPlanString().contains("inline LIMIT"));
        if (pushdown) {
            assertTrue(pn.get(1).toExplainPlanString().contains("inline LIMIT"));
        } else {
            assertFalse(pn.get(1).toExplainPlanString().contains("inline LIMIT"));
        }
    }

    public void testMultiPartNoLimitPushdown() {
        List<AbstractPlanNode> pn =
                compileToFragments("select A1, count(*) as tag from T1 group by A1 order by tag, A1 limit 1");

        for ( AbstractPlanNode nd : pn) {
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
        }
        assertTrue(pn.get(0).toExplainPlanString().contains("LIMIT"));
        assertFalse(pn.get(1).toExplainPlanString().contains("LIMIT"));
    }

    public void testMultiPartNoLimitPushdownByTwo() {
        List<AbstractPlanNode> pn =
                compileToFragments("select A1, count(*) as tag from T1 group by A1 order by 2 limit 1");

        for ( AbstractPlanNode nd : pn) {
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
        }
        assertTrue(pn.get(0).toExplainPlanString().contains("LIMIT"));
        assertFalse(pn.get(1).toExplainPlanString().contains("LIMIT"));
    }

    /**
     * Check if the aggregate node is pushed-down in the given plan. If the
     * pushDownTypes is null, it assumes that the aggregate node should NOT be
     * pushed-down.
     *
     * @param np
     *            The generated plan
     * @param isAggInlined
     *            Whether or not the aggregate node can be inlined
     * @param aggTypes
     *            The expected aggregate types for the original aggregate node.
     * @param pushDownTypes
     *            The expected aggregate types for the top aggregate node after
     *            pushing the original aggregate node down.
     */
    private void checkPushedDown(List<AbstractPlanNode> pn, boolean isAggInlined,
                                 ExpressionType[] aggTypes, ExpressionType[] pushDownTypes) {
        checkPushedDown(pn, isAggInlined, aggTypes, pushDownTypes, false);
    }

    private void checkPushedDown(List<AbstractPlanNode> pn, boolean isAggInlined,
            ExpressionType[] aggTypes, ExpressionType[] pushDownTypes,
            boolean hasProjectionNode) {

        // Aggregate push down check has to run on two fragments
        assertTrue(pn.size() == 2);

        AbstractPlanNode p = pn.get(0).getChild(0);;
        if (hasProjectionNode) {
            // Complex aggregation or optimized AVG
            assertTrue(p instanceof ProjectionPlanNode);
            p = p.getChild(0);
        }

        assertTrue(p instanceof AggregatePlanNode);
        String fragmentString = p.toJSONString();
        ExpressionType[] topTypes = (pushDownTypes != null) ? pushDownTypes : aggTypes;
        for (ExpressionType type : topTypes) {
            assertTrue(fragmentString.contains("\"AGGREGATE_TYPE\":\"" + type.toString() + "\""));
        }

        // Check the pushed down aggregation
        p = pn.get(1).getChild(0);

        if (pushDownTypes == null) {
            assertTrue(p instanceof AbstractScanPlanNode);
            return;
        }

        if (isAggInlined) {
            // If it's table count, it won't have inline plan node.
            // See ENG-6131
            if(p instanceof TableCountPlanNode) {
                return;
            }
            assertTrue(p instanceof AbstractScanPlanNode);
            assertTrue(p.getInlinePlanNode(PlanNodeType.AGGREGATE) != null ||
                    p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);

            if (p.getInlinePlanNode(PlanNodeType.AGGREGATE) != null) {
                p  = p.getInlinePlanNode(PlanNodeType.AGGREGATE);
            } else {
                p  = p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
            }

        } else {
            assertTrue(p instanceof AggregatePlanNode);
        }
        fragmentString = p.toJSONString();
        for (ExpressionType type : aggTypes) {
            assertTrue(fragmentString.contains("\"AGGREGATE_TYPE\":\"" + type.toString() + "\""));
        }
    }

}
