/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import junit.framework.TestCase;

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.DistinctPlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

public class TestPushDownAggregates extends TestCase {
    private PlannerTestAideDeCamp aide;

    private List<AbstractPlanNode> compile(String sql, int paramCount,
                                     boolean singlePartition)
    {
        List<AbstractPlanNode> pn = null;
        try {
            pn = aide.compile(sql, paramCount, singlePartition);
        }
        catch (PlanningErrorException ex) {
            throw ex;
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        assertTrue(pn != null);
        assertFalse(pn.isEmpty());
        return pn;
    }

    @Override
    protected void setUp() throws Exception {
        aide = new PlannerTestAideDeCamp(getClass().getResource("testplans-groupby-ddl.sql"),
                                         "testpushdownaggregates");

        // Set all tables except for D1 to non-replicated.
        Cluster cluster = aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            if (!t.getTypeName().equalsIgnoreCase("d1")) {
                t.setIsreplicated(false);
            }
            // partition column PKEY for Table t2
            if (!t.getTypeName().equalsIgnoreCase("t2")) {
                t.setIsreplicated(false);
                t.setPartitioncolumn(t.getColumns().get("PKEY"));
            }
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }

    public void testCountOnPartitionedTable() {
        List<AbstractPlanNode> pn = compile("SELECT count(A1) from T1", 0, false);
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT},
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM});
    }

    public void testSumOnPartitionedTable() {
        List<AbstractPlanNode> pn = compile("SELECT sum(A1) from T1", 0, false);
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM},
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM});
    }

    public void testMinOnPartitionedTable() {
        List<AbstractPlanNode> pn = compile("SELECT MIN(A1) from T1", 0, false);
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_MIN},
                        new ExpressionType[] {ExpressionType.AGGREGATE_MIN});
    }

    public void testMaxOnPartitionedTable() {
        List<AbstractPlanNode> pn = compile("SELECT MAX(A1) from T1", 0, false);
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_MAX},
                        new ExpressionType[] {ExpressionType.AGGREGATE_MAX});
    }

    public void testAvgOnPartitionedTable() {
        List<AbstractPlanNode> pn = compile("SELECT AVG(A1) from T1", 0, false);
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_AVG},
                        null);
    }

    public void testCountStarWithGroupBy() {
        List<AbstractPlanNode> pn = compile("SELECT A1, count(*) FROM T1 GROUP BY A1", 0, false);
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT_STAR},
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM});
    }

    public void testDistinctOnPartitionedTable() {
        List<AbstractPlanNode> pn = compile("SELECT DISTINCT A1 from T1", 0, false);
        checkPushedDownDistinct(pn, true);
    }

    public void testDistinctOnReplicatedTable() {
        List<AbstractPlanNode> pn = compile("SELECT DISTINCT D1_NAME from D1", 0, false);
        checkPushedDownDistinct(pn, false);
    }

   public void testAllPushDownAggregates() {
        List<AbstractPlanNode> pn =
            compile("SELECT A1, count(*), count(PKEY), sum(PKEY), min(PKEY), max(PKEY)" +
                    " FROM T1 GROUP BY A1", 0, false);
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

    public void testAllAggregates() {
        List<AbstractPlanNode> pn =
            compile("SELECT count(*), count(PKEY), sum(PKEY), min(PKEY), max(PKEY), avg(PKEY)" +
                    " FROM T1", 0, false);
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT_STAR,
                                              ExpressionType.AGGREGATE_COUNT,
                                              ExpressionType.AGGREGATE_SUM,
                                              ExpressionType.AGGREGATE_MIN,
                                              ExpressionType.AGGREGATE_MAX,
                                              ExpressionType.AGGREGATE_AVG},
                        null);
    }

    public void testGroupByNotInDisplayColumn() {
        try {
            compile("SELECT count(A1) FROM T1 GROUP BY A1", 0, false);
        } catch (PlanningErrorException e) {
            // There shouldn't be any plan
            return;
        }
        fail("This statement should not generate any plan.");
    }

    public void testGroupByWithoutAggregates() {
        List<AbstractPlanNode> pn = compile("SELECT A1 FROM T1 GROUP BY A1", 0, false);
        assertFalse(pn.get(0).findAllNodesOfType(PlanNodeType.HASHAGGREGATE).isEmpty());
        assertTrue(pn.get(1).findAllNodesOfType(PlanNodeType.HASHAGGREGATE).isEmpty());
    }

    public void testCountDistinct() {
        List<AbstractPlanNode> pn = compile("SELECT count(distinct A1) from T1", 0, false);
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT},
                        null);
    }

    public void testSumDistinct() {
        List<AbstractPlanNode> pn = compile("SELECT sum(distinct A1) from T1", 0, false);
        checkPushedDown(pn, true,
                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM},
                        null);
    }

    public void testSinglePartOffset() {
        List<AbstractPlanNode> pn =
                compile("select PKEY from T1 order by PKEY limit 5 offset 1", 0, true);
        assertEquals(1, pn.size());
        assertTrue(pn.get(0).toExplainPlanString().contains("LIMIT"));
    }

    public void testMultiPartOffset() {
        List<AbstractPlanNode> pn =
                compile("select PKEY from T1 order by PKEY limit 5 offset 1", 0, false);
        assertEquals(2, pn.size());
        assertTrue(pn.get(0).toExplainPlanString().contains("LIMIT"));
        assertTrue(pn.get(1).toExplainPlanString().contains("LIMIT"));
    }

    public void testLimit() {
        List<AbstractPlanNode> pn = compile("select PKEY from T1 order by PKEY limit 5", 0, false);
        PlanNodeList pnl = new PlanNodeList(pn.get(0));
        System.out.println(pnl.toDOTString("FRAG0"));
        pnl = new PlanNodeList(pn.get(1));
        System.out.println(pnl.toDOTString("FRAG1"));
    }

    public void testMultiPartLimitPushdown() {
        List<AbstractPlanNode> pn =
                compile("select I, count(*) as tag from T2 group by I order by I limit 1", 0, false);

        for ( AbstractPlanNode nd : pn) {
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
            assertTrue(nd.toExplainPlanString().contains("LIMIT"));
        }
    }

    public void testMultiPartLimitPushdownByOne() {
        List<AbstractPlanNode> pn =
                compile("select I, count(*) as tag from T2 group by I order by 1 limit 1", 0, false);

        for ( AbstractPlanNode nd : pn) {
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
            assertTrue(nd.toExplainPlanString().contains("LIMIT"));
        }
    }

    public void testMultiPartNoLimitPushdown() {
        List<AbstractPlanNode> pn =
                compile("select I, count(*) as tag from T2 group by I order by tag, I limit 1", 0, false);

        for ( AbstractPlanNode nd : pn) {
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
        }
        assertTrue(pn.get(0).toExplainPlanString().contains("LIMIT"));
        assertFalse(pn.get(1).toExplainPlanString().contains("LIMIT"));
    }

    public void testMultiPartNoLimitPushdownByTwo() {
        List<AbstractPlanNode> pn =
                compile("select I, count(*) as tag from T2 group by I order by 2 limit 1", 0, false);

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
     * @param isMultiPart
     *            Whether or not the plan is distributed
     * @param aggTypes
     *            The expected aggregate types for the original aggregate node.
     * @param pushDownTypes
     *            The expected aggregate types for the top aggregate node after
     *            pushing the original aggregate node down.
     */
    private void checkPushedDown(List<AbstractPlanNode> pn, boolean isMultiPart,
                                 ExpressionType[] aggTypes, ExpressionType[] pushDownTypes) {
        assertTrue(pn.size() > 0);

        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof HashAggregatePlanNode);
        if (pushDownTypes != null) {
            for (ExpressionType type : pushDownTypes) {
                assertTrue(p.toJSONString().contains("\"AGGREGATE_TYPE\":\"" +
                                                     type.toString() + "\""));
            }
        } else {
            for (ExpressionType type : aggTypes) {
                assertTrue(p.toJSONString().contains("\"AGGREGATE_TYPE\":\"" +
                                                     type.toString() + "\""));
            }
        }

        if (isMultiPart) {
            assertTrue(pn.size() == 2);
            p = pn.get(1).getChild(0);
        } else {
            p = p.getChild(0);
        }

        if (pushDownTypes != null) {
            assertTrue(p instanceof HashAggregatePlanNode);
            for (ExpressionType type : aggTypes) {
                assertTrue(p.toJSONString().contains("\"AGGREGATE_TYPE\":\"" +
                                                     type.toString() + "\""));
            }
        } else {
            assertTrue(p instanceof AbstractScanPlanNode);
        }
    }

    /**
     * Check if the distinct node is pushed-down in the given plan.
     *
     * @param np
     *            The generated plan
     * @param isMultiPart
     *            Whether or not the plan is distributed
     */
    private void checkPushedDownDistinct(List<AbstractPlanNode> pn, boolean isMultiPart) {
        assertTrue(pn.size() > 0);

        AbstractPlanNode p = pn.get(0).getChild(0).getChild(0);
        assertTrue(p instanceof DistinctPlanNode);
        assertTrue(p.toJSONString().contains("\"DISTINCT\""));

        if (isMultiPart) {
            assertTrue(pn.size() == 2);
            p = pn.get(1).getChild(0);
            assertTrue(p instanceof DistinctPlanNode);
            assertTrue(p.toJSONString().contains("\"DISTINCT\""));
        }
    }
}
