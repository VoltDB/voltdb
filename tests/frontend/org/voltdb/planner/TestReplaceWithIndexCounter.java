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
import org.voltdb.plannodes.IndexCountPlanNode;
import org.voltdb.types.ExpressionType;

public class TestReplaceWithIndexCounter extends TestCase {
    private PlannerTestAideDeCamp aide;

    private List<AbstractPlanNode> compile(String sql, int paramCount,
                                     boolean singlePartition)
    {
        List<AbstractPlanNode> pn = null;
        try {
            pn = aide.compile(sql, paramCount, singlePartition);
        }
        catch (NullPointerException ex) {
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
        aide = new PlannerTestAideDeCamp(getClass().getResource("testplans-indexcounter-ddl.sql"),
                                         "testindexcounter");

        // Set all tables except for D1 to non-replicated.
        Cluster cluster = aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            if (!t.getTypeName().equalsIgnoreCase("d1")) {
                t.setIsreplicated(false);
            }
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }

    public void testCountStar1() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS > 3", 0, true);
        checkIndexCounter(pn, false,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT_STAR},
                        null);
    }

    public void testCountStar2() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS < ?", 0, true);
        checkIndexCounter(pn, false,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT_STAR},
                        null);
    }

    public void testCountStar3() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS > 3 AND POINTS <= 6", 0, true);
        checkIndexCounter(pn, false,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT_STAR},
                        null);
    }

    public void testCountStar4() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS > ? AND POINTS < ?", 2, true);
        checkIndexCounter(pn, false,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT_STAR},
                        null);
    }

    public void testCountStar5() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE NAME ='XIN' AND POINTS > ? AND POINTS <= ?", 2, true);
        checkIndexCounter(pn, false,
                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT_STAR},
                        null);
    }

//    public void testCountStarOnPartitionedTable() {
//        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1", 0, false);
//        checkPushedDown(pn, true,
//                        new ExpressionType[] {ExpressionType.AGGREGATE_COUNT_STAR},
//                        new ExpressionType[] {ExpressionType.AGGREGATE_SUM});
//    }

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
    private void checkIndexCounter(List<AbstractPlanNode> pn, boolean isMultiPart,
                                 ExpressionType[] aggTypes, ExpressionType[] pushDownTypes) {
        assertTrue(pn.size() > 0);

        AbstractPlanNode p = pn.get(0).getChild(0);
        for (AbstractPlanNode ap: pn) {
            System.out.println("Explan tree:\n" + ap.toExplainPlanString());
        }


        for ( AbstractPlanNode nd : pn) {
            System.out.println("PlanNode DOT string:\n" + nd.toDOTString());
            System.out.println("PlanNode Explan string:\n" + nd.toExplainPlanString());

        }
//        ArrayList<AbstractPlanNode> children = new ArrayList<AbstractPlanNode>();
//        for (int i = 0; i < p.getChildCount(); i++) {
//            children.add(p.getChild(i));
//            System.out.println("Child " + i + " :" + p.getChild(i).toExplainPlanString());
//            System.out.println("Parent " + i + " :" + p.getParent(i).toExplainPlanString());
//        }

        assertTrue(p instanceof IndexCountPlanNode);
//        if (pushDownTypes != null) {
//            for (ExpressionType type : pushDownTypes) {
//                assertTrue(p.toJSONString().contains("\"AGGREGATE_TYPE\":\"" +
//                                                     type.toString() + "\""));
//            }
//        } else {
//            for (ExpressionType type : aggTypes) {
//                assertTrue(p.toJSONString().contains("\"AGGREGATE_TYPE\":\"" +
//                                                     type.toString() + "\""));
//            }
//        }
//
//        if (isMultiPart) {
//            assertTrue(pn.size() == 2);
//            p = pn.get(1).getChild(0);
//        } else {
//            p = p.getChild(0);
//        }
//
//        if (pushDownTypes != null) {
//            assertTrue(p instanceof HashAggregatePlanNode);
//            for (ExpressionType type : aggTypes) {
//                assertTrue(p.toJSONString().contains("\"AGGREGATE_TYPE\":\"" +
//                                                     type.toString() + "\""));
//            }
//        } else {
//            assertTrue(p instanceof AbstractScanPlanNode);
//        }
    }
}
