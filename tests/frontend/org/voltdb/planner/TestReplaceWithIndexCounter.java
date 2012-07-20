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

    // DOES NOT support the cases down below right now
    public void testCountStar0() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1", 0, true);
        checkIndexCounter(pn, true, false);
    }

    public void testCountStar00() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 ORDER BY POINTS", 0, true);
        checkIndexCounter(pn, true, false);
    }
    // TODO(xin): Replace it with IndexCount node later
    public void testCountStar1() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS = ?", 0, true);
        checkIndexCounter(pn, false, false);
    }
    // SeqScan is not supported right now
    public void testCountStar3() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS < ?", 0, true);
        checkIndexCounter(pn, false, false);
    }

    public void testCountStar7() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS >= 3 AND AGE = ?", 1, true);
        checkIndexCounter(pn, false, false);
    }

    public void testCountStar15() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND AGE = 3 AND POINTS < ?", 2, false);
        checkIndexCounter(pn, false, false);
    }

    public void testCountStar111() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE USERNAME >'XIN' AND POINTS = ?", 1, true);
        checkIndexCounter(pn, false, false);
    }

    // Down below are cases that we can replace

    public void testCountStar11() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS > ?", 1, true);
        checkIndexCounter(pn, false, false);
    }

    public void testCountStar4() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS >= 3", 0, true);
        checkIndexCounter(pn, false, true);
    }

    public void testCountStar5() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS > 3 AND POINTS <= 6", 0, true);
        checkIndexCounter(pn, false, true);
    }

    public void testCountStar8() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS > ? AND POINTS < ?", 2, true);
        checkIndexCounter(pn, false, true);
    }

    public void testCountStar12() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS >= ? AND POINTS <= ?", 2, true);
        checkIndexCounter(pn, false, true);
    }

    public void testCountStar13() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS < ?", 1, true);
        checkIndexCounter(pn, false, true);
    }

    public void testCountStar14() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE USERNAME ='XIN'", 1, true);
        checkIndexCounter(pn, false, false);
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
    private void checkIndexCounter(List<AbstractPlanNode> pn, boolean isMultiPart, boolean isReplaceable) {
        assertTrue(pn.size() > 0);

        AbstractPlanNode p = pn.get(0).getChild(0);
        for (AbstractPlanNode ap: pn) {
            System.out.println("Explan tree:\n" + ap.toExplainPlanString());
        }


        for ( AbstractPlanNode nd : pn) {
            System.out.println("PlanNode DOT string:\n" + nd.toDOTString());
            System.out.println("PlanNode Explan string:\n" + nd.toExplainPlanString());

        }
        if (isReplaceable)
            assertTrue(p instanceof IndexCountPlanNode);
        else
            assertTrue((p instanceof IndexCountPlanNode) == false);

    }
}
