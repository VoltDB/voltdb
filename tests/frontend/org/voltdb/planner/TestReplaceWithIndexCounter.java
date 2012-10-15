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
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexCountPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.TableCountPlanNode;

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

        // Set all tables except for P1 to non-replicated.
        Cluster cluster = aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            if (t.getTypeName().equalsIgnoreCase("p1")) {
                t.setIsreplicated(false);
                t.setPartitioncolumn(t.getColumns().get("ID"));
            } else {
                t.setIsreplicated(true);
            }
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }

    // DOES NOT support the cases down below right now

    // This is treated as new TABLE COUNT plan for replicated table
    public void testCountStar000() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1", 0, false);
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof TableCountPlanNode);
    }
    // This is treated as new TABLE COUNT plan for partitioned table
    public void testCountStar001() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from P1", 0, false);
        AbstractPlanNode p = pn.get(0).getChild(0);
        // AGGREGATE_SUM
        assertTrue(p instanceof AggregatePlanNode);
        p = pn.get(1).getChild(0);
        assertTrue(p instanceof TableCountPlanNode);
    }

    public void testCountStar002() {
        List<AbstractPlanNode> pn = compile("SELECT POINTS, count(*) from T1 Group by POINTS", 0, false);
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        p = p.getChild(0);
        assertTrue(p instanceof SeqScanPlanNode);
    }

    // This is generated as IndexScan other than SeqScan as "SELECT count(*) from T1"
    // "Order by" here cheat planner at this point
    // Maybe we should fix it later
    public void testCountStar01() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 ORDER BY POINTS ASC", 0, false);
        checkIndexCounter(pn, false);
    }

    public void testCountStar02() {
        List<AbstractPlanNode> pn = compile("SELECT P1.ID, P2.P2_ID from P1, P2 where P1.ID >= P2.P2_ID order by P1.ID, P2.P2_ID limit 10", 0, false);
        checkIndexCounter(pn, false);
    }

    public void testCountStar03() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS < 4 ORDER BY POINTS DESC", 0, false);
        checkIndexCounter(pn, false);
    }

    public void testCountStar04() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS = ?", 1, false);
        checkIndexCounter(pn, false);
    }

    public void testCountStar05() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS < ? ORDER BY ID DESC", 0, false);
        checkIndexCounter(pn, true);
    }

    public void testCountStar06() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS >= 3 AND AGE = ?", 1, false);
        checkIndexCounter(pn, false);
    }

    public void testCountStar07() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND AGE = 3 AND POINTS < ?", 1, false);
        checkIndexCounter(pn, false);
    }

    public void testCountStar08() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE USERNAME >'XIN' AND POINTS = ?", 1, false);
        checkIndexCounter(pn, false);
    }

    public void testCountStar10() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS > ?", 1, false);
        checkIndexCounter(pn, false);
    }

    // Down below are cases that we can replace
    public void testCountStar11() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS < 4 ORDER BY POINTS", 0, false);
        checkIndexCounter(pn, true);
    }

    public void testCountStar12() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS < ?", 1, false);
        checkIndexCounter(pn, true);
    }

    public void testCountStar13() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS >= 3", 0, false);
        checkIndexCounter(pn, true);
    }

    public void testCountStar14() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS > 3 AND POINTS <= 6", 0, false);
        checkIndexCounter(pn, true);
    }

    public void testCountStar15() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1 WHERE POINTS > ? AND POINTS < ?", 2, false);
        checkIndexCounter(pn, true);
    }

    public void testCountStar16() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS >= ? AND POINTS <= ?", 2, false);
        checkIndexCounter(pn, true);
    }

    public void testCountStar17() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS < ?", 1, false);
        checkIndexCounter(pn, true);
    }

    public void testCountStar18() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE USERNAME ='XIN'", 0, false);
        checkIndexCounter(pn, false);
    }

    // Planner bug with Constant value overflow
    public void testCountStar19() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS >= 3 AND POINTS <= 600000000000000000000000000", 0, false);
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue((p instanceof IndexCountPlanNode) == false);
    }
    // test with group by with Replicated table
    public void testCountStar20() {
        List<AbstractPlanNode> pn = compile("SELECT AGE, count(*) from T2 WHERE USERNAME ='XIN' AND POINTS < 1 Group by AGE", 0, false);
        for ( AbstractPlanNode nd : pn)
            System.out.println("PlanNode Explan string:\n" + nd.toExplainPlanString());
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        p = p.getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
    }

    // test with group by with Partitioned table
    public void testCountStar21() {
        List<AbstractPlanNode> pn = compile("SELECT RATIO, count(*) from P1 WHERE NUM < 1 Group by RATIO", 0, false);
        for ( AbstractPlanNode nd : pn)
            System.out.println("PlanNode Explan string:\n" + nd.toExplainPlanString());
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        p = pn.get(1).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        p = p.getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
    }

    // Test counting index feature with partitioned table
    public void testCountStar22() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from P1 WHERE NUM < ?", 1, false);
        for ( AbstractPlanNode nd : pn)
            System.out.println("PlanNode Explan string:\n" + nd.toExplainPlanString());
        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        p = pn.get(1).getChild(0);
        assertTrue(p instanceof IndexCountPlanNode);
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
            System.out.println("PlanNode Explan string:\n" + nd.toExplainPlanString());
        }
        AbstractPlanNode p = pn.get(0).getChild(0);
        if (isReplaceable)
            assertTrue(p instanceof IndexCountPlanNode);
        else
            assertTrue((p instanceof IndexCountPlanNode) == false);

    }
}
