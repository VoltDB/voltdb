/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import junit.framework.TestCase;

public class TestPushDownAggregates extends TestCase {
    private PlannerTestAideDeCamp aide;

    private List<AbstractPlanNode> compile(String sql, int paramCount,
                                     boolean singlePartition)
    {
        List<AbstractPlanNode> pn = null;
        try {
            pn = aide.compile(sql, paramCount, singlePartition);
        }
        catch (NullPointerException ex) {
            // aide may throw NPE if no plangraph was created
            ex.printStackTrace();
            fail();
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
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }

    public void testCountStarOnReplicatedTable() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from D1", 0, true);
        checkPushedDown(pn, false, false);
    }

    public void testCountStarOnPartitionedTable() {
        List<AbstractPlanNode> pn = compile("SELECT count(*) from T1", 0, false);
        checkPushedDown(pn, true, true);
    }

    public void testCountStarWithGroupBy() {
        List<AbstractPlanNode> pn = compile("SELECT A1, count(*) FROM T1 GROUP BY A1", 0, false);
        checkPushedDown(pn, true, true);
    }

    public void testCountStarWithOtherAggregates() {
        List<AbstractPlanNode> pn = compile("SELECT count(*), max(A1) FROM T1", 0, false);
        checkPushedDown(pn, true, false);
    }

    private void checkPushedDown(List<AbstractPlanNode> pn, boolean isMultiPart,
                                 boolean isPushedDown) {
        assertTrue(pn.size() > 0);

        AbstractPlanNode p = pn.get(0).getChild(0);
        assertTrue(p instanceof HashAggregatePlanNode);
        if (isPushedDown) {
            assertTrue(p.toJSONString().contains("\"AGGREGATE_TYPE\":\"AGGREGATE_SUM\""));
        } else {
            assertTrue(p.toJSONString().contains("\"AGGREGATE_TYPE\":\"AGGREGATE_COUNT_STAR\""));
        }

        if (isMultiPart) {
            assertTrue(pn.size() == 2);
            p = pn.get(1).getChild(0);
        } else {
            p = p.getChild(0);
        }

        if (isPushedDown) {
            assertTrue(p instanceof HashAggregatePlanNode);
            assertTrue(p.toJSONString().contains("\"AGGREGATE_TYPE\":\"AGGREGATE_COUNT_STAR\""));
        } else {
            assertTrue(p instanceof AbstractScanPlanNode);
        }
    }
}
