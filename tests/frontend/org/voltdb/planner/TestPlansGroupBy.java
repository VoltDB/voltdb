/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import junit.framework.TestCase;

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;

public class TestPlansGroupBy extends TestCase {

    private PlannerTestAideDeCamp aide;

    private AbstractPlanNode compile(String sql, int paramCount) {
        AbstractPlanNode pn = null;
        try {
            pn =  aide.compile(sql, paramCount);
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
        return pn;
    }


    @Override
    protected void setUp() throws Exception {
        aide = new PlannerTestAideDeCamp(TestPlansGroupBy.class.getResource("testplans-groupby-ddl.sql"), "testplansgroupby");

        // Set all tables to non-replicated.
        Cluster cluster = aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            t.setIsreplicated(false);
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }


    public void testGroupByA1() {
        AbstractPlanNode pn = null;
        pn = compile("SELECT A1 from T1 group by A1", 0);
        if (pn != null)
            System.out.println(pn.toJSONString());
    }

    public void testCountA1GroupByA1() {
        AbstractPlanNode pn = null;
        pn = compile("SELECT count(A1) from T1 group by A1", 0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
   }

    public void testCountA1() {
        AbstractPlanNode pn = null;
        pn = compile("SELECT count(A1) from T1", 0);
        if (pn != null)
            System.out.println(pn.toJSONString());
    }

    public void testCountStar()
    {
        AbstractPlanNode pn = null;
        pn = compile("SELECT count(*) from T1", 0);
        if (pn != null)
        {
            System.out.println(pn.toJSONString());
        }
    }

   public void testCountDistinctA1() {
       AbstractPlanNode pn = null;
       pn = compile("SELECT count(distinct A1) from T1", 0);
       if (pn != null)
           System.out.println(pn.toJSONString());
   }

    public void testDistinctA1() {
        AbstractPlanNode pn = null;
        pn = compile("SELECT DISTINCT A1 FROM T1", 0);
        if (pn != null)
                System.out.println(pn.toJSONString());
    }

    // This plan won't compile right until ENG-490 is fixed...
    /*public void testGroupSingleJoin() {
        AbstractPlanNode pn = null;
        pn = compile(
                        "select D1.D1_NAME, sum(V.SUM_V1), sum(V.SUM_V2), sum(V.SUM_V3) "
                                        + "from D1, V where D1.D1_PKEY = V.V_D1_PKEY "
                                        + "group by D1.D1_NAME", 0);
        assert(false);
        if (pn != null)
                System.out.println(pn.toJSONString());
    }*/
}
