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

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeTree;

public class testLoadPlanNodeFromJSON extends TestCase {
    private PlannerTestAideDeCamp aide;

    private AbstractPlanNode compile(String sql, int paramCount,
            boolean singlePartition) {
        List<AbstractPlanNode> pn = null;
        try {
            pn = aide.compile(sql, paramCount, singlePartition);
        } catch (NullPointerException ex) {
            // aide may throw NPE if no plangraph was created
            ex.printStackTrace();
            fail();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        assertTrue(pn != null);
        assertFalse(pn.isEmpty());
        assertTrue(pn.get(0) != null);
        return pn.get(0);
    }

    @Override
    protected void setUp() throws Exception {
        aide = new PlannerTestAideDeCamp(
                TestIndexSelection.class
                        .getResource("testplans-indexselection-ddl.sql"),
                "testindexselectionplans");

        // Set all tables to non-replicated.
        Cluster cluster = aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database")
                .getTables();
        for (Table t : tmap) {
            t.setIsreplicated(false);
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }

    public void testLoadQueryPlans() throws JSONException {
        testLoadQueryPlanTree(
                "select count(*) from l,t where lname=? and l.a=t.a order by l.b limit ?;",
                3, true);
        testLoadQueryPlanTree(
                "select * from l,t where lname=? and l.a=t.a order by l.b limit ?;",
                3, true);
        testLoadQueryPlanTree(
                "select l.id, count(*) as tag from l group by l.id order by tag, l.id limit ?;",
                3, true);
        testLoadQueryPlanTree(
                "select count(*) from l where lname=? and id < ?;",
                3, true);
    }

    public void testLoadQueryPlanTree(String sql, int paraCount,
            boolean singlePartition) throws JSONException {
        AbstractPlanNode pn = compile(sql, paraCount, singlePartition);
        PlanNodeTree pnt = new PlanNodeTree(pn);
        String str = pnt.toJSONString();
        System.out.println(str);
        JSONArray jarray = new JSONObject(str)
                .getJSONArray(PlanNodeTree.Members.PLAN_NODES.name());
        PlanNodeTree pnt1 = new PlanNodeTree();
        pnt1.loadFromJSONArray(jarray, aide.getDatabase());
        String str1 = pnt1.toJSONString();
        assertTrue(str.equals(str1));
    }
}
