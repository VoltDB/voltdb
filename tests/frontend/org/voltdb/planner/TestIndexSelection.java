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

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;

public class TestIndexSelection extends TestCase {

    private PlannerTestAideDeCamp aide;

    private AbstractPlanNode compile(String sql, int paramCount,
                                     boolean singlePartition)
    {
        List<AbstractPlanNode> pn = null;
        try {
            pn =  aide.compile(sql, paramCount, singlePartition);
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
        assertTrue(pn.get(0) != null);
        return pn.get(0);
    }

    @Override
    protected void setUp() throws Exception {
        aide = new PlannerTestAideDeCamp(TestIndexSelection.class.getResource("testplans-indexselection-ddl.sql"),
                                         "testindexselectionplans");

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

    /*public void testEng931Plan()
    {
        AbstractPlanNode pn = null;
        pn =
            compile("select a from t where a = ? and b = ? and c = ? and d = ? and e >= ? and e <= ?;", 6, true);
        assertTrue(pn != null);

        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"IDX_1\""));

        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
    }*/

    public void testEng2541Plan() throws JSONException
    {
        AbstractPlanNode pn = null;
        pn = compile("select * from l where lname=? and b=0 order by id asc limit ?;", 3, true);
        assertTrue(pn != null);

        pn = pn.getChild(0);
        //assertTrue(pn instanceof IndexScanPlanNode);
        //assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"IDX_1\""));

        if (pn != null) {
            JSONObject j = new JSONObject(pn.toJSONString());
            System.out.println(j.toString(2));
            System.out.println();
            System.out.println(pn.toExplainPlanString());
        }
    }
    
    public void testGetLeafLists() {
    	AbstractPlanNode pn = null;
        pn = compile("select * from l where lname=? and b=0 order by id asc limit ?;", 3, true);
        assertTrue(pn != null);
        while( pn.getChild(0) != null )
        {
        	System.out.println( pn );
        	pn = pn.getChild(0);
        }
        System.out.println( pn );
        
//        ArrayList<AbstractPlanNode> collected = pn.getLeafLists();
//        System.out.println( collected);
//        System.out.println( collected.size() );
//        for( AbstractPlanNode n : collected )
//           System.out.println( n.toExplainPlanString() );
    }
}
