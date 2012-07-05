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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.plannodes.PlanNodeTree;

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

//    public void testEng2541Plan() throws JSONException
//    {
//        AbstractPlanNode pn = null;
//        pn = compile("select * from l where lname=? and b=0 order by id asc limit ?;", 3, true);
//        assertTrue(pn != null);
//
//        while( pn.getChildCount() > 0 )
//        	pn = pn.getChild(0);
//        //assertTrue(pn instanceof IndexScanPlanNode);
//        //assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"IDX_1\""));
//
//        if (pn != null) {
//            JSONObject j = new JSONObject(pn.toJSONString());
//            System.out.println(j.toString(2));
//            System.out.println();
//            System.out.println(pn.toExplainPlanString());
//        }
//    }
    
//    public void testGetLeafLists() {
//    	AbstractPlanNode pn = null;
//        pn = compile("select * from l where lname=? and b=0 order by id asc limit ?;", 3, true);
//        assertTrue(pn != null);
//        
//        ArrayList<AbstractPlanNode> collected = pn.getLeafLists();
//        System.out.println( collected);
//        System.out.println( collected.size() );
//        for( AbstractPlanNode n : collected )
//           System.out.println( n.toExplainPlanString() );
//        //assertTrue( collected.size() == 1 );
//        JSONObject j;
//		try {
//			j = new JSONObject( collected.get(0).toJSONString() );
//			System.out.println(j.getString("PLAN_NODE_TYPE"));
//		} catch (JSONException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	
//        //assertTrue( j.getString("PLAN_NODE_TYPE").equalsIgnoreCase("LIMIT") = 1 );
//    }
    
    public void testDiffLeaves() {
    	AbstractPlanNode pn1 = null;
    	AbstractPlanNode pn2 = null;
        //pn1 = compile("select * from l where lname=? and b=0 order by id asc limit ?;", 3, true);
    	pn1 = compile("select * from l, t where t.b=l.b limit ?;", 3, true);
        pn2 = compile("select * from l, t where l.b=t.b limit ?;", 3, true);
        assertTrue(pn1 != null);
        assertTrue(pn2 != null);
        //plannerTester.diffLeaves(pn1, pn2);
        PlanNodeList nodelist1 = new PlanNodeList( pn1);
        try {
			nodelist1.constructList();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //System.out.println( nodelist1.toJSONString() );
        PlanNodeTree pnt = new PlanNodeTree( pn1 );
       
        JSONObject jobj;
        String prettyJson = null;
//		try {
//			jobj = new JSONObject(pnt.toJSONString());
//			prettyJson = jobj.toString(4);
//		} catch (JSONException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
        
        prettyJson = pnt.toJSONString();
        System.out.println( prettyJson );
        try {
			jobj = new JSONObject( prettyJson );
			
			JSONArray values = jobj.getJSONArray("PLAN_NODES");
			System.out.println( values.getJSONObject(0).toString() );
			System.out.println( values.getJSONObject(0).getString("OUTPUT_SCHEMA") );
			for( int i = 0; i < 6; i++ )
				System.out.println( values.getJSONObject(i).getString("PLAN_NODE_TYPE") );
			String str = values.getJSONObject(3).getString("CHILDREN_IDS");
			System.out.println(str);
			System.out.println( str.split(",")[1] );
			System.out.println( values.getJSONObject(0).getJSONArray("CHILDREN_IDS").get(0));
			//ArrayList<Integer> intlist =  
			System.out.println( jobj.toString() );
		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        
//        try {
//			BufferedWriter writer = new BufferedWriter( new FileWriter("/home/zhengli/prettyJson.txt") );
//			writer.write( prettyJson );
//			writer.flush();
//			writer.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
        
        prettyJson = "";
        String line = null;
        try {
			BufferedReader reader = new BufferedReader( new FileReader( "/home/zhengli/prettyJson.txt" ));
				while( (line = reader.readLine() ) != null ){
					line = line.trim();
					prettyJson += line;
				}
			}
        catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
			System.out.println( prettyJson );
		JSONObject jobj1;
		try {
			jobj1 = new JSONObject( prettyJson );
			JSONArray jarray = 	jobj1.getJSONArray("PLAN_NODES");
			PlanNodeTree pnt1 = new PlanNodeTree();
			pnt1.loadFromJSONArray(jarray);
			System.out.println( pnt1.toJSONString() );
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        
    }
    
}

