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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.lang3.tuple.Pair;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.plannodes.PlanNodeTree;
import org.voltdb.types.PlanNodeType;
public class testPlannerTester extends TestCase {
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
        aide = new PlannerTestAideDeCamp(testPlannerTester.class.getResource("testplans-plannerTester-ddl.sql"),
                                         "testplans-plannerTester-ddl");

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
    public void testBatchCompileSave() {
    	try {
    		plannerTester.setUp("/home/zhengli/test1");
			plannerTester.batchCompileSave();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void testCompile() {
				try {
					plannerTester.setUp("/home/zhengli/workspace/voltdb/examples/voter/ddl.sql",
							"ddl", "votes", "phone_number");
					//List<AbstractPlanNode> pnList = plannerTester.compile("select * from l, t where t.a=l.a limit ?;", 3, false);
					List<AbstractPlanNode> pnList = plannerTester.compile("INSERT INTO votes (phone_number, state, contestant_number) VALUES (?, ?, ?);", 4, false);
					//assertTrue( pnList.size() == 2 );
					System.out.println( pnList.size() );
					System.out.println( pnList.get(0).toExplainPlanString() );
					
					AbstractPlanNode pn = plannerTester.combinePlanNodes( pnList );
					System.out.println( pn.toExplainPlanString() );
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    }
    
    public void testWriteAndLoad() throws Exception {
    	AbstractPlanNode pn = null;
    	//pn = compile("select * from l, t where t.b=l.b limit ?;", 3, true);
    	//pn = compile("select * from l where b = ? limit ?;", 3, true);
    	//pn = compile("select * from l where lname=? and b=0 order by id asc limit ?;", 0, true);
    	//pn = compile("select * from t where a = ? order by a limit ?;",3, true);
    	plannerTester.setUp("/home/zhengli/workspace/voltdb/tests/frontend/org/voltdb/planner/testplans-plannerTester-ddl.sql",
				"testplans-plannerTester-ddl", "L", "a");
		List<AbstractPlanNode> pnList = plannerTester.compile("select * from l, t where t.a=l.a limit ?;", 3, false);

    	String path = "/home/zhengli/";
    	System.out.println( pnList.size() );
    	
    	pn = plannerTester.combinePlanNodes(pnList);
    	System.out.println( pn.toExplainPlanString() );
    	plannerTester.writePlanToFile( pn, path, "prettyJson.txt");
    	
    	PlanNodeTree pnt = plannerTester.loadPlanFromFile(path);
    	System.out.println( pnt.toJSONString() );
    	//System.out.println( pnt.getRootPlanNode().toExplainPlanString() );
    	ArrayList<AbstractPlanNode> list1 = pn.getLists();
    	ArrayList<AbstractPlanNode> list2 = pnt.getRootPlanNode().getLists();
    	assertTrue( list1.size() == list2.size() );
    	for( int i = 0; i < list1.size(); i++ ) {
    		Map<PlanNodeType, AbstractPlanNode> inlineNodes1 = list1.get(i).getInlinePlanNodes();
    		Map<PlanNodeType, AbstractPlanNode> inlineNodes2 = list2.get(i).getInlinePlanNodes();
    		if(  inlineNodes1 != null )
    			assertTrue( inlineNodes1.size() == inlineNodes2.size() );
    	}
    }
    
//    public void testLoadFromJSON() {
//    	AbstractPlanNode pn1 = null;
//        //pn1 = compile("select * from l where lname=? and b=0 order by id asc limit ?;", 3, true);
//    	pn1 = compile("select * from l, t where t.b=l.b limit ?;", 3, true);
//        assertTrue(pn1 != null);
//        PlanNodeTree pnt = new PlanNodeTree( pn1 );
//       
//        JSONObject jobj;
//        String prettyJson = null;
//        
//        prettyJson = pnt.toJSONString();
//        System.out.println( prettyJson );
//        try {
//			jobj = new JSONObject( prettyJson );
//			
//			JSONArray values = jobj.getJSONArray("PLAN_NODES");
//			System.out.println( values.getJSONObject(0).toString() );
//			System.out.println( values.getJSONObject(0).getString("OUTPUT_SCHEMA") );
//			for( int i = 0; i < 6; i++ )
//				System.out.println( values.getJSONObject(i).getString("PLAN_NODE_TYPE") );
//			String str = values.getJSONObject(3).getString("CHILDREN_IDS");
//			System.out.println(str);
//			System.out.println( str.split(",")[1] );
//			System.out.println( values.getJSONObject(0).getJSONArray("CHILDREN_IDS").get(0));
//			//ArrayList<Integer> intlist =  
//			System.out.println( jobj.toString() );
//		} catch (JSONException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//        //write plan to file
////        try {
////			BufferedWriter writer = new BufferedWriter( new FileWriter("/home/zhengli/prettyJson.txt") );
////			writer.write( prettyJson );
////			writer.flush();
////			writer.close();
////		} catch (IOException e) {
////			// TODO Auto-generated catch block
////			e.printStackTrace();
////		} 
//        prettyJson = "";
//        String line = null;
//        //load plan from file
//        try {
//			BufferedReader reader = new BufferedReader( new FileReader( "/home/zhengli/prettyJson.txt" ));
//				while( (line = reader.readLine() ) != null ){
//					line = line.trim();
//					prettyJson += line;
//				}
//			}
//        catch (IOException e) {
//    		// TODO Auto-generated catch block
//    		e.printStackTrace();
//    	}
//		System.out.println( prettyJson );
//		JSONObject jobj1;
//		try {
//			jobj1 = new JSONObject( prettyJson );
//			JSONArray jarray = 	jobj1.getJSONArray("PLAN_NODES");
//			PlanNodeTree pnt1 = new PlanNodeTree();
//			pnt1.loadFromJSONArray(jarray);
//			System.out.println( pnt1.toJSONString() );
//		} catch (JSONException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    }
    public void testGetLists() {
    	AbstractPlanNode pn1 = null;
        pn1 = compile("select * from l, t where t.b=l.b limit ?;", 3, true);
        
        ArrayList<AbstractPlanNode> pnlist = pn1.getLists();
        
        System.out.println( pn1.toExplainPlanString() );
        System.out.println( pnlist.size() );
        for( int i = 0; i<pnlist.size(); i++ ){
        	System.out.println( pnlist.get(i).toJSONString() );
        }
        assertTrue( pnlist.size() == 6 );
    }
    
    public void testDiffInlineNodes() {
    	AbstractPlanNode pn1 = null;
    	AbstractPlanNode pn2 = null;
        //pn1 = compile("select * from l where lname=? and b=0 order by id asc limit ?;", 0, true);
        pn1 = compile("select * from l, t where t.b=l.b limit ?;", 3, true);
        //pn2 = compile("select * from l, t where t.b=l.b limit ?;", 3, true);
        pn2 = compile("select * from l where b = ? limit ?;", 3, true);
        assertTrue(pn1 != null);
        assertTrue(pn2 != null);
        System.out.println( pn1.toExplainPlanString() );
        System.out.println( pn2.toExplainPlanString() );
        plannerTester.diffInlineNodes(pn1, pn2);
    }
    
    public void testDiff() {
    	AbstractPlanNode pn1 = null;
    	AbstractPlanNode pn2 = null;
    	ArrayList<AbstractPlanNode> list1 = new ArrayList<AbstractPlanNode>();
    	ArrayList<AbstractPlanNode> list2 = new ArrayList<AbstractPlanNode>();
        pn1 = compile("select * from l order by b limit ?;", 3, true);
        pn2 = compile("select * from l order by a limit ?;", 3, true);
        assertTrue(pn1 != null);
        assertTrue(pn2 != null);
        list1.add(pn1);
        list2.add(pn2);
        pn1 = compile("select * from l, t where t.a=l.b order by b limit ?;", 3, true);
        pn2 = compile("select * from l, t where t.a=l.b order by a limit ?;", 3, true);
        list1.add(pn1);
        list2.add(pn2);
        pn1 = compile("select * from l, t where t.a=l.b order by b;", 3, true);
        pn2 = compile("select * from l, t where t.a=l.b order by a limit ?;", 3, true);
        list1.add(pn1);
        list2.add(pn2);
        pn1 = compile("select * from l, t where t.a=l.b limit ?;", 3, true);
        pn2 = compile("select * from l, t where t.a=l.b order by a limit ?;", 3, true);
        list1.add(pn1);
        list2.add(pn2);
        pn1 = compile("select * from l, t where t.a=l.b;", 3, true);
        pn2 = compile("select * from l, t where t.a=l.b order by a limit ?;", 3, true);
        list1.add(pn1);
        list2.add(pn2);
        pn1 = compile("select * from l order by a limit ?;", 3, true);
        pn2 = compile("select * from l, t where t.a=l.b order by a limit ?;", 3, true);
        list1.add(pn1);
        list2.add(pn2);
        pn1 = compile("select * from l limit ?;", 3, true);
        pn2 = compile("select * from l, t where t.a=l.b order by a limit ?;", 3, true);
        list1.add(pn1);
        list2.add(pn2);
        pn1 = compile("select * from l where a=? order by b limit ?;", 3, true);
        pn2 = compile("select * from l, t where t.a=l.b order by a limit ?;", 3, true);
        list1.add(pn1);
        list2.add(pn2);
        pn1 = compile("select * from l where a=? limit ?;", 3, true);
        pn2 = compile("select * from l, t where t.a=l.b order by a limit ?;", 3, true);
        list1.add(pn1);
        list2.add(pn2);
        pn1 = compile("select * from l where a=?;", 3, true);
        pn2 = compile("select * from l, t where t.a=l.b order by a limit ?;", 3, true);
        list1.add(pn1);
        list2.add(pn2);
        int size = list1.size();
        for( int i = 0; i < size; i++ ) {
        	System.out.println(i);
        	System.out.println(list1.get(i).toExplainPlanString());
            System.out.println(list2.get(i).toExplainPlanString());
            plannerTester.diffScans(list1.get(i), list2.get(i));
            plannerTester.diffInlineNodes(list1.get(i), list2.get(i));
        }
    }
    
    public void testBatchDiff() {
    	int size = 7;
    	String pathBaseline = "/tmp/volttest/baseline1/testplans-plannerTester-ddl.plan";
    	String pathNew = "/tmp/volttest/new1/testplans-plannerTester-ddl.plan";
    	plannerTester.batchDiff(pathBaseline, pathNew, size);
    }
    
    public void testWholeProcess() {
    	try {
			plannerTester.setUp("/home/zhengli/test1");
			plannerTester.batchCompileSave();
			plannerTester.batchDiff();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
    //make sure plan files are already in baseline directories 
    public void testDiffVoltExamples() {
    	try {
    		plannerTester.setUp("/home/zhengli/test1");
    		plannerTester.batchDiff();
    	
    		plannerTester.setUp("/home/zhengli/Voter");
    		plannerTester.batchDiff();
    	
    		plannerTester.setUp("/home/zhengli/voltcache");
    		plannerTester.batchDiff();
    	
    		plannerTester.setUp("/home/zhengli/voltkv");
    		plannerTester.batchDiff();
    	}
    	catch ( Exception e ) {
    		e.printStackTrace();
    	}
    }
    
    public void testMain() {
    	String[] args = {"-d","-C=/home/zhengli/test1", "-C=/home/zhengli/Voter","-C=/home/zhengli/voltcache","-C=/home/zhengli/voltkv"};
    	plannerTester.main(args);
    }
}

