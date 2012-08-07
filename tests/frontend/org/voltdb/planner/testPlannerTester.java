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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeTree;
import org.voltdb.types.PlanNodeType;
public class testPlannerTester extends TestCase {
    private PlannerTestAideDeCamp aide;
    private String m_currentDir;
    private String m_homeDir;

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
        m_currentDir = new File(".").getCanonicalPath();
        m_homeDir = System.getProperty("user.home");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }

    public void testGetLeafLists() {
        AbstractPlanNode pn = null;
        pn = compile("select * from l where lname=? and b=0 order by id asc limit ?;", 3, true);
        assertTrue(pn != null);

        ArrayList<AbstractPlanNode> collected = pn.getScanNodeList();
        System.out.println( collected );
        System.out.println( collected.size() );
        for( AbstractPlanNode n : collected ) {
            System.out.println( n.toExplainPlanString() );
        }
        assertTrue( collected.size() == 1 );
        JSONObject j;
        try {
            j = new JSONObject( collected.get(0).toJSONString() );
            System.out.println(j.getString("PLAN_NODE_TYPE"));
            assertTrue( j.getString("PLAN_NODE_TYPE").equalsIgnoreCase("INDEXSCAN")  );
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    public void testBatchCompileSave() {
        try {
            plannerTester.setUp(m_homeDir+"/test1");
            plannerTester.batchCompileSave();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testCompile() {
        try {
            //                                  plannerTester.setUp(m_currentDir+"/examples/voter/ddl.sql",
            //                                                  "ddl", "l", "phone_number");
            //List<AbstractPlanNode> pnList = plannerTester.compile("INSERT INTO votes (phone_number, state, contestant_number) VALUES (?, ?, ?);", 4, false);
            //assertTrue( pnList.size() == 2 );

            plannerTester.setUpForTest(m_currentDir+"/tests/frontend/org/voltdb/planner/testplans-plannerTester-ddl.sql",
                    "testplans-plannerTester-ddl", "L", "a");
            List<AbstractPlanNode> pnList = plannerTester.compile("select * from l, t where t.a=l.a limit ?;", 3, false);
            System.out.println( pnList.size() );
            System.out.println( pnList.get(0).toExplainPlanString() );

            AbstractPlanNode pn = plannerTester.combinePlanNodes( pnList );
            System.out.println( pn.toExplainPlanString() );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testWriteAndLoad() throws Exception {
        AbstractPlanNode pn = null;
        //pn = compile("select * from l, t where t.b=l.b limit ?;", 3, true);
        //pn = compile("select * from l where b = ? limit ?;", 3, true);
        //pn = compile("select * from l where lname=? and b=0 order by id asc limit ?;", 0, true);
        //      pn = compile("select * from t where a = ? order by a limit ?;",3, true);
        String path = m_homeDir+"/";
        plannerTester.setUpForTest(m_currentDir+"/tests/frontend/org/voltdb/planner/testplans-plannerTester-ddl.sql",
                "testplans-plannerTester-ddl", "L", "a");
        List<AbstractPlanNode> pnList = plannerTester.compile("select * from l, t where t.a=l.a limit ?;", 3, false);

        System.out.println( pnList.size() );

        pn = plannerTester.combinePlanNodes(pnList);
        System.out.println( pn.toJSONString() );
        System.out.println( pn.toExplainPlanString() );
        plannerTester.writePlanToFile( pn, path, "prettyJson.txt", "");

        ArrayList<String> getsql = new ArrayList<String>();
        PlanNodeTree pnt = plannerTester.loadPlanFromFile(path+"prettyJson.txt", getsql);
        System.out.println( pnt.toJSONString() );
        System.out.println( pnt.getRootPlanNode().toExplainPlanString() );
        ArrayList<AbstractPlanNode> list1 = pn.getLists();
        ArrayList<AbstractPlanNode> list2 = pnt.getRootPlanNode().getLists();
        assertTrue( list1.size() == list2.size() );
        for( int i = 0; i < list1.size(); i++ ) {
            Map<PlanNodeType, AbstractPlanNode> inlineNodes1 = list1.get(i).getInlinePlanNodes();
            Map<PlanNodeType, AbstractPlanNode> inlineNodes2 = list2.get(i).getInlinePlanNodes();
            if(  inlineNodes1 != null ) {
                assertTrue( inlineNodes1.size() == inlineNodes2.size() );
            }
        }
    }

    public void testLoadJoinType() throws FileNotFoundException {
        AbstractPlanNode pn = null;
        pn = compile("select * from l, t where l.b=t.b limit ?;", 3, true);

        String path = m_homeDir+"/";
        System.out.println( pn.toExplainPlanString() );
        System.out.println( pn.toJSONString() );
        plannerTester.writePlanToFile( pn, path, "prettyJson.txt", "");

        ArrayList<String> getsql = new ArrayList<String>();
        PlanNodeTree pnt = plannerTester.loadPlanFromFile(path+"prettyJson.txt", getsql);
        System.out.println( pnt.toJSONString() );
        System.out.println( pnt.getRootPlanNode().toExplainPlanString() );
        //System.out.println( pnt.getRootPlanNode().toExplainPlanString() );
        ArrayList<AbstractPlanNode> list1 = pn.getLists();
        ArrayList<AbstractPlanNode> list2 = pnt.getRootPlanNode().getLists();
        assertTrue( list1.size() == list2.size() );
        for( int i = 0; i < list1.size(); i++ ) {
            Map<PlanNodeType, AbstractPlanNode> inlineNodes1 = list1.get(i).getInlinePlanNodes();
            Map<PlanNodeType, AbstractPlanNode> inlineNodes2 = list2.get(i).getInlinePlanNodes();
            if(  inlineNodes1 != null ) {
                assertTrue( inlineNodes1.size() == inlineNodes2.size() );
            }
        }
    }

    //    public void testLoadFromJSON() {
    //          AbstractPlanNode pn1 = null;
    //        //pn1 = compile("select * from l where lname=? and b=0 order by id asc limit ?;", 3, true);
    //          pn1 = compile("select * from l, t where t.b=l.b limit ?;", 3, true);
    //        assertTrue(pn1 != null);
    //        PlanNodeTree pnt = new PlanNodeTree( pn1 );
    //
    //        JSONObject jobj;
    //        String prettyJson = null;
    //
    //        prettyJson = pnt.toJSONString();
    //        System.out.println( prettyJson );
    //        try {
    //                  jobj = new JSONObject( prettyJson );
    //
    //                  JSONArray values = jobj.getJSONArray("PLAN_NODES");
    //                  System.out.println( values.getJSONObject(0).toString() );
    //                  System.out.println( values.getJSONObject(0).getString("OUTPUT_SCHEMA") );
    //                  for( int i = 0; i < 6; i++ )
    //                          System.out.println( values.getJSONObject(i).getString("PLAN_NODE_TYPE") );
    //                  String str = values.getJSONObject(3).getString("CHILDREN_IDS");
    //                  System.out.println(str);
    //                  System.out.println( str.split(",")[1] );
    //                  System.out.println( values.getJSONObject(0).getJSONArray("CHILDREN_IDS").get(0));
    //                  //ArrayList<Integer> intlist =
    //                  System.out.println( jobj.toString() );
    //          } catch (JSONException e1) {
    //                  //
    //                  e1.printStackTrace();
    //          }
    //        //write plan to file
    ////        try {
    ////                        BufferedWriter writer = new BufferedWriter( new FileWriter(m_homeDir+"/prettyJson.txt") );
    ////                        writer.write( prettyJson );
    ////                        writer.flush();
    ////                        writer.close();
    ////                } catch (IOException e) {
    ////                        //
    ////                        e.printStackTrace();
    ////                }
    //        prettyJson = "";
    //        String line = null;
    //        //load plan from file
    //        try {
    //                  BufferedReader reader = new BufferedReader( new FileReader( m_homeDir+"/prettyJson.txt" ));
    //                          while( (line = reader.readLine() ) != null ){
    //                                  line = line.trim();
    //                                  prettyJson += line;
    //                          }
    //                  }
    //        catch (IOException e) {
    //                  //
    //                  e.printStackTrace();
    //          }
    //          System.out.println( prettyJson );
    //          JSONObject jobj1;
    //          try {
    //                  jobj1 = new JSONObject( prettyJson );
    //                  JSONArray jarray =      jobj1.getJSONArray("PLAN_NODES");
    //                  PlanNodeTree pnt1 = new PlanNodeTree();
    //                  pnt1.loadFromJSONArray(jarray);
    //                  System.out.println( pnt1.toJSONString() );
    //          } catch (JSONException e) {
    //                  //
    //                  e.printStackTrace();
    //          }
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
        plannerTester.diffInlineAndJoin(pn1, pn2);
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
        pn1 = compile("select * from l, t where t.a=l.a;", 3, true);
        pn2 = compile("select * from l, t where t.b=l.b order by a limit ?;", 3, true);
        list1.add(pn1);
        list2.add(pn2);
        int size = list1.size();
        for( int i = 0; i < size; i++ ) {
            System.out.println(i);
            plannerTester.diff(list1.get(i), list2.get(i), true);
            System.out.println(list1.get(i).toExplainPlanString());
            System.out.println(list2.get(i).toExplainPlanString());

        }
    }

    //    public void testBatchDiff() {
    //          int size = 7;
    //          String pathBaseline = "/tmp/volttest/test1Baseline/";
    //          String pathNew = "/tmp/volttest/test1New/";
    //          plannerTester.setTestName( "Test1" );
    //          try {
    //                  plannerTester.batchDiff(pathBaseline, pathNew, size);
    //          } catch (IOException e) {
    //                  e.printStackTrace();
    //          }
    //    }

//        public void testWholeProcess() {
//              try {
//                      plannerTester.setUp(m_homeDir+"/test1");
//                      plannerTester.batchCompileSave();
//                      plannerTester.batchDiff();
//              } catch (Exception e) {
//                      e.printStackTrace();
//              }
//        }

    //make sure plan files are already in baseline directories
    //    public void testDiffVoltExamples() {
    //          try {
    //                  plannerTester.setUp(m_homeDir+"/test1");
    //                  plannerTester.batchDiff();
    //
    //                  plannerTester.setUp(m_homeDir+"/Voter");
    //                  plannerTester.batchDiff();
    //
    //                  plannerTester.setUp(m_homeDir+"/voltcache");
    //                  plannerTester.batchDiff();
    //
    //                  plannerTester.setUp(m_homeDir+"/voltkv");
    //                  plannerTester.batchDiff();
    //          }
    //          catch ( Exception e ) {
    //                  e.printStackTrace();
    //          }
    //    }

    public void testMain() {
        String[] args = {"-cs","-d","-s","-e",
//                "-C="+m_currentDir+"/tests/frontend/org/voltdb/planner/config/voter",
//                "-C="+m_currentDir+"/tests/frontend/org/voltdb/planner/config/test1",
//                "-C="+m_currentDir+"/tests/frontend/org/voltdb/planner/config/voltcache",
//                "-C="+m_currentDir+"/tests/frontend/org/voltdb/planner/config/voltkv",
                "-C="+m_homeDir+"/"+"test1",
                "-r="+m_homeDir,
//                "-help"
                };
        plannerTester.main(args);
    }

    //    public void testwrite() {
    //          try {
    //                  BufferedWriter writer = new BufferedWriter( new FileWriter(m_homeDir+"/testwrite") );
    //                  writer.write("abc");
    //                  writer.write("\n");
    //                  writer.write("def");
    //
    //                  writer.flush();
    //                  writer.close();
    //                  writer = new BufferedWriter( new FileWriter(m_homeDir+"/testwrite") );
    //                  writer.append("\nabcde");
    //                  writer.flush();
    //                  writer.close();
    //          } catch (IOException e) {
    //                  //
    //                  e.printStackTrace();
    //          }
    //    }

    //    public void testPrint() {
    //          try {
    //                  System.out.println( new File(".").getCanonicalPath() );
    //          } catch (IOException e) {
    //                  e.printStackTrace();
    //          }
    //    }
}

