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
import org.voltdb.plannodes.SendPlanNode;
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

    public void testGetScanNodeList() {
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

//    public void testBatchCompileSave() {
//        try {
//            plannerTester.setUp(m_homeDir+"/test1");
//            plannerTester.batchCompileSave();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    public void testCompile() {
        try {
            plannerTester.setUpForTest(m_currentDir+"/tests/frontend/org/voltdb/planner/testplans-plannerTester-ddl.sql",
                    "testplans-plannerTester-ddl", "L", "a");
            List<AbstractPlanNode> pnList = plannerTester.compile("select * from l, t where t.a=l.a limit ?;", 3, false);
            System.out.println( pnList.size() );
            System.out.println( pnList.get(0).toExplainPlanString() );

            assert( pnList.size() == 2 );
            assert( pnList.get(1) instanceof SendPlanNode );
            if ( pnList.get(0).reattachFragment( ( SendPlanNode )pnList.get(1) ) )
                System.out.println( pnList.get(0).toExplainPlanString() );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testWriteAndLoad() throws Exception {
        AbstractPlanNode pn = null;
        String path = m_homeDir+"/";
        plannerTester.setUpForTest(m_currentDir+"/tests/frontend/org/voltdb/planner/testplans-plannerTester-ddl.sql",
                "testplans-plannerTester-ddl", "L", "a");
        List<AbstractPlanNode> pnList = plannerTester.compile("select * from l, t where t.a=l.a limit ?;", 3, false);

        System.out.println( pnList.size() );

        assert( pnList.get(1) instanceof SendPlanNode );
        pnList.get(0).reattachFragment( (SendPlanNode) pnList.get(1) );
        pn = pnList.get(0);
        System.out.println( pn.toJSONString() );
        System.out.println( pn.toExplainPlanString() );
        plannerTester.writePlanToFile( pn, path, "prettyJson.txt", "");

        ArrayList<String> getsql = new ArrayList<String>();
        PlanNodeTree pnt = plannerTester.loadPlanFromFile(path+"prettyJson.txt", getsql);
        System.out.println( pnt.toJSONString() );
        System.out.println( pnt.getRootPlanNode().toExplainPlanString() );
        ArrayList<AbstractPlanNode> list1 = pn.getPlanNodeList();
        ArrayList<AbstractPlanNode> list2 = pnt.getRootPlanNode().getPlanNodeList();
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
        ArrayList<AbstractPlanNode> list1 = pn.getPlanNodeList();
        ArrayList<AbstractPlanNode> list2 = pnt.getRootPlanNode().getPlanNodeList();
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

    public void testGetList() {
        AbstractPlanNode pn1 = null;
        pn1 = compile("select * from l, t where t.b=l.b limit ?;", 3, true);

        ArrayList<AbstractPlanNode> pnlist = pn1.getPlanNodeList();

        System.out.println( pn1.toExplainPlanString() );
        System.out.println( pnlist.size() );
        for( int i = 0; i<pnlist.size(); i++ ){
            System.out.println( pnlist.get(i).toJSONString() );
        }
        assertTrue( pnlist.size() == 6 );
    }

    public void testScanDiff() {
        assertTrue( compileDiffMatchPattern(
                "select * from t where a = ?;", 3, true,
                "select * from l,t where l.a = t.a order by b limit ?;", 3, true,
                "Scan time diff :",
                "(1 => 2)",
                "Table diff at leaf 0:",
                "(INDEXSCAN on T => SEQSCAN on L)",
                "Diff at leaf 1 :",
                "(Empty => INDEXSCAN on T using COVER2_TREE)"
                ) );
    }

    public void testJoinDiff() {
        assertTrue( compileDiffMatchPattern(
                "select count(*) from t;", 3, true,
                "select * from l,t where l.a = t.a order by b limit ?;", 3, true,
                "Join Node List diff:",
                "([] => NESTLOOPINDEX[5])"
                ) );
        assertTrue( compileDiffMatchPattern(
                "select * from l, t where t.a=l.a;", 3, true,
                "select * from l, t where t.b=l.b order by a limit ?;", 3, true,
                "Join Node Type diff:",
                "(NESTLOOPINDEX[3] => NESTLOOP[5])"
                ) );
    }

    public void testPlanNodeandInlinePositionDiff() {
      assertTrue( compileDiffMatchPattern(
      "select * from l order by a;", 3, true,
      "select * from l order by a limit ?;", 3, true,
      "ORDERBY diff:",
      "([3] => [4])",
      "SEQSCAN diff:",
      "([4] => [5])",
      "PROJECTION diff:",
      "([2] => [3])",
      "LIMIT diff:" ,
      "([] => [2])",
      "Inline PROJECTION diff:",
      "([SEQSCAN[4]] => [SEQSCAN[5]])"
      ) );
    }

    public void testComprehensiveDiff() {
      assertTrue( compileDiffMatchPattern(
              "select * from l, t where t.a=l.a;", 3, true,
              "select * from l, t where t.b=l.b order by a limit ?;", 3, true,
              "Diff scan at leaf 1 :",
              "(INDEXSCAN on T => SEQSCAN on T)",
              "Plan tree size diff: (4 => 7)",
              "ORDERBY diff:",
              "([] => [4])",
              "NESTLOOP diff:",
              "([] => [5])",
              "PROJECTION diff:",
              "([2] => [3])",
              "LIMIT diff:",
              "([] => [2])",
              "NESTLOOPINDEX diff:",
              "([3] => [])",
              "Inline INDEXSCAN diff:",
              "([NESTLOOPINDEX[3]] => [])",
              "Inline PROJECTION diff:",
              "([SEQSCAN[4]] => [SEQSCAN[6], SEQSCAN[7]])",
              "Join Node Type diff:",
              "(NESTLOOPINDEX[3] => NESTLOOP[5])"
              ));
    }

    public boolean compileDiffMatchPattern( String sql1, int paraCount1, boolean sp1,
                                          String sql2, int paraCount2,  boolean sp2,
                                          String... patterns) {
        AbstractPlanNode pn1 = compile( sql1, paraCount1, sp1 );
        AbstractPlanNode pn2 = compile( sql2, paraCount2, sp2 );
        plannerTester.diff( pn1, pn2, true );
        int numMatched = 0;
        for( String str : plannerTester.m_diffMessages ) {
            for( String pattern : patterns )
                if( str.contains( pattern ) )
                    numMatched++;
        }
        if( numMatched >= patterns.length )
            return true;
        else
            return false;
    }

  public void testMain() {
  String[] args = {"-d","-s","-e","-cs",
          "-C="+m_currentDir+"/tests/scripts/plannertester/config/test1",
//          "-C="+m_currentDir+"/tests/scripts/plannertester/config/voltcache",
//          "-C="+m_currentDir+"/tests/scripts/plannertester/config/voltkv",
//          "-C="+m_currentDir+"/tests/scripts/plannertester/config/voter",
          "-r="+m_homeDir,
//          "-help"
          };
  plannerTester.main(args);
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

}
