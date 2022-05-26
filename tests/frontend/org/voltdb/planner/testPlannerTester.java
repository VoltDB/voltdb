/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.PlanNodeType;
public class testPlannerTester extends PlannerTestCase {
    private String m_currentDir;
    private String m_homeDir;

    @Override
    protected void setUp() throws Exception {
        setupSchema(testPlannerTester.class.getResource("testplans-plannerTester-ddl.sql"),
                    "testplans-plannerTester-ddl", true);
        m_currentDir = new File(".").getCanonicalPath() + "/";
        m_homeDir = System.getProperty("user.home") + "/";
    }

    /// Unit test some of the techniques used by plannerTester
    public void testGetScanNodeList() {
        AbstractPlanNode pn = null;
        pn = compile("select * from l where lname=? and b=0 order by id asc limit ?;");

        List<AbstractScanPlanNode> collected = pn.getScanNodeList();
        System.out.println(collected);
        System.out.println(collected.size());
        for (AbstractPlanNode n : collected) {
            System.out.println(n.toExplainPlanString());
        }
        assertTrue(collected.size() == 1);
        JSONObject j;
        try {
            j = new JSONObject(collected.get(0).toJSONString());
            System.out.println(j.getString("PLAN_NODE_TYPE"));
            assertTrue(j.getString("PLAN_NODE_TYPE").equalsIgnoreCase("INDEXSCAN"));
        }
        catch (JSONException e) {
            e.printStackTrace();
        }
    }

    public void testReAttachFragments() {
        try {
            plannerTester.setUpForTest(m_currentDir +
                    "tests/frontend/org/voltdb/planner/testplans-plannerTester-ddl.sql",
                    "testplans-plannerTester-ddl");
            List<AbstractPlanNode> pnList = plannerTester.testCompile("select * from l, t where t.a=l.a limit ?;");
            System.out.println(pnList.size());
            System.out.println(pnList.get(0).toExplainPlanString());

            assert(pnList.size() == 2);
            assert(pnList.get(1) instanceof SendPlanNode);
            if (pnList.get(0).reattachFragment(pnList.get(1))) {
                AbstractPlanNode pn = pnList.get(0);
                System.out.println(pn.toExplainPlanString());
                assertTrue(pn.toExplainPlanString().contains("SEND PARTITION RESULTS TO COORDINATOR"));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testWriteAndLoad() throws Exception {
        AbstractPlanNode pn = null;
        plannerTester.setUpForTest(m_currentDir +
                "tests/frontend/org/voltdb/planner/testplans-plannerTester-ddl.sql",
                "testplans-plannerTester-ddl");
        List<AbstractPlanNode> pnList = plannerTester.testCompile("select * from l, t where t.a=l.a limit ?;");

        System.out.println(pnList.size());

        pn = pnList.get(0);
        assert(pnList.get(1) instanceof SendPlanNode);
        pn.reattachFragment(pnList.get(1));
        System.out.println(pn.toJSONString());
        System.out.println(pn.toExplainPlanString());
        plannerTester.writePlanToFile(pn, m_homeDir, "prettyJson.txt", "");

        List<String> getsql = new ArrayList<String>();
        AbstractPlanNode pn2 = plannerTester.loadPlanFromFile(m_homeDir + "prettyJson.txt", getsql);
        System.out.println(pn2.toExplainPlanString());
        List<AbstractPlanNode> list1 = pn.getPlanNodeList();
        List<AbstractPlanNode> list2 = pn2.getPlanNodeList();
        assertTrue(list1.size() == list2.size());
        for (int i = 0; i < list1.size(); i++) {
            Map<PlanNodeType, AbstractPlanNode> inlineNodes1 = list1.get(i).getInlinePlanNodes();
            Map<PlanNodeType, AbstractPlanNode> inlineNodes2 = list2.get(i).getInlinePlanNodes();
            if (inlineNodes1 != null) {
                assertTrue(inlineNodes1.size() == inlineNodes2.size());
            }
        }
    }

    public void testLoadJoinType() throws FileNotFoundException {
        AbstractPlanNode pn = null;
        pn = compile("select * from l, t where l.b=t.b limit ?;");

        plannerTester.setUpForTest(m_currentDir +
                "tests/frontend/org/voltdb/planner/testplans-plannerTester-ddl.sql",
                "testplans-plannerTester-ddl");
        System.out.println(pn.toExplainPlanString());
        System.out.println(pn.toJSONString());
        plannerTester.writePlanToFile(pn, m_homeDir, "prettyJson.txt", "");

        List<String> getsql = new ArrayList<>();
        AbstractPlanNode pn2 = plannerTester.loadPlanFromFile(m_homeDir + "prettyJson.txt", getsql);
        System.out.println(pn2.toExplainPlanString());
        List<AbstractPlanNode> list1 = pn.getPlanNodeList();
        List<AbstractPlanNode> list2 = pn2.getPlanNodeList();
        assertTrue(list1.size() == list2.size());
        for (int i = 0; i < list1.size(); i++) {
            Map<PlanNodeType, AbstractPlanNode> inlineNodes1 = list1.get(i).getInlinePlanNodes();
            Map<PlanNodeType, AbstractPlanNode> inlineNodes2 = list2.get(i).getInlinePlanNodes();
            if (inlineNodes1 != null) {
                assertTrue(inlineNodes1.size() == inlineNodes2.size());
            }
        }
    }

    /// Unit test some of the techniques used by plannerTester
    public void testGetList() {
        AbstractPlanNode pn1 = null;
        pn1 = compile("select * from l, t where t.b=l.b limit ?;");

        List<AbstractPlanNode> pnlist = pn1.getPlanNodeList();

        System.out.println(pn1.toExplainPlanString());
        System.out.println(pnlist.size());
        for (int i = 0; i < pnlist.size(); i++) {
            System.out.println(pnlist.get(i).toJSONString());
        }
        assertTrue(pnlist.size() == 6);
    }

    public void testScanDiff()
    {
        assertTrue(compileDiffMatchPattern("select * from t where a = ?;",
                                            "select * from l,t where l.a = t.a order by b limit ?;",
                                            "Scan time diff :",
                                            "(1 => 2)",
                                            "Table diff at leaf 0:",
                                            "(INDEXSCAN on T => INDEXSCAN on L)",
                                            "Table diff at leaf 1:",
                                            "([] => INDEXSCAN on T)"));
    }

    /// Unit test plannerTester as a plan diff engine.
    public void testJoinDiff()
    {
        assertTrue(compileDiffMatchPattern("select count(*) from t;",
                                            "select * from l,t where l.a = t.a order by b limit ?;",
                                            "Join Node List diff:",
                                            "([] => NESTLOOPINDEX[5])"));
        assertTrue(compileDiffMatchPattern("select * from l, t where t.a=l.a;",
                                            "select * from l, t where t.b=l.b order by a limit ?;",
                                            "Join Node Type diff:",
                                            "(NESTLOOPINDEX[3] => NESTLOOP[5])"));
    }

    /// Unit test plannerTester as a plan diff engine.
    public void testPlanNodeAndInlinePositionDiff()
    {
        assertTrue(compileDiffMatchPattern("select * from l order by a;",
                                            "select * from l order by a limit ?;",
                                            "ORDERBY diff:",
                                            "([3] => [4])",
                                            "INDEXSCAN diff:",
                                            "([4] => [5])",
                                            "PROJECTION diff:",
                                            "([2] => [3])",
                                            "LIMIT diff:" ,
                                            "([] => [2])",
                                            "Inline PROJECTION diff:",
                                            "([INDEXSCAN[4]] => [INDEXSCAN[5]])"));
    }

    /// Unit test plannerTester as a plan diff engine.
    public void testComprehensiveDiff()
    {
        assertTrue(compileDiffMatchPattern("select * from l, t where t.a=l.a;",
                                            "select * from l, t where t.b=l.b order by a limit ?;",
                                            "Table diff at leaf 0:",
                                            "(INDEXSCAN on L => SEQSCAN on T)",
                                            "Table diff at leaf 1:",
                                            "(INDEXSCAN on T => INDEXSCAN on L)",
                                            "Plan tree size diff: (4 => 7)",
                                            "ORDERBY diff:",
                                            "([] => [4])",
                                            "NESTLOOP diff:",
                                            "([] => [5])",
                                            "SEQSCAN diff:",
                                            "([] => [6])",
                                            "PROJECTION diff:",
                                            "([2] => [3])",
                                            "INDEXSCAN diff:",
                                            "([4] => [7])",
                                            "LIMIT diff:",
                                            "([] => [2])",
                                            "NESTLOOPINDEX diff:",
                                            "([3] => [])",
                                            "Inline INDEXSCAN diff:",
                                            "([NESTLOOPINDEX[3]] => [])",
                                            "Inline PROJECTION diff:",
                                            "([INDEXSCAN[4]] => [SEQSCAN[6], INDEXSCAN[7]])",
                                            "Join Node Type diff:",
                                            "(NESTLOOPINDEX[3] => NESTLOOP[5])"));
    }

    public boolean compileDiffMatchPattern(String sql1, String sql2, String... patterns)
    {
        AbstractPlanNode pn1 = compile(sql1);
        AbstractPlanNode pn2 = compile(sql2);
        plannerTester.diff(pn1, pn2, true);
        int numMatched = 0;
        for (String str : plannerTester.m_diffMessages) {
            String[] splits = str.split("\n");
            for (String split : splits) {
                int wasMatched = numMatched;
                for (String pattern : patterns) {
                    if (split.trim().equals(pattern)) {
                        numMatched++;
                        break;
                    }
                }
                if (wasMatched == numMatched) {
                    System.out.println("Skipped harmless noise? :" + split);
                }
            }
        }
        return (numMatched == patterns.length);
    }

}
