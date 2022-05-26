/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.voltdb.planner;

import static org.hamcrest.xml.HasXPath.hasXPath;

import java.io.ByteArrayInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.hamcrest.MatcherAssert;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.plannodes.PlanNodeList;
import org.w3c.dom.Document;

// @Ignore
public class TestPlansCommonTableExpression extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(getClass().getResource("testplans-cte.sql"),
                    "testcte", false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private static Document parse(String xml) throws Exception {
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(false);
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        return documentBuilder.parse(new ByteArrayInputStream(xml.getBytes()));
    }

    private void assertXPaths(VoltXMLElement xml,
                              String ...paths) throws Exception {
        Document xmlDoc = parse(xml.toXML());
        for (String path : paths) {
            MatcherAssert.assertThat(xmlDoc,
                          hasXPath(path));
        }
    }

    private String formatPlanList(PlanNodeList pnl) throws JSONException {
        String raw = pnl.toJSONString();
        JSONObject obj = new JSONObject(raw);
        return obj.toString(4);
    }

    public String formatPlan(CompiledPlan plan) throws JSONException {
        StringBuilder sb = new StringBuilder();
        PlanNodeList coordPlan = new PlanNodeList(plan.rootPlanGraph, false);
        PlanNodeList distPlan
                = ((plan.subPlanGraph == null)
                        ? null
                        : new PlanNodeList(plan.subPlanGraph, false));
        sb.append("Coordinator plan:\n")
          .append(formatPlanList(coordPlan))
          .append("\n");
        if (distPlan != null) {
            sb.append("Distributed Plan:\n")
              .append(formatPlanList(distPlan))
              .append("\n");
        }
        return(sb.toString());
    }

    public void testRepl() throws Exception {
        String SQL = "WITH RECURSIVE RT(ID, NAME) AS "
                     + "("
                     + "  SELECT ID, NAME FROM CTE_TABLE WHERE ID = ?"
                     + "    UNION ALL "
                     + "  SELECT RT.ID, RT.NAME "
                     + "  FROM RT JOIN CTE_TABLE "
                     + "          ON RT.ID IN (CTE_TABLE.LEFT_RENT, CTE_TABLE.RIGHT_RENT)"
                     + ") "
                     + "SELECT * FROM RT;";
        try {
            VoltXMLElement xml = compileToXML(SQL);
            assertXPaths(xml,
                    "/select[count(withClause/withList) = 1]",
                    "/select[count(withClause/withList/withListElement) = 1]",
                    "/select[count(withClause/withList/withListElement/table) = 1]",
                    "/select[count(withClause/withList/withListElement/select) = 2]",
                    "/select/withClause[@recursive='true']/withList/withListElement/table[1 and @name='RT']");
            compileAdHocPlanThrowing(SQL, true, true, DeterminismMode.SAFER);
        } catch (HSQLParseException e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testPlansCTE() throws Exception {
        String SQL = "WITH RECURSIVE RT(ID, NAME) AS "
                     + "("
                     + "  SELECT ID, NAME FROM CTE_TABLE WHERE ID = ?"
                     + "    UNION ALL "
                     + "  SELECT RT.ID, RT.NAME "
                     + "  FROM RT JOIN CTE_TABLE "
                     + "          ON RT.ID IN (CTE_TABLE.LEFT_RENT, CTE_TABLE.RIGHT_RENT)"
                     + ") "
                     + "SELECT * FROM RT;";
        try {
            VoltXMLElement xml = compileToXML(SQL);
            assertXPaths(xml,
                    "/select[count(withClause/withList) = 1]",
                    "/select[count(withClause/withList/withListElement) = 1]",
                    "/select[count(withClause/withList/withListElement/table) = 1]",
                    "/select[count(withClause/withList/withListElement/select) = 2]",
                    "/select/withClause[@recursive='true']/withList/withListElement/table[1 and @name='RT']");
            CompiledPlan plan = compileAdHocPlanThrowing(SQL, false, true, DeterminismMode.SAFER);
            assertNull(plan.subPlanGraph);
            PlanNodeList pt = new PlanNodeList(plan.rootPlanGraph, false);
            String planStr = pt.toJSONString();
            JSONObject jsonPlan = new JSONObject(planStr);
            JSONArray elists = jsonPlan.getJSONArray("EXECUTE_LISTS");
            assertEquals(3, elists.length());
            JSONArray mainQ = elists.getJSONObject(0).getJSONArray("EXECUTE_LIST");
            assertEquals(2, mainQ.length());
            JSONArray baseQ = elists.getJSONObject(1).getJSONArray("EXECUTE_LIST");
            assertEquals(2, baseQ.length());
            JSONArray recQ  = elists.getJSONObject(2).getJSONArray("EXECUTE_LIST");
            assertEquals(4, recQ.length());
        } catch (HSQLParseException e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testCTEPartitioning() {
        CompiledPlan plan;
        String SQL;
        ////////////////
        //
        // Positive tests
        //
        ////////////////
        // This should work ok.  Everything is
        // partitioned, but the query is forced SP.
        SQL = "WITH RECURSIVE RT AS ("
                + "  SELECT ID, EMP_ID, MGR_ID, 0, 0, 0 FROM R_EMPLOYEES "
                + "UNION ALL "
                + "  SELECT L.*, R.ID, R.EMP_ID, R.MGR_ID FROM R_EMPLOYEES L, RT R "
                + ") "
                + "SELECT * FROM R_EMPLOYEES, RT";
        plan = compileAdHocPlan(SQL, false, true, DeterminismMode.SAFER);
        // This should work fine as well.  Everything is
        // Partitioned, but it's forced to be single partitioned.
        SQL = "WITH RECURSIVE RT AS ("
                + "  SELECT ID, EMP_ID, MGR_ID, 0, 0, 0 FROM P_EMPLOYEES "
                + "UNION ALL "
                + "  SELECT L.*, R.ID, R.EMP_ID, R.MGR_ID FROM P_EMPLOYEES L, RT R"
                + ") "
                + "SELECT * FROM P_EMPLOYEES, RT";
        plan = compileAdHocPlan(SQL, false, true, DeterminismMode.SAFER);
        // This is slightly more complex.  We are
        // going to try to infer partioning, and we
        // are not forcing this to be SP.  So it will
        // try to run MP.  But this should be ok,
        // since the base and recursive table is
        // replicated.
        SQL = "WITH RECURSIVE RT AS ("
                + "  SELECT ID, EMP_ID, MGR_ID, 0, 0, 0 FROM R_EMPLOYEES "
                + "UNION ALL "
                + "  SELECT L.*, R.ID, R.EMP_ID, R.MGR_ID FROM R_EMPLOYEES L, RT R"
                + ") "
                + "SELECT * FROM R_EMPLOYEES, RT";
        plan = compileAdHocPlan(SQL, true, false, DeterminismMode.SAFER);
        // This is like the previous case, but the main
        // query scans a partitioned table.  So, the
        // query is MP.
        SQL = "WITH RECURSIVE RT AS ("
                + "  SELECT ID, EMP_ID, MGR_ID, 0, 0, 0 FROM R_EMPLOYEES "
                + "UNION ALL "
                + "  SELECT L.*, R.ID, R.EMP_ID, R.MGR_ID FROM R_EMPLOYEES L, RT R"
                + ") "
                + "SELECT * FROM P_EMPLOYEES, RT";
        plan = compileAdHocPlan(SQL, true, false, DeterminismMode.SAFER);

        ////////////////
        //
        // Negative tests.
        //
        ////////////////
        // Since the main query is partitioned, and we are trying
        // to infer and we are not forcing SP, then this is an MP
        // query.  The base query scans a partitioned table, so that's
        // just a failure.
        String NPErrorMessage = "The query defining a common table in a multi-partitioned query can only use replicated tables.";
        try {
            SQL = "WITH RECURSIVE RT AS ("
                + "  SELECT ID, EMP_ID, MGR_ID, 0, 0, 0 FROM P_EMPLOYEES "
                + "UNION ALL "
                + "  SELECT L.*, R.ID, R.EMP_ID, R.MGR_ID FROM R_EMPLOYEES L, RT R"
                + ") "
                + "SELECT * FROM P_EMPLOYEES, RT";

            compileAdHocPlanThrowing(SQL, true, false, DeterminismMode.SAFER);
            fail("Expected failure with partitioned common tables, MP query.");
        } catch (PlanningErrorException ex) {
            assertTrue(ex.getMessage().contains(NPErrorMessage));
        }
        // This is like the last, but the recursive scan is the other
        // order.
        try {
            SQL = "WITH RECURSIVE RT AS ("
                + "  SELECT ID, EMP_ID, MGR_ID, 0, 0, 0 FROM P_EMPLOYEES "
                + "UNION ALL "
                + "  SELECT L.*, R.ID, R.EMP_ID, R.MGR_ID FROM RT L, R_EMPLOYEES R "
                + ") "
                + "SELECT * FROM P_EMPLOYEES, RT";
            plan = compileAdHocPlanThrowing(SQL, true, false, DeterminismMode.SAFER);
            fail("Expected failure with partitioned common tables, MP query.");
        } catch (PlanningErrorException ex) {
            assertTrue(ex.getMessage().contains(NPErrorMessage));
        }
        // This is similar to the last two, but the main query
        // scans a replicated table.  This could be SP, but the partitioned
        // table in the base case query forces MP.
        try {
            SQL = "WITH RECURSIVE RT AS ("
                + "  SELECT ID, EMP_ID, MGR_ID, 0, 0, 0 FROM P_EMPLOYEES "
                + "UNION ALL "
                + "  SELECT L.*, R.ID, R.EMP_ID, R.MGR_ID FROM R_EMPLOYEES L, RT R"
                + ") "
                + "SELECT * FROM R_EMPLOYEES, RT";
            plan = compileAdHocPlanThrowing(SQL, true, false, DeterminismMode.SAFER);
            fail("Expected failure with partitioned common tables, MP query.");
        } catch (PlanningErrorException ex) {
            assertTrue(ex.getMessage().contains(NPErrorMessage));
        }
        // This is like the last, but the recursive scan is the other
        // order.
        try {
            SQL = "WITH RECURSIVE RT AS ("
                + "  SELECT ID, EMP_ID, MGR_ID, 0, 0, 0 FROM P_EMPLOYEES "
                + "UNION ALL "
                + "  SELECT L.*, R.ID, R.EMP_ID, R.MGR_ID FROM RT L, R_EMPLOYEES R "
                + ") "
                + "SELECT * FROM R_EMPLOYEES, RT";
            plan = compileAdHocPlanThrowing(SQL, true, false, DeterminismMode.SAFER);
            fail("Expected failure with partitioned common tables, MP query.");
        } catch (PlanningErrorException ex) {
            assertTrue(ex.getMessage().contains(NPErrorMessage));
        }
        // Since the main query is partitioned, and we are trying
        // to infer and we are not forcing SP, then this is an MP
        // query.  The base query scans a replicated table, which is
        // ok, but the recursive query scans a partitioned table,
        // which is just a failure.
        // just a failure.
        try {
            SQL = "WITH RECURSIVE RT AS ("
                + "  SELECT ID, EMP_ID, MGR_ID, 0, 0, 0 FROM R_EMPLOYEES "
                + "UNION ALL "
                + "  SELECT L.*, R.ID, R.EMP_ID, R.MGR_ID FROM P_EMPLOYEES L, RT R"
                + ") "
                + "SELECT * FROM P_EMPLOYEES, RT";
            plan = compileAdHocPlanThrowing(SQL, true, false, DeterminismMode.SAFER);
            fail("Expected failure with partitioned common tables, MP query.");
        } catch (PlanningErrorException ex) {
            assertTrue(ex.getMessage().contains(NPErrorMessage));
        }
        // This is like the last one, but the recursive scan is in the
        // other order.
        try {
            SQL = "WITH RECURSIVE RT AS ("
                + "  SELECT ID, EMP_ID, MGR_ID, 0, 0, 0 FROM R_EMPLOYEES "
                + "UNION ALL "
                + "  SELECT L.*, R.ID, R.EMP_ID, R.MGR_ID FROM RT L, P_EMPLOYEES R "
                + ") "
                + "SELECT * FROM P_EMPLOYEES, RT";
            plan = compileAdHocPlanThrowing(SQL, true, false, DeterminismMode.SAFER);
            fail("Expected failure with partitioned common tables, MP query.");
        } catch (PlanningErrorException ex) {
            assertTrue(ex.getMessage().contains(NPErrorMessage));
        }

    }

    public void testSubqueries() throws Exception {
        try {
            String SQL =
                      "with recursive cte as ( "
                    + "    ( select * from cte_table where 0 < id order by id limit 1 ) "
                    + "  union all "
                    + "    ( select * from cte where id < 100 order by id limit 1 ) ) "
                    + "select * from cte; "
                    ;
            CompiledPlan plan = compileAdHocPlanThrowing(SQL, false, true, DeterminismMode.SAFER);
            assertNotNull(plan);
        } catch (PlanningErrorException ex) {
            fail("Unexpected error: " + ex.getMessage());
        }
    }

    public void testNegative() throws Exception {
        String SQL;
        // Nested with statements are not allowed.
        SQL = "with recursive rt as ( select * from cte_table,"
                + "                                 ( with recursive bcase as (select * from te_table"
                + "                                                             union all"
                + "                                                          select * from bcase, cte_table)"
                + "                                      select * from bcase ) badWith )"
                + "select * from rt;";
        // Multiple common tables are not allowed.
        failToCompile(SQL, "With statements may not be nested.");
        SQL = "with recursive rt as ( select * from cte_table union all select * from rt, cte_table ),"
                + "           st as ( select * from cte_table union all select * from rt, cte_table ),"
                + "           tt as ( select * from cte_table union all select * from rt, cte_table )"
                + "select * from rt, st, tt;";
        failToCompile(SQL, "Only one common table is allowed.");

        // Similar to a failing query that sqlcoverage caught.
        SQL= "WITH CTE1 AS ( "
                + "SELECT LEFT_RENT, SUM(ID) AG1 FROM CTE_TABLE GROUP BY LEFT_RENT), "
                + "CTE2 AS (  "
                + "SELECT LEFT_RENT FROM CTE1 WHERE AG1 > (SELECT MIN(AG1) FROM CTE1) ) "
                + "SELECT L, NAME, SUM(ID) AS AG1, SUM(R) AS AG2 FROM RRT "
                + "WHERE R IN (SELECT LEFT_RENT FROM CTE2) GROUP BY L, NAME";
        failToCompile(SQL, "Only one common table is allowed.");
    }

    public void testNegativeDML() throws Exception {
        String sql;

        sql = "with the_cte as (select * from cte_table) insert into cte_table select * from the_cte";
        failToCompile(sql, "unexpected token: INSERT");

        sql = "with recursive the_cte as "
                + "(select 3 as n from cte_table "
                + "union all "
                + "select n - 1 from the_cte where n > 0) "
                + "insert into cte_table select * from the_cte";
        failToCompile(sql, "unexpected token: INSERT");
    }

    public void testOtherSetOperators() throws Exception {
        String sql;

        String format = "with recursive the_cte as ( "
                + "select * from cte_table "
                + "%s "
                + "select * from the_cte) "
                + "select * from the_cte";
        String[] setOps = {"UNION", "INTERSECT", "EXCEPT"};
        for (String setOp : setOps) {
            sql = String.format(format, setOp);
            if (setOp.equals("UNION")) {
                failToCompile(sql, "unexpected token: SELECT required: ALL");
            }
            else {
                failToCompile(sql, "unexpected token: " + setOp + " required: UNION");
            }
        }

        sql = "with recursive the_cte as ( "
                + "select * from cte_table "
                + ") "
                + "select * From the_cte";
        failToCompile(sql, "unexpected token: ) required: UNION");
    }

    public void testGroupByAndOrderBy() {
        String sql;

        // Queries that pass have corresponding regressionsuite tests.
        // This method locks down error messages for queries that fail
        // during parsing or planning.

        // Recursive CTEs (base query)
        // - GB clause
        // - OB clause
        // - Both OB and GB clauses

        // ORDER BY
        // One needs parentheses here to be grammatically correct.
        sql = "with recursive the_cte as ( "
                + "select left_rent, id "
                + "from cte_table "
                + "order by left_rent "
                + "union all "
                + "select left_rent, id - 1 "
                + "from the_cte "
                + "where cnt > 0 "
                + ") "
                + "select * from the_cte";
        failToCompile(sql, "unexpected token: ORDER required: UNION");

        // Both ORDER BY and GROUP BY
        // One needs parentheses here to be grammatically correct.
        sql = "with recursive the_cte as ( "
                + "select left_rent, count(*) as cnt "
                + "from cte_table "
                + "group by left_rent "
                + "order by cnt desc limit 1 "
                + "union all "
                + "select left_rent, cnt - 1 "
                + "from the_cte "
                + "where cnt > 0 "
                + ") "
                + "select * from the_cte";
        failToCompile(sql, "unexpected token: ORDER required: UNION");

        // Recursive CTEs (recursive query)
        // - GB clause
        // - OB clause
        // - Both OB and GB clauses

        // OB clause
        // Parenthesis required around query with ORDER BY
        sql = "with recursive the_cte as ( "
                + "select id, left_rent, right_rent from cte_table "
                + "union all "
                + "select left_rent, id, right_rent "
                + "from the_cte "
                + "order by left_rent, right_rent desc "
                + ") "
                + "select * from the_cte";
        failToCompile(sql, "expected token: ORDER required: )");

        // OB and GB clause
        // Parenthesis required around query with ORDER BY
        sql = "with recursive the_cte as ( "
                + "select id, left_rent, right_rent from cte_table "
                + "union all "
                + "select left_rent, max(id) as min_id, min(id) "
                + "from the_cte "
                + "group by left_rent "
                + "order by min_id "
                + ") "
                + "select * from the_cte";
        failToCompile(sql, "unexpected token: ORDER required: )");
    }
}
