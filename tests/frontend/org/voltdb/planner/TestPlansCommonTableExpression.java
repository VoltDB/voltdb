/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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
            System.out.println(xml.toXML());
            assertXPaths(xml,
                    "/select[count(withClause/withList) = 1]",
                    "/select[count(withClause/withList/withListElement) = 1]",
                    "/select[count(withClause/withList/withListElement/table) = 1]",
                    "/select[count(withClause/withList/withListElement/select) = 2]",
                    "/select/withClause[@recursive='true']/withList/withListElement/table[1 and @name='RT']");
            CompiledPlan plan = compileAdHocPlan(SQL, true, true, DeterminismMode.SAFER);
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
            System.out.println(xml.toXML());
            assertXPaths(xml,
                    "/select[count(withClause/withList) = 1]",
                    "/select[count(withClause/withList/withListElement) = 1]",
                    "/select[count(withClause/withList/withListElement/table) = 1]",
                    "/select[count(withClause/withList/withListElement/select) = 2]",
                    "/select/withClause[@recursive='true']/withList/withListElement/table[1 and @name='RT']");
            CompiledPlan plan = compileAdHocPlan(SQL, false, true, DeterminismMode.SAFER);
            assertNull(plan.subPlanGraph);
            PlanNodeList pt = new PlanNodeList(plan.rootPlanGraph, false);
            String planStr = pt.toJSONString();
            JSONObject jsonPlan = new JSONObject(planStr);
            System.out.println(jsonPlan.toString(4));
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

    public void testNegative() throws Exception {
        String SQL;
        // Nested with statements are not allowed.
        SQL = "with recursive rt as ( select * from cte_table,"
                + "                                 ( with recursive bcase as (select * from cte_table"
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
    }
}
