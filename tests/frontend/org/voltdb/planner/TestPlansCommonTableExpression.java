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
 */package org.voltdb.planner;

import org.voltdb.types.PlanNodeType;

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


    /*
     * The VoltXML and plan for this query is:
    <?xml version="1.0" encoding="UTF-8" ?>
    <insert table="CTE_TABLE">
      <columns>
        <column name="ID"/>
        <column name="NAME"/>
        <column name="LEFT_RENT"/>
        <column name="RIGHT_RENT"/>
      </columns>
      <select>
        <columns>
          <columnref alias="ID"
                     column="ID"
                     id="1"
                     index="0"
                     table="CTE_TABLE"/>
          <columnref alias="NAME"
                     column="NAME"
                     id="2"
                     index="1"
                     table="CTE_TABLE"/>
          <columnref alias="LEFT_RENT"
                     column="LEFT_RENT"
                     id="3"
                     index="2"
                     table="CTE_TABLE"/>
          <columnref alias="RIGHT_RENT"
                     column="RIGHT_RENT"
                     id="4"
                     index="3"
                     table="CTE_TABLE"/>
        </columns>
        <parameters/>
        <tablescans>
          <tablescan jointype="inner"
                     table="CTE_TABLE"/>
        </tablescans>
      </select>
      <parameters/>
    </insert>

    Plan for <insert into cte_table ( select * from cte_table );>
      Plan for fragment 1 of 2
        Explain:
          RETURN RESULTS TO STORED PROCEDURE
           LIMIT 1
            RECEIVE FROM ALL PARTITIONS
        Nodes:
          Node type SEND
          Node type LIMIT
          Node type RECEIVE
      Plan for fragment 2 of 2
        Explain:
          RETURN RESULTS TO STORED PROCEDURE
           INSERT into "CTE_TABLE"
            INDEX SCAN of "CTE_TABLE" using its primary key index (for deterministic order only)
        Nodes:
          Node type SEND
          Node type INSERT
          Node type INDEXSCAN
            Inline PROJECTION
            Inline PROJECTION
     */
    public void testPlansCTE() {
        String SQL = "insert into cte_table ( select * from cte_table );";
        validatePlan(SQL, 2,
                     PlanNodeType.SEND,
                     PlanNodeType.LIMIT,
                     PlanNodeType.RECEIVE,
                     PlanNodeType.INVALID,
                     PlanNodeType.SEND,
                     PlanNodeType.INSERT,
                     new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                             PlanNodeType.PROJECTION));
    }

    /*
     * The VoltXML and plan for this query is:
    <?xml version="1.0" encoding="UTF-8" ?>
    <select>
      <columns>
        <columnref alias="ID"
                   column="ID"
                   id="1"
                   index="0"
                   table="CTE_TABLE"/>
        <columnref alias="NAME"
                   column="NAME"
                   id="2"
                   index="1"
                   table="CTE_TABLE"/>
        <columnref alias="LEFT_RENT"
                   column="LEFT_RENT"
                   id="3"
                   index="2"
                   table="CTE_TABLE"/>
        <columnref alias="RIGHT_RENT"
                   column="RIGHT_RENT"
                   id="4"
                   index="3"
                   table="CTE_TABLE"/>
      </columns>
      <parameters/>
      <tablescans>
        <tablescan jointype="inner"
                   table="CTE_TABLE">
          <wherecond>
            <operation id="7"
                       opsubtype="any"
                       optype="equal">
              <row id="5">
                <columnref alias="ID"
                           column="ID"
                           id="1"
                           index="0"
                           table="CTE_TABLE"/>
              </row>
              <tablesubquery id="6">
                <select>
                  <columns>
                    <columnref alias="ID"
                               column="ID"
                               id="8"
                               index="0"
                               table="CTE_TABLE"/>
                  </columns>
                  <parameters/>
                  <tablescans>
                    <tablescan jointype="inner"
                               table="CTE_TABLE"/>
                  </tablescans>
                </select>
              </tablesubquery>
            </operation>
          </wherecond>
        </tablescan>
      </tablescans>
    </select>

    Plan for <select * from cte_table where id in ( select id from cte_table );>
      Plan for fragment 1 of 1
        Explain:
          RETURN RESULTS TO STORED PROCEDURE
           INDEX SCAN of "CTE_TABLE" using its primary key index (for deterministic order only)
            filter by (EXISTS (Subquery_1
           on arguments (ID)
          ))

          Subquery_1
           INDEX SCAN of "CTE_TABLE" using its primary key index
           uniquely match (ID = ID)
           inline LIMIT 1
        Nodes:
          Node type SEND
          Node type INDEXSCAN
            Inline PROJECTION
            Inline PROJECTION
     */
    public void testPlansCTESubquery() {
        String SQL = "select * from cte_table where id in ( select id from cte_table );";
        validatePlan(SQL, 1,
                     PlanNodeType.SEND,
                     new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                             PlanNodeType.PROJECTION));
    }

    /*
     * This VoltXML and plan for this query is:
     *
    <?xml version="1.0" encoding="UTF-8" ?>
    <select>
      <columns>
        <columnref alias="ID"
                   column="ID"
                   id="1"
                   index="0"
                   table="CTE_TABLE"
                   tablealias="L"/>
        <columnref alias="NAME"
                   column="NAME"
                   id="2"
                   index="1"
                   table="CTE_TABLE"
                   tablealias="L"/>
        <columnref alias="LEFT_RENT"
                   column="LEFT_RENT"
                   id="3"
                   index="2"
                   table="CTE_TABLE"
                   tablealias="L"/>
        <columnref alias="RIGHT_RENT"
                   column="RIGHT_RENT"
                   id="4"
                   index="3"
                   table="CTE_TABLE"
                   tablealias="L"/>
        <columnref alias="ID"
                   column="ID"
                   id="5"
                   index="0"
                   table="CTE_TABLE"
                   tablealias="R"/>
        <columnref alias="NAME"
                   column="NAME"
                   id="6"
                   index="1"
                   table="CTE_TABLE"
                   tablealias="R"/>
        <columnref alias="LEFT_RENT"
                   column="LEFT_RENT"
                   id="7"
                   index="2"
                   table="CTE_TABLE"
                   tablealias="R"/>
        <columnref alias="RIGHT_RENT"
                   column="RIGHT_RENT"
                   id="8"
                   index="3"
                   table="CTE_TABLE"
                   tablealias="R"/>
      </columns>
      <parameters/>
      <tablescans>
        <tablescan jointype="inner"
                   table="CTE_TABLE"
                   tablealias="L"/>
        <tablescan jointype="inner"
                   table="CTE_TABLE"
                   tablealias="R">
          <joincond>
            <operation id="13"
                       opsubtype="any"
                       optype="equal">
              <row id="9">
                <columnref alias="ID"
                           column="ID"
                           id="1"
                           index="0"
                           table="CTE_TABLE"
                           tablealias="L"/>
              </row>
              <table id="12">
                <row id="10">
                  <columnref alias="LEFT_RENT"
                             column="LEFT_RENT"
                             id="7"
                             index="2"
                             table="CTE_TABLE"
                             tablealias="R"/>
                </row>
                <row id="11">
                  <columnref alias="RIGHT_RENT"
                             column="RIGHT_RENT"
                             id="8"
                             index="3"
                             table="CTE_TABLE"
                             tablealias="R"/>
                </row>
              </table>
            </operation>
          </joincond>
        </tablescan>
      </tablescans>
    </select>

    Plan for <select * from cte_table l join cte_table r on l.id IN (r.left_rent, r.right_rent)>
      Plan for fragment 1 of 1
        Explain:
          RETURN RESULTS TO STORED PROCEDURE
           NEST LOOP INNER JOIN
            filter by (L.ID IN ANY (R.LEFT_RENT, R.RIGHT_RENT))
            INDEX SCAN of "CTE_TABLE (L)" using its primary key index (for deterministic order only)
            INDEX SCAN of "CTE_TABLE (R)" using its primary key index (for deterministic order only)
        Nodes:
          Node type SEND
          Node type NESTLOOP
            Child 1: INDEXSCAN
          Node type INDEXSCAN
            Inline PROJECTION
            Inline PROJECTION
     */
    public void testJoin() {
        String SQL = "select * from cte_table l join cte_table r on l.id IN (r.left_rent, r.right_rent)";
        validatePlan(SQL, 1,
                     PlanNodeType.SEND,
                     PlanNodeType.NESTLOOP,
                     new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                             PlanNodeType.PROJECTION));
    }
    public void testMultiJoin() {
        String SQL = "select l.id, r.id, m.id from cte_table l join ( cte_table m join cte_table r on m.id = r.left_rent ) on l.id in (l.left_rent, r.right_rent)";
        validatePlan(SQL, 1,
                     PlanNodeType.SEND,
                     PlanNodeType.NESTLOOP);
    }
}
