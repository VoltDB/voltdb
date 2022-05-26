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

import java.util.List;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.PlanNodeType;

public class TestPlansDistinct extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(TestPlansDistinct.class.getResource("testplans-groupby-ddl.sql"),
                "testplansgroupby", false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testColumnsWithoutGroupby()
    {
        String sql1, sql2;

        // single column DISTINCT
        // A3 is partition key
        sql1 = "SELECT distinct A3 from T3";
        sql2 = "SELECT A3 from T3 group by A3";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // B3 is not partition key
        sql1 = "SELECT distinct B3 from T3";
        sql2 = "SELECT B3 from T3 group by B3";
        checkQueriesPlansAreTheSame(sql1, sql2);

        //
        // Multiple columns DISTINCT
        //
        sql1 = "SELECT distinct A3, B3 from T3";
        sql2 = "SELECT A3, B3 from T3 group by A3, B3";
        checkQueriesPlansAreTheSame(sql1, sql2);

        sql1 = "SELECT distinct A3, B3 from T3";
        sql2 = "SELECT A3, B3 from T3 group by A3, B3";
        checkQueriesPlansAreTheSame(sql1, sql2);

        sql2 = "SELECT A3, B3 from T3 group by B3, A3";
        checkQueriesPlansAreTheSame(sql1, sql2);

        sql2 = "SELECT B3, A3 from T3 group by B3, A3";
        checkQueriesPlansAreTheSame(sql1, sql2);

        sql2 = "SELECT B3, A3 from T3 group by A3, B3";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // LIMIT/OFFSET
        sql1 = "SELECT distinct A3, B3 from T3 LIMIT 10";
        sql2 = "SELECT B3, A3 from T3 group by A3, B3 LIMIT 10";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // Distinct *
        sql1 = "SELECT distinct * from T3";
        sql2 = "SELECT pkey, A3, B3, C3, D3 from T3 group by pkey, A3, B3, C3, D3";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // Distinct on table aggregation
        // single table aggregate select
        sql1 = "SELECT distinct SUM(A3) from T3";
        sql2 = "SELECT SUM(A3) from T3";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // multiple table aggregate select
        sql1 = "SELECT distinct count(*), SUM(A3) from T3";
        sql2 = "SELECT count(*), SUM(A3) from T3";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // table aggregate with HAVING
        sql1 = "select Distinct min(A3), max(A3) from T3 having min(A3) > 0";
        sql2 = "select min(A3), max(A3) from T3 having min(A3) > 0";
        checkQueriesPlansAreTheSame(sql1, sql2);
    }

    public void testExpressionsWithoutGroupby()
    {
        String sql1, sql2;
        // distinct with expression
        sql1 = "SELECT distinct A3, floor(B3) from T3";
        sql2 = "SELECT A3, floor(B3) from T3 group by A3, floor(B3)";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // distinct with expression on partition column
        sql1 = "SELECT distinct floor(A3), floor(B3) from T3";
        sql2 = "SELECT floor(A3), floor(B3) from T3 group by floor(A3), floor(B3)";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // expression with parameters
        sql1 = "SELECT distinct A3, floor(B3+1) from T3";
        sql2 = "SELECT A3, floor(B3+1) from T3 group by A3, floor(B3+1)";
        checkQueriesPlansAreTheSame(sql1, sql2);
    }

    public void testMatViewsWithoutGroupby()
    {
        String sql1, sql2;
        // View: V_P1_NO_FIX_NEEDED
        sql1 = "SELECT DISTINCT V_A1, V_SUM_C1 FROM V_P1_NO_FIX_NEEDED";
        sql2 = "SELECT V_A1, V_SUM_C1 FROM V_P1_NO_FIX_NEEDED GROUP BY V_A1, V_SUM_C1";
        checkQueriesPlansAreTheSame(sql1, sql2);

        sql1 = "SELECT DISTINCT ABS(V_A1), V_SUM_C1 FROM V_P1_NO_FIX_NEEDED";
        sql2 = "SELECT ABS(V_A1), V_SUM_C1 FROM V_P1_NO_FIX_NEEDED GROUP BY ABS(V_A1), V_SUM_C1";
        checkQueriesPlansAreTheSame(sql1, sql2);

        sql1 = "SELECT distinct A3, floor(B3+1) from T3";
        sql2 = "SELECT A3, floor(B3+1) from T3 group by A3, floor(B3+1)";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // Partition view tables without partition key
        String[] tbs = {"V_P1", "V_P1_ABS"};

        for (String tb: tbs) {
            // distinct single group by column
            sql1 = "SELECT distinct V_A1 FROM " + tb;
            sql2 = "SELECT V_A1 FROM " + tb + " GROUP BY V_A1";
            checkQueriesPlansAreTheSame(sql1, sql2);

            // distinct single aggregated column
            sql1 = "SELECT distinct V_SUM_C1 FROM " + tb;
            sql2 = "SELECT V_SUM_C1 FROM " + tb + " GROUP BY V_SUM_C1";
            checkQueriesPlansAreTheSame(sql1, sql2);

            // adding order by
            sql1 = "SELECT distinct V_SUM_C1 FROM " + tb + " ORDER BY V_SUM_C1";
            sql2 = "SELECT V_SUM_C1 FROM " + tb + " GROUP BY V_SUM_C1 ORDER BY V_SUM_C1";
            checkQueriesPlansAreTheSame(sql1, sql2);

            // multiple columns
            sql1 = "SELECT distinct V_A1, V_SUM_C1 FROM " + tb;
            sql2 = "SELECT V_A1, V_SUM_C1 FROM " + tb + " GROUP BY V_A1, V_SUM_C1";
            checkQueriesPlansAreTheSame(sql1, sql2);

            // multiple aggregated columns in the view
            sql1 = "SELECT distinct V_CNT, V_SUM_C1 FROM " + tb;
            sql2 = "SELECT V_CNT, V_SUM_C1 FROM " + tb + " GROUP BY V_CNT, V_SUM_C1";
            checkQueriesPlansAreTheSame(sql1, sql2);

            // expressions
            sql1 = "SELECT distinct V_A1, V_SUM_C1 / 10 FROM " + tb;
            sql2 = "SELECT V_A1, V_SUM_C1 / 10 FROM " + tb + " GROUP BY V_A1, V_SUM_C1 / 10";
            checkQueriesPlansAreTheSame(sql1, sql2);
        }
    }

    public void testNegative()
    {
        String sql;

        // Having
        sql = "SELECT distinct A3, B3 from T3 HAVING COUNT(*) > 3";
        failToCompile(sql, "expression not in aggregate or GROUP BY columns: PUBLIC.T3.A3");

        sql = "SELECT distinct A3, B3, C3 from T3 group by A3, B3";
        failToCompile(sql, "expression not in aggregate or GROUP BY columns: PUBLIC.T3.C3");

        sql = "SELECT distinct A3 from T3 group by A3, B3, C3";
        compileToFragments(sql); // make sure the DISTINCT with GROUP BY is still working

        // invalid ORDER BY expression
        String errorMsg = "invalid ORDER BY expression";

        // (1) without GROUP BY
        sql = "SELECT distinct B3 from T3 order by A3";
        failToCompile(sql, errorMsg);

        sql = "SELECT distinct A3, B3 from T3 order by C3";
        failToCompile(sql, errorMsg);

        // (2) with GROUP BY
        sql = "SELECT distinct A3 from T3 group by A3, B3, C3 ORDER BY B3";
        failToCompile(sql, errorMsg);

        //
        // (3) with GROUP BY Primary key, which is the very edge case
        //

        // P1 primary key (PKEY)
        sql = "select PKEY, max(B1) FROM P1 group by PKEY order by C1";
        failToCompile(sql, errorMsg);

        sql = "select max(B1) FROM P1 group by PKEY order by C1";
        failToCompile(sql, errorMsg);

        // When including C1 in the display columns, it will be OK
        sql = "select DISTINCT C1, max(B1) FROM P1 group by PKEY order by C1";
        compileToFragments(sql);

        sql = "select DISTINCT C1, max(B1) FROM P1 group by PKEY order by ABS(C1)";
        compileToFragments(sql);

        // T3 primary key (PKEY, A3)
        sql = "select DISTINCT PKEY, max(B3) FROM T3 group by PKEY, A3 order by C3";
        failToCompile(sql, errorMsg);

        sql = "select distinct max(B3) FROM T3 group by PKEY, A3 order by C3";
        failToCompile(sql, errorMsg);

        sql = "select distinct C3, max(B3) FROM T3 group by PKEY, A3 order by C3";
        compileToFragments(sql);

        // test ORDER BY GROUP BY key without in display list
        sql = "select distinct max(B3) FROM T3 group by PKEY, A3 order by A3";
        failToCompile(sql, errorMsg);
    }

    public void testColumnsWithGroupby()
    {
        String sql1, sql2;
        // Group by with multiple columns distinct

        // PKEY, A3 is the primary key or contains the unique key.
        // A3 is the Partition key

        sql1 = "SELECT distinct B3, C3 from T3 group by PKEY, A3";
        sql2 = "SELECT B3, C3 from T3 group by PKEY, A3";
        checkDistinctWithGroupbyPlans(sql1, sql2);

        // single column distinct
        sql1 = "SELECT distinct SUM(C3) from T3 group by A3, B3";
        sql2 = "SELECT SUM(C3) from T3 group by A3, B3";
        checkDistinctWithGroupbyPlans(sql1, sql2);

        sql1 = "SELECT distinct SUM(C3) from T3 group by D3, B3";
        sql2 = "SELECT SUM(C3) from T3 group by D3, B3";
        checkDistinctWithGroupbyPlans(sql1, sql2);

        // multiple columns distinct
        sql1 = "SELECT distinct B3, SUM(C3), COUNT(*) from T3 group by A3, B3";
        sql2 = "SELECT B3, SUM(C3), COUNT(*) from T3 group by A3, B3";
        checkDistinctWithGroupbyPlans(sql1, sql2);

        sql1 = "SELECT distinct B3, SUM(C3), COUNT(*) from T3 group by D3, B3";
        sql2 = "SELECT B3, SUM(C3), COUNT(*) from T3 group by D3, B3";
        checkDistinctWithGroupbyPlans(sql1, sql2);

        // variance on select list and group by list
        sql1 = "SELECT distinct A3, sum(C3) from T3 group by A3, B3";
        sql2 = "SELECT A3, sum(C3) from T3 group by A3, B3";
        checkDistinctWithGroupbyPlans(sql1, sql2);

        sql1 = "SELECT distinct D3, sum(C3) from T3 group by D3, B3";
        sql2 = "SELECT D3, sum(C3) from T3 group by D3, B3";
        checkDistinctWithGroupbyPlans(sql1, sql2);

        // group by 3 columns
        sql1 = "SELECT distinct A3, B3 from T3 group by A3, B3, C3";
        sql2 = "SELECT A3, B3 from T3 group by A3, B3, C3";
        checkDistinctWithGroupbyPlans(sql1, sql2);

        // order by
        sql1 = "SELECT distinct A3, B3 from T3 group by A3, B3, C3 ORDER BY A3, B3";
        sql2 = "SELECT A3, B3 from T3 group by A3, B3, C3 ORDER BY A3, B3";
        checkDistinctWithGroupbyPlans(sql1, sql2);

        // order by normal case
        sql1 = "SELECT distinct D3, B3 from T3 group by D3, B3, C3 ORDER BY D3, B3";
        sql2 = "SELECT D3, B3 from T3 group by D3, B3, C3 ORDER BY D3, B3";
        checkDistinctWithGroupbyPlans(sql1, sql2);

        // Having
        sql1 = "SELECT distinct B3, SUM(C3), COUNT(*) from T3 group by A3, B3 Having SUM(C3) > 3";
        sql2 = "SELECT B3, SUM(C3), COUNT(*) from T3 group by A3, B3 Having SUM(C3) > 3";
        checkDistinctWithGroupbyPlans(sql1, sql2);

        // LIMIT/OFFSET is tricky,
        // a lot of PUSH DOWN can happen: ORDER BY/LIMIT, DISTINCT

        // A3 is the Partition column for table T3
        // LIMIT can be pushed down with order by plan node for this case
        sql1 = "SELECT distinct A3, COUNT(*) from T3 group by A3, B3 ORDER BY A3 LIMIT 3";
        sql2 = "SELECT A3, COUNT(*) from T3 group by A3, B3 ORDER BY A3 LIMIT 3";
        checkDistinctWithGroupbyPlans(sql1, sql2, true);

        sql1 = "SELECT distinct B3, COUNT(*) from T3 group by A3, B3 ORDER BY B3 LIMIT 3";
        sql2 = "SELECT B3, COUNT(*) from T3 group by A3, B3 ORDER BY B3 LIMIT 3";
        checkDistinctWithGroupbyPlans(sql1, sql2, true);
    }

    public void testExpressionsWithGroupby() {
         String sql1, sql2;

         // distinct on expression
         sql1 = "SELECT distinct sum(C3)/count(C3) from T3 group by A3, B3";
         sql2 = "SELECT sum(C3)/count(C3) from T3 group by A3, B3";
         checkDistinctWithGroupbyPlans(sql1, sql2);
    }

    protected void checkDistinctWithGroupbyPlans(String distinctSQL, String groupbySQL) {
        checkDistinctWithGroupbyPlans(distinctSQL, groupbySQL, false);
    }

    /**
     *
     * @param distinctSQL Group by query with distinct
     * @param groupbySQL Group by query without distinct
     */
    protected void checkDistinctWithGroupbyPlans(String distinctSQL, String groupbySQL,
            boolean limitPushdown) {
        List<AbstractPlanNode> pns1 = compileToFragments(distinctSQL);
        List<AbstractPlanNode> pns2 = compileToFragments(groupbySQL);
        //printExplainPlan(pns1);
        //printExplainPlan(pns2);

        assertTrue(pns1.get(0) instanceof SendPlanNode);
        assertTrue(pns2.get(0) instanceof SendPlanNode);

        AbstractPlanNode apn1, apn2;
        apn1 = pns1.get(0).getChild(0);
        apn2 = pns2.get(0).getChild(0);

        boolean hasTopProjection1 = false;
        if (apn1 instanceof ProjectionPlanNode) {
            apn1 = apn1.getChild(0);
            hasTopProjection1 = true;
        }

        boolean hasTopProjection2 = false;
        if (apn2 instanceof ProjectionPlanNode) {
            apn2 = apn2.getChild(0);
            hasTopProjection2 = true;
        }

        // DISTINCT plan node is rewrote with GROUP BY and adds above the original GROUP BY node
        // there may be another projection node in between for complex aggregation case
        boolean hasOrderby = false, hasLimit = false;
        boolean groupByMergeReceive = false;
        // infer the ORDERBY/LIMIT information from the base line query
        if (apn2 instanceof OrderByPlanNode) {
            hasOrderby = true;
            if (apn2.getInlinePlanNode(PlanNodeType.LIMIT) != null) {
                hasLimit = true;
            }
            apn2 = apn2.getChild(0);
        } else if (apn2 instanceof LimitPlanNode) {
            hasLimit = true;
            apn2 = apn2.getChild(0);
        } else if (apn2 instanceof MergeReceivePlanNode) {
            assertTrue(apn2.getInlinePlanNode(PlanNodeType.ORDERBY) != null);
            hasOrderby = true;
            hasLimit = apn2.getInlinePlanNode(PlanNodeType.LIMIT) != null;
            groupByMergeReceive = true;
        }

        // check the DISTINCT query plan
        boolean distinctMergeReceive = false;
        if (hasOrderby) {
            if (apn1 instanceof OrderByPlanNode) {
                assertTrue(apn1 instanceof OrderByPlanNode);
                if (hasLimit) {
                    // check inline limit
                    assertNotNull(apn1.getInlinePlanNode(PlanNodeType.LIMIT));
                }
                apn1 = apn1.getChild(0);
            } else if (apn1 instanceof MergeReceivePlanNode) {
                distinctMergeReceive = true;
                assertNotNull(apn1.getInlinePlanNode(PlanNodeType.ORDERBY));
                assertEquals(0, apn1.getChildCount());
            } else {
                fail("The distinctSQL top node is not OrderBy or MergeReceive.");
            }
        } else if (hasLimit) {
            assertTrue(apn1 instanceof LimitPlanNode);
            apn1 = apn1.getChild(0);
        }

        // Check DISTINCT group by plan node
        if (distinctMergeReceive) {
            AbstractPlanNode aggr = AggregatePlanNode.getInlineAggregationNode(apn1);
            assertTrue(aggr instanceof AggregatePlanNode);
            assertEquals(0, ((AggregatePlanNode)aggr).getAggregateTypesSize());
            assertEquals(pns1.get(0).getOutputSchema().size(),
                ((AggregatePlanNode)aggr).getGroupByExpressionsSize());
            if (hasLimit) {
                // check inline limit
                assertNotNull(aggr.getInlinePlanNode(PlanNodeType.LIMIT));
            }
        } else {
            assertTrue(apn1 instanceof HashAggregatePlanNode);
            assertEquals(0, ((HashAggregatePlanNode)apn1).getAggregateTypesSize());
            assertEquals(pns1.get(0).getOutputSchema().size(),
                    ((HashAggregatePlanNode)apn1).getGroupByExpressionsSize());
            apn1 = apn1.getChild(0);
        }

        // check projection node for complex aggregation case
        if (apn1 instanceof ProjectionPlanNode) {
            apn1 = apn1.getChild(0);
            assertFalse(hasTopProjection1);
        }
        if (apn2 instanceof ProjectionPlanNode) {
            apn2 = apn2.getChild(0);
            assertFalse(hasTopProjection2);
        }

        // check the rest plan nodes.
        if (distinctMergeReceive == false && groupByMergeReceive == false) {
            assertEquals(apn1.toExplainPlanString(), apn2.toExplainPlanString());
        } else if (distinctMergeReceive == true && groupByMergeReceive == true) {
            // In case of applied MergeReceive optimization the apn1 and apn2 nodes
            // should not have any children
            assertEquals(0, apn1.getChildCount());
            assertEquals(0, apn2.getChildCount());
        }

        // Distributed DISTINCT GROUP BY
        if (pns1.size() > 1) {
            if (! limitPushdown) {
                assertEquals(pns1.get(1).toExplainPlanString(), pns2.get(1).toExplainPlanString());
                return;
            }

            assertTrue(pns1.get(1) instanceof SendPlanNode);
            assertTrue(pns2.get(1) instanceof SendPlanNode);

            apn1 = pns1.get(1).getChild(0);
            apn2 = pns2.get(1).getChild(0);

            // ignore the ORDER BY/LIMIT pushdown plan node
            // because DISTINCT case can not be pushed down
            assertTrue(apn2 instanceof OrderByPlanNode);
            assertNotNull(apn2.getInlinePlanNode(PlanNodeType.LIMIT));

            apn2 = apn2.getChild(0);
            // If the MergeReceive optimization was applied, the explain plan could legitimately
            // differ (for example, index for grouping purpose vs index for sort order), or different
            // winners may produce completely different paths.
            if (distinctMergeReceive == false && groupByMergeReceive == false) {
                assertEquals(apn1.toExplainPlanString(), apn2.toExplainPlanString());
            }
        }
    }

    public void testMatViewsWithGroupby()
    {
        String sql1, sql2;
        // Partition view tables without partition key
        String[] tbs = {"V_P1", "V_P1_ABS"};

        for (String tb: tbs) {

            // Because of the GROUP BY is contained in the display columns list
            // DISTINCT can be dropped
            sql1 = "SELECT distinct V_SUM_C1 FROM " + tb + " GROUP by V_SUM_C1 LIMIT 5";
            sql2 = "SELECT V_SUM_C1 FROM " + tb + " GROUP by V_SUM_C1 LIMIT 5";
            checkQueriesPlansAreTheSame(sql1, sql2);

            sql1 = "SELECT distinct V_SUM_C1 FROM " + tb + " GROUP by V_SUM_C1 ORDER BY 1 LIMIT 5";
            sql2 = "SELECT V_SUM_C1 FROM " + tb + " GROUP by V_SUM_C1 ORDER BY 1 LIMIT 5";
            checkQueriesPlansAreTheSame(sql1, sql2);

            // count(*) may be too special
            sql1 = "SELECT distinct count(*) FROM " + tb + " GROUP by V_SUM_C1 LIMIT 5";
            sql2 = "SELECT count(*) FROM " + tb + " GROUP by V_SUM_C1 LIMIT 5";
            checkDistinctWithGroupbyPlans(sql1, sql2);

            sql1 = "SELECT distinct sum(V_SUM_D1) FROM " + tb + " GROUP by V_SUM_C1 LIMIT 5";
            sql2 = "SELECT sum(V_SUM_D1) FROM " + tb + " GROUP by V_SUM_C1 LIMIT 5";
            checkDistinctWithGroupbyPlans(sql1, sql2);

            // TODO: add more tests
        }
    }
}
