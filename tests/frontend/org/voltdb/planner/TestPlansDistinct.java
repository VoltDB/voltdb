/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltdb.plannodes.AbstractPlanNode;

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

    List<AbstractPlanNode> pns = new ArrayList<AbstractPlanNode>();

    public void testMultipleColumns()
    {
        String sql1, sql2;

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
        sql1 = "SELECT distinct count(*), SUM(A3) from T3";
        sql2 = "SELECT count(*), SUM(A3) from T3";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // multiple table aggregate select
        sql1 = "SELECT distinct SUM(A3) from T3";
        sql2 = "SELECT SUM(A3) from T3";
        checkQueriesPlansAreTheSame(sql1, sql2);
    }

    public void testMultipleExpressions()
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

    public void testMaterializedViews()
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

        // Group by with multiple columns distinct

        // PKEY, A3 is the primary key or contains the unique key.
        sql = "SELECT distinct B3, C3 from T3 group by PKEY, A3";
        failToCompile(sql, "Multiple DISTINCT columns with GROUP BY clause currently unsupported");

        // edge case because grouping by primary key
        sql = "SELECT distinct A3, B3, sum(C3) from T3 group by A3, B3";
        failToCompile(sql, "Multiple DISTINCT columns with GROUP BY clause currently unsupported");

        sql = "SELECT distinct A3, B3 from T3 group by A3, B3, C3";
        failToCompile(sql, "Multiple DISTINCT columns with GROUP BY clause currently unsupported");

        sql = "SELECT distinct A3 from T3 group by A3, B3, C3";
        compileToFragments(sql); // make sure the DISTINCT with GROUP BY is still working

        // distinct on expression with group by clause
        sql = "SELECT distinct sum(C3) from T3 group by A3, B3";
        failToCompile(sql, "DISTINCT of an expression with GROUP BY clause currently unsupported");
    }
}
