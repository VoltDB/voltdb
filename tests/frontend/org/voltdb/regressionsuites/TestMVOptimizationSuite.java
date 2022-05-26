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

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

import junit.framework.Test;

/**
 * Tests for optimizations based on materialized views
 */

public class TestMVOptimizationSuite extends RegressionSuite {
    public TestMVOptimizationSuite(String name) {
        super(name);
    }
    // Table creation. Views are created individually in each test.
    static private void setupSchema(VoltProjectBuilder builder) throws IOException {
        builder.addLiteralSchema(
                "CREATE TABLE T1(A INT UNIQUE NOT NULL PRIMARY KEY, B INT NOT NULL, A1 INT, B1 INT UNIQUE, C VARCHAR(32));\n" +
                        "CREATE INDEX TA ON T1(A);\n" +
                        "CREATE TABLE T2(C0 INT, B0 INT NOT NULL);\n" +
                        "CREATE INDEX TC ON T2(C0);\n" +
                        "PARTITION TABLE T2 ON COLUMN B0;\n" +
                        "CREATE TABLE T11(A INT, A1 INT);\n" +
                        "CREATE TABLE T3(A INT, A1 INT, B INT);\n" +
                        "CREATE TABLE sensor(id int, zone smallint not null, temperature tinyint, humidity tinyint, " +
                        "vibration tinyint, pressure smallint, uptime bigint, last_served timestamp);\n");
        builder.setUseDDLSchema(true);
    }

    static private void populateTablesWithViews(Client client, String... views) throws IOException, ProcCallException {
        for(String view : views) {
            client.callProcedure("@AdHoc", view);
        }
        client.callProcedure("@AdHoc",
                "INSERT INTO T1 VALUES(1, 2, 10, 20, 'patter');\n" +
                        "INSERT INTO T1 VALUES(2, 3, 20, 30, 'pattern');\n" +
                        "INSERT INTO T1 VALUES(3, 4, 30, 40, 'potter');\n" +
                        "INSERT INTO T1 VALUES(4, 5, 40, 50, 'pottern');\n" +
                        "INSERT INTO T1 VALUES(5, 6, 50, 60, 'pattern ');\n" +
                        "INSERT INTO T1 VALUES(6, 7, 40, 70, 'pattern%@~`');\n" +
                        "INSERT INTO T1 VALUES(7, 8, 30, 80, 'patternV5');\n" +
                        "INSERT INTO T1 VALUES(8, 9, 20, 90, 'pAttern');\n" +
                        "INSERT INTO T1 VALUES(9, 10, 10, 10, 'PATTERN*');\n" +
                        "INSERT INTO T1 VALUES(15, 6, 50, 160, 'pattern*');\n" +
                        "INSERT INTO T1 VALUES(16, 7, 40, 170, 'patter*');\n" +
                        "INSERT INTO T1 VALUES(17, 8, null, 180, 'patternrettap');\n" +

                        "INSERT INTO T11 VALUES(-1, -1);\n" +
                        "INSERT INTO T11 VALUES(-5, -5);\n" +
                        "INSERT INTO T11 VALUES(-10, -10);\n" +
                        "INSERT INTO T11 VALUES(-50, -50);\n" +
                        "INSERT INTO T11 VALUES(-100, -100);\n" +
                        "INSERT INTO T11 VALUES(-500, -500);\n");
    }

    private void cleanTableAndViews(Client client, String... views) throws IOException, ProcCallException {
        client.callProcedure("@AdHoc", "Truncate table T1; \n" +
                "Truncate table T11; \n");
        for (final String view : views) {
            client.callProcedure("@AdHoc", "Drop view " + view);
        }
    }

    // Initialize catalog for given views and populate tables with some data
    private Client withViews(String... views) {
        try {
            final Client client = getClient();
            populateTablesWithViews(client, views);
            return client;
        } catch (IOException | ProcCallException ex) {
            fail(ex.getMessage());
        }
        return null;
    }

    // check that the result of given ad-hoc query matches with another ad hoc query (when provided),
    // and the explanation of the former contains given piece.
    private void checkThat(Client client, String adHocQuery, String matchedQuery, String contains)
            throws IOException, ProcCallException {
        if (matchedQuery != null) {
            assertTablesAreEqual("Exact match with view",
                    client.callProcedure("@AdHoc", adHocQuery).getResults()[0],
                    client.callProcedure("@AdHoc", matchedQuery).getResults()[0]);
        }
        assertTrue(getQueryPlan('\n', client, adHocQuery).toLowerCase().contains(contains.toLowerCase()));
    }

    public void testSimple() throws IOException, ProcCallException {
        final Client client = withViews(
                "CREATE VIEW v5_1 AS SELECT DISTINCT a1 distinct_a1, COUNT(b1) count_b1, SUM(a) sum_a, " +
                        "COUNT(*) counts FROM t1 WHERE b >= 2 OR b1 IN (3,30,300) GROUP BY a1;",
                "CREATE VIEW long_running (id, zone, min_temperature, max_temperature, sum_temperature, " +
                        "min_humidity, max_humidity, max_uptime, counts) AS SELECT " +
                        "id, zone, min(temperature), max(temperature), sum(temperature), min(humidity), " +
                        "max(humidity), max(uptime), count(*) FROM sensor WHERE uptime >= 72000 " +
                        "GROUP BY id, zone;");
        // An ad-hoc query that's "exactly the same" as an existing view definition will get rewritten
        checkThat(client,
                "SELECT DISTINCT a1 a1, COUNT(b1) cnt_b1, SUM(a) sum_a, COUNT(*) cnt FROM t1 WHERE b >= 2 OR b1 IN (3, 30, 300) GROUP BY a1;",
                "SELECT distinct_a1 a1, count_b1 b1, sum_a, counts cnt FROM v5_1",
                "sequential scan of \"v5_1\"");
        // A query that displays a subset of columns of existing view should also be rewritten
        checkThat(client,
                "SELECT SUM(a) FROM t1 WHERE b1 IN (3, 30, 300) OR 2 <= b GROUP BY a1",
                "SELECT sum_a c1 FROM v5_1", "sequential scan of \"v5_1\"");
        // Split the filter, and the query is not optimized
        checkThat(client,
                "SELECT SUM(a) FROM t1 WHERE b1 IN (3, 30) OR b1 in (300, 3) OR b > 2 OR b = 2 GROUP BY a1",
                null, "sequential scan of \"t1\"");
        // Or change filter
        checkThat(client,
                "SELECT SUM(a) FROM t1 WHERE b1 IN (3, 30, 300) OR b >= 20 GROUP BY a1",
                null, "sequential scan of \"t1\"");
        // Check that directly running matched query in subquery should not go wrong
        final String q =
                "SELECT * FROM (select zone, min(temperature) min_temperature, max(humidity) max_humidity FROM sensor " +
                "WHERE uptime >= 72000 GROUP BY id, zone) foo ORDER BY zone;";
        client.callProcedure("@AdHoc", q).getResults();
        cleanTableAndViews(client, "V5_1", "long_running");
    }

    public void testMVResolution() throws IOException, ProcCallException {
        final Client client = withViews(
                "CREATE VIEW v5_1 AS SELECT DISTINCT a1 distinct_a1, COUNT(b1) count_b1, SUM(a) sum_a, " +
                        "COUNT(*) counts FROM t1 WHERE b >= 2 OR b1 IN (3,30,300) GROUP BY a1;",
                "CREATE VIEW v5_2 AS SELECT a a, COUNT(b1) count_b1, SUM(a) sum_a, " +
                        "COUNT(*) counts FROM t1 WHERE b >= 2 AND b1 IN (3,30,300) GROUP BY a;",
                "CREATE VIEW v5_3 AS SELECT a1 a1, COUNT(b1) count_b1, SUM(a) sum_a, " +
                        "COUNT(*) counts FROM t1 WHERE b >= 2 OR b1 IN (3,30,300) GROUP BY a1;",    // v5_1 === v5_3
                "CREATE VIEW v5_4 AS SELECT a1 a1, COUNT(b1) count_b1, SUM(a) sum_a, " +
                        "COUNT(*) counts FROM t1 WHERE a >= 2 OR b1 IN (3,30,300) GROUP BY a1;");
        // call a query with single unmatched constant
        assertTablesAreEqual("Exact match with view",
                client.callProcedure("@AdHoc", "SELECT COUNT(b1) cb FROM t1 WHERE b >= 2 OR b1 in (3, 30, 300) GROUP BY a1 order by cb").getResults()[0],
                client.callProcedure("@AdHoc", "SELECT count_b1 cb FROM v5_1 order by cb").getResults()[0]);
        final String plan = getQueryPlan('\n', client, "SELECT COUNT(b1) cb FROM t1 WHERE b >= 2 OR b1 in (3, 30, 300) GROUP BY a1");
        assertTrue("Plan should have used V5_1 or V5_3: \n" + plan,
              plan.toLowerCase().contains("sequential scan of \"v5_3\"".toLowerCase()) ||
              plan.toLowerCase().contains("sequential scan of \"v5_1\"".toLowerCase()));
        cleanTableAndViews(client, "V5_1", "V5_2", "V5_3", "V5_4");
    }

    static public Test suite() {
        try {
            final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMVOptimizationSuite.class);
            VoltProjectBuilder project = new VoltProjectBuilder();
            VoltServerConfig config = new LocalCluster("test-mv-optimization.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            setupSchema(project);
            assertTrue(config.compile(project));
            builder.addServerConfig(config);

            project = new VoltProjectBuilder();
            config = new LocalCluster("test-mv-optimization.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
            setupSchema(project);
            assertTrue(config.compile(project));
            builder.addServerConfig(config);
            return builder;
        } catch (IOException excp) {
            fail("Failure setting up schema for project");
        }
        return null;
    }
}


