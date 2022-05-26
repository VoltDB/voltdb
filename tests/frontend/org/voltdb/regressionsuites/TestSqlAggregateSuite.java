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
import java.math.BigDecimal;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;
import org.voltdb_testprocs.regressionsuites.aggregates.Insert;

/**
 * System tests for basic aggregate and DISTINCT functionality
 */

public class TestSqlAggregateSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

    static final int ROWS = 10;

    public void testAggregateInArithmetics() throws IOException, ProcCallException {
        Client client = getClient();
        String table = "ENG10429";
        String insert = "INSERT INTO %s VALUES (%d, %d, %d);";
        client.callProcedure("@AdHoc", String.format(insert, table, 123, 3, 369));
        client.callProcedure("@AdHoc", String.format(insert, table, 15, 3, 45));
        client.callProcedure("@AdHoc", String.format(insert, table, 64, 2, 128));
        client.callProcedure("@AdHoc", String.format(insert, table, 77, 2, 154));
        String query = "SELECT SUM(a)/b AS val FROM " + table + " GROUP BY b ORDER BY 1;";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(2, results[0].getRowCount());
        results[0].advanceRow();
        assertEquals(46, results[0].getLong(0));
        results[0].advanceRow();
        assertEquals(70, results[0].getLong(0));
    }

    public void testDistinct() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            Client client = getClient();
            for (int i = 0; i < ROWS; ++i)
            {
                client.callProcedure("Insert", table, i, "desc",
                                     new BigDecimal(10.0), i / 2, 14.5);
            }
            String query = String.format("select distinct %s.NUM from %s",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            // lazy check that we get 5 rows back, put off checking contents
            assertEquals(5, results[0].getRowCount());
        }
    }

    public void testAggregates() throws IOException, ProcCallException
    {
        String[] aggs = {"count", "sum", "min", "max"};
        long[] expected_results = {10,
                                   (0 + 1 + 2 + 3 + 4) * 2,
                                   0,
                                   4};
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            Client client = getClient();
            for (int i = 0; i < ROWS; ++i)
            {
                client.callProcedure("Insert", table, i, "desc",
                                     new BigDecimal(10.0), i / 2, 14.5);
            }
            for (int i = 0; i < aggs.length; ++i)
            {
                String query = String.format("select %s(%s.NUM) from %s",
                                             aggs[i], table, table);
                VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
                assertEquals(expected_results[i], results[0].asScalarLong());
            }
            // Do avg separately since the column is a float and makes
            // asScalarLong() unhappy
            String query = String.format("select avg(%s.NUM) from %s",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(2.0,
                         ((Number)results[0].get(0, results[0].getColumnType(0))).doubleValue());
        }
    }

    public void testAggregatesOnEmptyTable() throws IOException, ProcCallException
    {
        String[] aggs = {"count", "sum", "min", "max"};
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            Client client = getClient();
            for (int i = 0; i < aggs.length; ++i)
            {
                String query = String.format("select %s(%s.NUM) from %s",
                                             aggs[i], table, table);
                VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
                if (aggs[i].equals("count")) {
                    assertEquals(0, results[0].asScalarLong());
                } else {
                    final VoltTableRow row = results[0].fetchRow(0);
                    row.get(0, results[0].getColumnType(0));
                    if (!isHSQL()) {
                        assertTrue(row.wasNull());
                    }
                }
            }
            // Do avg separately since the column is a float and makes
            // asScalarLong() unhappy
            String query = String.format("select avg(%s.NUM) from %s",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            @SuppressWarnings("unused")
            final double value = ((Number)results[0].get(0, results[0].getColumnType(0))).doubleValue();
            if (!isHSQL()) {
                assertTrue(results[0].wasNull());
            }
        }
    }

    // This test case includes all of the broken cases of sum, min, max, and avg
    // which didn't actually do DISTINCT.
    // This is only visible for sum and avg, of course
    public void testAggregatesWithDistinct()
    throws IOException, ProcCallException
    {
        String[] aggs = {"count", "sum", "min", "max"};
        long[] expected_results = {5,
                                   (0 + 1 + 2 + 3 + 4),
                                   0,
                                   4};
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            Client client = getClient();
            for (int i = 0; i < ROWS; ++i)
            {
                client.callProcedure("Insert", table, i, "desc",
                                     new BigDecimal(10.0), i / 2, 14.5);
            }
            for (int i = 0; i < aggs.length; ++i)
            {
                String query = String.format("select %s(distinct(%s.NUM)) from %s",
                                             aggs[i], table, table);
                VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
                assertEquals(expected_results[i], results[0].asScalarLong());
            }
            // Do avg separately since the column is a float and makes
            // asScalarLong() unhappy
            String query = String.format("select avg(distinct(%s.NUM)) from %s",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(2.0,
                         ((Number)results[0].get(0, results[0].getColumnType(0))).doubleValue());
        }
    }

    public void testStringMinMaxAndCount()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            Client client = getClient();
            for (int i = 0; i < ROWS; ++i)
            {
                client.callProcedure("Insert", table, i, String.valueOf(i),
                                     new BigDecimal(10.0), i / 2, 14.5);
            }
            for (int i = ROWS; i < ROWS + 5; ++i)
            {
                client.callProcedure("Insert", table, i, VoltType.NULL_STRING_OR_VARBINARY,
                                     new BigDecimal(10.0), i / 2, 14.5);
            }
            String query = String.format("select MIN(%s.DESC) from %s",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals("0", results[0].getString(0));
            query = String.format("select MAX(%s.DESC) from %s",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals("9", results[0].getString(0));
            query = String.format("select COUNT(%s.DESC) from %s",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(ROWS, results[0].asScalarLong());
        }
    }

    public void testAggregatesWithNulls() throws IOException, ProcCallException
    {
        int good_rows = 10;
        int null_rows = 5;

        String[] aggs = {"sum", "min", "max", "avg"};
        long[] expected_int_results = {(0 + 1 + 2 + 3 + 4) * 2,
                                       0,
                                       4,
                                       2};
        double[] expected_float_results = {(0 + 0.5 + 1 + 1.5 + 2 + 2.5 + 3 +
                                            3.5 + 4 + 4.5),
                                           0.0,
                                           4.5,
                                           2.25};
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            Client client = getClient();
            for (int i = 0; i < good_rows; ++i)
            {
                client.callProcedure("Insert", table, i, "desc",
                                     new BigDecimal(i / 2.0), i / 2, i / 2.0);
            }
            for (int i = good_rows; i < good_rows + null_rows; ++i)
            {
                client.callProcedure("Insert", table, i, VoltType.NULL_STRING_OR_VARBINARY,
                                     VoltType.NULL_DECIMAL,
                                     VoltType.NULL_INTEGER,
                                     VoltType.NULL_FLOAT);
            }
            // do count separately since it's always integer return type
            String query = String.format("select count(%s.CASH) from %s",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(good_rows, results[0].asScalarLong());
            query = String.format("select count(%s.NUM) from %s",
                                         table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(good_rows, results[0].asScalarLong());
            query = String.format("select count(%s.RATIO) from %s",
                                         table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(good_rows, results[0].asScalarLong());
            for (int i = 0; i < aggs.length; ++i)
            {
                query = String.format("select %s(%s.CASH) from %s",
                                      aggs[i], table, table);
                results = client.callProcedure("@AdHoc", query).getResults();
                results[0].advanceRow();
                assertEquals(expected_float_results[i],
                             results[0].getDecimalAsBigDecimal(0).doubleValue());
                query = String.format("select %s(%s.NUM) from %s",
                                             aggs[i], table, table);
                results = client.callProcedure("@AdHoc", query).getResults();
                assertEquals(expected_int_results[i], results[0].asScalarLong());
                query = String.format("select %s(%s.RATIO) from %s",
                                             aggs[i], table, table);
                results = client.callProcedure("@AdHoc", query).getResults();
                results[0].advanceRow();
                assertEquals(expected_float_results[i], results[0].getDouble(0));
            }
            // and finish up with count(*) for good measure
            query = String.format("select count(*) from %s", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(good_rows + null_rows, results[0].asScalarLong());
        }
    }

    public void testAggregatesWithOnlyNulls() throws IOException, ProcCallException
    {
        int null_rows = 5;

        String[] aggs = {"sum", "min", "max", "avg"};
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            Client client = getClient();
            for (int i = 0; i < null_rows; ++i)
            {
                client.callProcedure("Insert", table, i, VoltType.NULL_STRING_OR_VARBINARY,
                                     VoltType.NULL_DECIMAL,
                                     VoltType.NULL_INTEGER,
                                     VoltType.NULL_FLOAT);
            }
            // do count separately since it's always integer return type
            String query = String.format("select count(%s.CASH) from %s",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].asScalarLong());
            query = String.format("select count(%s.NUM) from %s",
                                         table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].asScalarLong());
            query = String.format("select count(%s.RATIO) from %s",
                                         table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].asScalarLong());
            for (int i = 0; i < aggs.length; ++i)
            {
                query = String.format("select %s(%s.CASH) from %s",
                                      aggs[i], table, table);
                results = client.callProcedure("@AdHoc", query).getResults();
                results[0].advanceRow();
                @SuppressWarnings("unused")
                BigDecimal dec_val = results[0].getDecimalAsBigDecimal(0);
                assert(results[0].wasNull());
                query = String.format("select %s(%s.NUM) from %s",
                                             aggs[i], table, table);
                results = client.callProcedure("@AdHoc", query).getResults();
                results[0].advanceRow();
                @SuppressWarnings("unused")
                long long_val = results[0].getLong(0);
                if ( ! isHSQL()) {
                    assert(results[0].wasNull());
                }
                query = String.format("select %s(%s.RATIO) from %s",
                        aggs[i], table, table);
                results = client.callProcedure("@AdHoc", query).getResults();
                results[0].advanceRow();
                @SuppressWarnings("unused")
                double doub_val = results[0].getDouble(0);
                if ( ! isHSQL()) {
                    assert(results[0].wasNull());
                }
            }
            // and finish up with count(*) for good measure
            query = String.format("select count(*) from %s", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(null_rows, results[0].asScalarLong());
        }
    }

    // simple test case for eng909
    public void testOneDistinctAggregateAndOneNot() throws IOException, ProcCallException
    {
        String[] aggs = {"count", "sum", "min", "max"};
        long[] expected_distinct_results = {5,
                                            (0 + 1 + 2 + 3 + 4),
                                            0,
                                            4};
        long[] expected_results = {10,
                                   (0 + 1 + 2 + 3 + 4) * 2,
                                   0,
                                   4};
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            Client client = getClient();
            for (int i = 0; i < ROWS; ++i)
            {
                client.callProcedure("Insert", table, i, "desc",
                                     new BigDecimal(10.0), i / 2, 14.5);
            }
            for (int i = 0; i < aggs.length; ++i)
            {
                String query = String.format("select %s(distinct(%s.NUM)), %s(%s.NUM) from %s",
                                             aggs[i], table, aggs[i], table, table);
                VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
                results[0].advanceRow();
                assertEquals(expected_distinct_results[i], results[0].getLong(0));
                assertEquals(expected_results[i], results[0].getLong(1));
            }
        }
    }

    // simple test case for eng205.  Use the query from above
    // to also test this with distinct applied
    public void testAggregateWithExpression() throws IOException, ProcCallException
    {
        String[] aggs = {"count", "sum", "min", "max"};
        long[] expected_distinct_results = {5,
                                            (0 + 1 + 2 + 3 + 4) * 2,
                                            0,
                                            8};
        long[] expected_results = {10,
                                   (0 + 1 + 2 + 3 + 4) * 4,
                                   0,
                                   8};
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            Client client = getClient();
            for (int i = 0; i < ROWS; ++i)
            {
                client.callProcedure("Insert", table, i, "desc",
                                     new BigDecimal(10.0), i / 2, 14.5);
            }
            for (int i = 0; i < aggs.length; ++i)
            {
                String query = String.format("select %s(distinct(%s.NUM * 2)), %s(%s.NUM * 2) from %s",
                                             aggs[i], table, aggs[i], table, table);
                VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
                results[0].advanceRow();
                assertEquals(expected_distinct_results[i], results[0].getLong(0));
                assertEquals(expected_results[i], results[0].getLong(1));
            }
        }
    }

    // Test case for ENG-4980
    public void testCountDistinctPartitionColumn() throws IOException, ProcCallException
    {
        String[] aggs = {"count", "sum", "min", "max"};
        long[] expected_results = {5,
                                   0 + 1 + 2 + 3 + 4,
                                   0,
                                   4};
        Client client = getClient();
        for (int i = 0; i < ROWS; ++i)
        {
            int value = i / 2;
            String query = "INSERT INTO ENG4980 VALUES (" + value + ", " + value + ");";
            System.out.println(query);
            client.callProcedure("@AdHoc", query);
        }
        for (int i = 0; i < aggs.length; ++i)
        {
            String query = String.format("select %s(distinct pid) from ENG4980",
                                         aggs[i]);
            System.out.println(query);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(expected_results[i], results[0].getLong(0));
        }
    }

    // This is a regression test for ENG-9394
    public void testInlineVarcharCountDistinct() throws IOException, ProcCallException {
        Client client = getClient();
        // These values were empirically determined to
        // produce a wrong answer before this bug was fixed.
        String[] codeValues = {
                "x9knsslnDEx1vPVE3AmoJSyF",
                "x9iMVxN9IM3PbKn0rVuPfK0GI",
                "xcsj3vLpLh",
                "x8DuKNF0GeQ8UG",
                "xiqRc8iVY2u1oN5kizy3CA7",
                "xecZXl8bsE4Pw3LBhI7B8G",
                "xfKP25a2foPTw2FiCRdUsZj",
                "xp7KR8SOp5B8kopvUdnc3gmeAskWVwJ",
                "xG6",
                "xhw0OVDvKJRyYdmUj9z3UcODKNB",
                "xPy5PXeRtJcZhUFuyIiV09h",
                "x8Vc4ExuM4c7SU5F6XZ6pWiUnnrO93v",
                "xNe2H70Em",
                "x8cepckgTyLhKR8cF10JgR4JzWCUu",
                "xuT6JlTjfObhqUJ",
                "xovLMc0FWA04m",
                "xJroR",
                "xdSjQW",
                "xHw",
                "xMMnkLHq8b493PhefMjtLQjV"
        };

        for (int i = 0; i < codeValues.length; ++i) {
            client.callProcedure("ENG_9394.insert",
                    codeValues[i],
                    codeValues[i],
                    Integer.toString(i),
                    new TimestampType("2010-10-31 11:11:26"),
                    new TimestampType("2048-10-31 11:11:27"),
                    "000");
        }

        VoltTable vt;
        String stmt = "select count(distinct code) "
                + "from ENG_9394 "
                + "where dt1 <= current_timestamp "
                + "and dt2 >= current_timestamp "
                + "and acode < '999'";
        vt = client.callProcedure("@AdHoc", stmt).getResults()[0];
        vt.advanceRow();
        long countDistinct = vt.getLong(0);

        stmt = "select count(*) "
                + "from ("
                + "  select distinct code "
                + "  from ENG_9394 "
                + "  where dt1 <= current_timestamp "
                + "  and dt2 >= current_timestamp "
                + "  and acode < '999') as subq";
        vt = client.callProcedure("@AdHoc", stmt).getResults()[0];
        vt.advanceRow();
        long countStarSubquery = vt.getLong(0);
        assertEquals(countStarSubquery, countDistinct);
    }

    // ENG-3645 crashed on an aggregates memory management issue.
    public void testEng3645() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("@AdHoc",
                "SELECT SUM(HOURS),AVG(HOURS),MIN(HOURS),MAX(HOURS) FROM ENG3465 WHERE EMPNUM='E1';").getResults();
        assertTrue(results[0].advanceRow());
    }

    public void testEng18917() throws IOException, ProcCallException {
        // ignore the hsql backend for this test case, since the decode function is not implemented in hsql
        if (isHSQL()) {
            return;
        }
        Client client = getClient();

        client.callProcedure("@AdHoc", "INSERT INTO Eng18917 VALUES (1, 9201, 'A')");
        client.callProcedure("@AdHoc", "INSERT INTO Eng18917 VALUES (2, 9201, 'A')");
        client.callProcedure("@AdHoc", "INSERT INTO Eng18917 VALUES (3, 9201, 'B')");
        client.callProcedure("@AdHoc", "INSERT INTO Eng18917 VALUES (4, 9201, 'B')");
        client.callProcedure("@AdHoc", "INSERT INTO Eng18917 VALUES (5, 9201, 'B')");
        client.callProcedure("@AdHoc", "INSERT INTO Eng18917 VALUES (6, 9201, 'B')");

        final ClientResponse parameterizedQueryResponse = client.callProcedure("@AdHoc",
                "SELECT\n" +
                        "Eng18917.A0 AS A00000016,\n" +
                        "SUM(DECODE(Eng18917.A0, CAST(? AS VARCHAR), CAST(? AS INTEGER), NULL)) AS A00000045,\n" +
                        "SUM(DECODE(Eng18917.A0, CAST(? AS VARCHAR), CAST(? AS INTEGER), NULL)) AS A00000073\n" +
                        "FROM Eng18917\n" +
                        "GROUP BY Eng18917.A0\n" +
                        "ORDER BY Eng18917.A0 ASC", "A", 1, "B", 1);
        final VoltTable parameterizedResults = parameterizedQueryResponse.getResults()[0];
        assertContentOfTable(new Object[][]{{"A", 2, null}, {"B", null, 4}}, parameterizedResults);
    }

    public void testEng19702() throws IOException, ProcCallException {
        // ignore the hsql backend for this test case, since the decode function is not implemented in hsql
        if (isHSQL()) {
            return;
        }
        final Client client = getClient();
        client.callProcedure("@AdHoc", "INSERT INTO Eng19702_Balance VALUES ('asi', 'abi', '2020-06-09')");
        client.callProcedure("@AdHoc", "INSERT INTO Eng19702_BalanceValue VALUES ('asi', 'abi', 1000.0, '2020-05-01')");
        assertContentOfTable(new Object[][]{{new BigDecimal("1000.0")}},
                client.callProcedure("@AdHoc",
                        "SELECT Checksum from (\n" +
                                "   SELECT B.SubscriberId, B.BalanceId,\n" +
                                "      B.ExpiryDate,\n" +
                                "      SUM(BV.BalanceValue) AS BalanceValue,\n" +
                                "      MIN(LastUpdated)     AS MinLastUpdated,\n" +
                                "      COALESCE(MAX(DECODE(B.BalanceId, 'abi', LastUpdated, NULL)), MAX(LastUpdated)) AS LastUpdated,\n" +
                                "      MAX(DECODE(B.BalanceId, 'abi', BV.BalanceValue, 0)) AS Checksum\n" +
                                "   FROM Eng19702_Balance B JOIN Eng19702_BalanceValue BV\n" +
                                "   ON B.SubscriberId = BV.SubscriberId AND B.SubscriberId = 'asi' AND B.BalanceId = BV.BalanceId\n" +
                                "   GROUP BY B.SubscriberId, B.BalanceId) subquery")
                        .getResults()[0]);
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestSqlAggregateSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSqlAggregateSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(Insert.class.getResource("aggregate-sql-ddl.sql"));
        project.addPartitionInfo("P1", "ID");
        project.addProcedure(Insert.class);

        config = new LocalCluster("sqlaggregate-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        config = new LocalCluster("sqlaggregate-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        config = new LocalCluster("sqlaggregate-twosites.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // HSQL backend testing fails a few cases,
        // probably due to differences in null representation -- it doesn't support MIN_VALUE as null
        // These specific cases are qualified with if ( ! isHSQL()).
        config = new LocalCluster("sqlaggregate-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        return builder;
    }

}
