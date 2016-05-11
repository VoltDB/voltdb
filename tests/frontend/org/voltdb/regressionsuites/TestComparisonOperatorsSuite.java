/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.sql.Timestamp;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;


public class TestComparisonOperatorsSuite  extends RegressionSuite {
    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestComparisonOperatorsSuite(String name) {
        super(name);
    }

    static private void setUpSchema(VoltProjectBuilder project) throws IOException
    {

        final String literalSchema =
                "CREATE TABLE S1 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER, " +
                "DEPT INTEGER, " +
                "PRIMARY KEY (ID) );" +

                "CREATE TABLE S2 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER, " +
                "DEPT INTEGER, " +
                "PRIMARY KEY (ID) );" +

                "CREATE TABLE R1 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "DESC VARCHAR(300), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PAST TIMESTAMP, " +
                "PRIMARY KEY (ID) ); " +

                // Test unique generalized index on a function of an already indexed column.
                "CREATE UNIQUE INDEX R1_ABS_ID_DESC ON R1 ( ABS(ID), DESC ); " +

                // Test generalized expression index with a constant argument.
                "CREATE INDEX R1_ABS_ID_SCALED ON R1 ( ID / 3 ); " +

                //Test generalized expression index with case when.
                "CREATE INDEX R1_CASEWHEN ON R1 (CASE WHEN num < 3 THEN num/2 ELSE num + 10 END); " +

                "CREATE TABLE INLINED_VC_VB_TABLE (" +
                "ID INTEGER DEFAULT 0 NOT NULL," +
                "VC1 VARCHAR(6)," +     // inlined
                "VC2 VARCHAR(16)," +    // not inlined
                "VB1 VARBINARY(6)," +   // inlined
                "VB2 VARBINARY(64));" + // not inlined
                "";

            project.addLiteralSchema(literalSchema);
    }

    public void testIsDistinctFrom() throws Exception
    {
        // is Distinct from operator does not work on HSQL-backend based. It results
        // in run time exception with message "unsupported internal operation: Expression"
        // So do not run "is distinct from" against HSQL backend
        if(!isHSQL()) {
            System.out.println("\nSTARTING test is Distinct from ...");
            Client client = getClient();
            populateTableForIsDistinctFromTests(client);
            subTestIsDistinctFrom(client);
            subTestIsDistinctFromUsingSubqueries(client);
            subTestIsDistinctFromInCompatibleTypes(client);
        }
    }

    private void populateTableForIsDistinctFromTests(Client client)
            throws IOException, NoConnectionsException, ProcCallException
    {
                                //       id, wage, dept
        client.callProcedure("S1.insert", 1,    1000, 1);
        client.callProcedure("S1.insert", 3,    3000, 1);
        client.callProcedure("S1.insert", 5,    2553, 3);
        client.callProcedure("S1.insert", 7,    4552, 2);
        client.callProcedure("S1.insert", 9,    5152, 2);
        client.callProcedure("S1.insert", 10,   null, 2);

        client.callProcedure("S2.insert", 1, 1000, 2);
        client.callProcedure("S2.insert", 4, null, 2);
        client.callProcedure("S2.insert", 5, 5253, 3);
    }

    private void subTestIsDistinctFrom(Client client)
            throws Exception
    {
        // 'is Distinct from' operator does not work on HSQL-backend - it's missing end-to-end
        // support. It results in runt time exception "unsupported internal operation: Expression"
        // So assert in case if these test cases are executed on HSQL backend
        // Once support for 'is distinct from' is available on HSQL-backend, remove the assert below.
        // results for HSQL backend below are HSQL 2.3.2/3 version and also verified against postgresql.
        // so it come handy and have left them in there.
        assert(!isHSQL());

        String sql;

        //ENG-8946: NULL constant in runtime exception when trying to resolve in HSQL
        // NULL constant - results in non-parameterized plan
        //sql = "SELECT * FROM S2 A WHERE  A.WAGE is not distinct from NULL;";
        //validateTableOfLongs(client, sql, new long[][] {{4,     Long.MIN_VALUE,     2}});

        // Non-Null constant results in parameterized plan
        sql = "SELECT * FROM S2 A WHERE  A.WAGE is  distinct from 1000.01 order by A.ID;";
        validateTableOfLongs(client, sql, new long[][] {{1,     1000,               2},
                                                        {4,     Long.MIN_VALUE,     2},
                                                        {5,     5253,               3}});

        // Join operation
        // case 1: on column that can't have null values
        sql = "Select S1.ID ID," +
                "S1.Wage, S1.Dept, " +
                "S2.WAGE, S2.Dept " +
                "from S1, S2 where S1.ID is not distinct from S2.ID order by S1.ID;";
        //vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        //System.out.println("\n\nsql: " + sql + "\nResult: " + (vt.toString()) + " row Count: "+ vt.getRowCount() + "\n\n");
        validateTableOfLongs(client, sql, new long[][] {{1, 1000, 1, 1000, 2},
                                                        {5, 2553, 3, 5253, 3}});

        // case 2.1: on column that can have null values; result set does not has null values
        sql = "Select S1.Wage," +
                "S1.ID, S1.Dept, " +
                "S2.ID, S2.Dept " +
                "from S1, S2 where S1.WAGE is not distinct from S2.WAGE order by S1.ID;";
        validateTableOfLongs(client, sql, new long[][] {{1000,              1,  1,  1,  2},
                                                        {Long.MIN_VALUE,    10, 2,  4,  2}});


        // case 2.2: on column that can have null values; result set has null values
        sql = "Select S1.ID ID," +
                "S1.Wage, S1.Dept, " +
                "S2.WAGE, S2.Dept " +
                "from S1, S2 where S1.WAGE is distinct from S2.WAGE order by S1.ID, S2.WAGE ASC;";

        validateTableOfLongs(client, sql, new long[][] {{1, 1000,           1,  Long.MIN_VALUE, 2},
                                                        {1, 1000,           1,  5253,           3},
                                                        {3,  3000,          1,  Long.MIN_VALUE, 2},
                                                        {3,  3000,          1,  1000,           2},
                                                        {3,  3000,          1,  5253,           3},
                                                        {5,  2553,          3,  Long.MIN_VALUE, 2},
                                                        {5,  2553,          3,  1000,           2},
                                                        {5,  2553,          3,  5253,           3},
                                                        {7,  4552,          2,  Long.MIN_VALUE, 2},
                                                        {7,  4552,          2,  1000,           2},
                                                        {7,  4552,          2,  5253,           3},
                                                        {9,  5152,          2,  Long.MIN_VALUE, 2},
                                                        {9,  5152,          2,  1000,           2},
                                                        {9,  5152,          2,  5253,           3},
                                                        {10, Long.MIN_VALUE,2,  1000,           2},
                                                        {10, Long.MIN_VALUE,2,  5253,           3}});


        // left join on column that has null values
        sql = "Select S2.wage, S2.ID, count (*) from S1 left Join S2 On " +
                "S2.WAGE is not distinct from S2.wage group by S2.wage, S2.ID order by s2.wage;";
        validateTableOfLongs(client, sql, new long[][] {{Long.MIN_VALUE,    4,   6},
                                                        {1000,              1,   6},
                                                        {5253,              5,   6}});
    }

    private void subTestIsDistinctFromUsingSubqueries(Client client) throws Exception
    {
        // 'is Distinct from' operator does not work on HSQL-backend - it's missing end-to-end
        // support. It results in runt time exception "unsupported internal operation: Expression"
        // So assert in case if these test cases are executed on HSQL backend
        // Once support for 'is distinct from' is available on HSQL-backend, remove the assert below.
        // results for HSQL backend below are HSQL 2.3.2/3 version and also verified against postgresql.
        // so it come handy and have left them in there.
        assert(!isHSQL());

        String sql;

        // test cases below test different subquery condition paths in EE like LHS NULL, RHS NOT NULL and so forth
        sql = "SELECT wage salary, count(*) from S2 "+
                "WHERE wage is distinct from  " +
                "(SELECT MIN(wage) FROM S1 where wage is distinct from 2553) " +
                "GROUP BY wage " +
                "HAVING COUNT(*) is distinct from 7 " +
                "ORDER BY wage";
        validateTableOfLongs(client, sql, new long[][] {{Long.MIN_VALUE,    1},
                                                        {5253,              1}});

        sql = "SELECT id, wage, count(*) from S1 "+
                "WHERE wage is distinct from  " +
                "(SELECT wage FROM S2 where id is not distinct from 4) " +
                "GROUP BY wage, id HAVING COUNT(*) is distinct from 7 ORDER BY id;";
        validateTableOfLongs(client, sql, new long[][] {{1, 1000,   1},
                                                        {3, 3000,   1},
                                                        {5, 2553,   1},
                                                        {7, 4552,   1},
                                                        {9, 5152,   1}});

        sql = "SELECT id, wage salary, count(*)  from S1 " +
                "WHERE  (select S2.wage from S2 where S2.ID<>1 and S2.id<>5) is not distinct from wage "+
                "GROUP BY wage, id HAVING COUNT(*) is distinct from 7 ORDER BY wage;";
        validateTableOfLongs(client, sql, new long[][] {{10,    Long.MIN_VALUE,    1}});



        sql = "select S1.wage, count(*) from S1 Right Join S2 "+
                "On S2.wage is distinct from  " +
                "(SELECT MIN(wage) FROM S1 where wage is  distinct from 1000) " +
                "GROUP BY S1.wage  having COUNT(*) is not distinct from 1;";
        validateTableOfLongs(client, sql, new long[][] {});

        sql = "select * from S1 where S1.wage = ANY " +
                "(select S2.wage from S2 where S2.wage is distinct from  5253 or S2.wage is not distinct from  1000);";
        validateTableOfLongs(client, sql, new long[][] {{1,     1000,      1}});

        // currently ANY/ALL operator is not supported with "is distinct from" comparison operator
        sql = "select * from S1 where S1.WAGE is not distinct from ANY " +
                "(select S2.wage from S2 where S2.wage is distinct from  5253 or S2.Wage is not distinct from  1000);";
        verifyStmtFails(client, sql, "unexpected token: SELECT");

        sql = "select * from S1 where S1.WAGE is not distinct from ANY " +
                "(select S2.wage from S2 where S2.wage is distinct from  5253 or S2.Wage is not distinct from  Null);";
        verifyStmtFails(client, sql, "unexpected token: SELECT");
    }

    private void subTestIsDistinctFromInCompatibleTypes(Client client) throws Exception
    {
        String sql;
        sql = "SELECT * FROM S1 A WHERE  A.WAGE is distinct from \'Z\';";
        verifyStmtFails(client, sql, "incompatible data types in combination");
    }

    public void testCaseWhen() throws Exception {
        System.out.println("STARTING test Case When...");
        Client cl = getClient();
        VoltTable vt;
        String sql;

        //                           ID, DESC,   NUM, FLOAT, TIMESTAMP
        cl.callProcedure("R1.insert", 1, "VoltDB", 1, 1.0, new Timestamp(1000000000000L));
        cl.callProcedure("R1.insert", 2, "Memsql",  5, 5.0, new Timestamp(1000000000000L));

        sql = "SELECT ID, CASE WHEN num < 3 THEN 0 ELSE 8 END FROM R1 ORDER BY 1;";
        validateTableOfLongs(cl, sql, new long[][] {{1, 0},{2, 8}});

        sql = "SELECT ID, CASE WHEN num < 3 THEN num/2 ELSE num + 10 END FROM R1 ORDER BY 1;";
        validateTableOfLongs(cl, sql, new long[][] {{1, 0},{2, 15}});

        sql = "SELECT ID, CASE WHEN num > 0 AND num < 5 THEN num * 5 " +
                "WHEN num >=5 THEN num * 10  ELSE num END FROM R1 ORDER BY 1;";
        validateTableOfLongs(cl, sql, new long[][] {{1, 5},{2, 50}});


        // (2) Test case when Types.
        sql = "SELECT ID, CASE WHEN num > 0 AND num < 5 THEN NULL " +
                "WHEN num >=5 THEN num * 10  ELSE num END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(VoltType.BIGINT, vt.getColumnType(1));
        validateTableOfLongs(vt, new long[][] {{1, Long.MIN_VALUE},{2, 50}});

        sql = "SELECT ID, CASE WHEN num > 0 AND num < 5 THEN NULL " +
                "WHEN num >=5 THEN NULL  ELSE num END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(VoltType.INTEGER, vt.getColumnType(1));
        validateTableOfLongs(vt, new long[][] {{1, Long.MIN_VALUE},{2, Long.MIN_VALUE}});

        // Expected failed type cases:
        try {
            sql = "SELECT ID, CASE WHEN num > 0 AND num < 5 THEN NULL " +
                    "WHEN num >=5 THEN NULL ELSE NULL END FROM R1 ORDER BY 1;";
            vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
            fail();
        } catch (Exception ex) {
            assertNotNull(ex);
            assertTrue(ex.getMessage().contains("data type cast needed for parameter or null literal"));
        }

        try {
            // Use String as the casted type
            sql = "SELECT ID, CASE WHEN num > 0 AND num < 5 THEN NULL " +
                    "WHEN num >=5 THEN NULL ELSE 'NULL' END FROM R1 ORDER BY 1;";
            vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        } catch (Exception ex) {
            fail();
        }

        try {
            sql = "SELECT ID, CASE WHEN num > 0 AND num < 5 THEN NULL " +
                    "WHEN num >=5 THEN 'I am null'  ELSE num END FROM R1 ORDER BY 1;";
            vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
            // hsql232 ENG-8586 CASE WHEN having no incompatibility problem with this: fail();
        } catch (Exception ex) {
            assertNotNull(ex);
            assertTrue(ex.getMessage().contains("incompatible data types in combination"));
        }

        // Test string types
        sql = "SELECT ID, CASE WHEN desc > 'Volt' THEN 'Good' ELSE 'Bad' END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(2, vt.getRowCount());
        vt.advanceRow();
        assertEquals(vt.getLong(0), 1);
        assertTrue(vt.getString(1).equals("Good"));
        vt.advanceRow();
        assertEquals(vt.getLong(0), 2);
        if (isHSQL()) {
            assertTrue(vt.getString(1).contains("Bad"));
        }
        else {
            assertTrue(vt.getString(1).equals("Bad"));
        }


        // Test string concatenation
        sql = "SELECT ID, desc || ':' ||  CASE WHEN desc > 'Volt' THEN 'Good' ELSE 'Bad' END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(2, vt.getRowCount());
        vt.advanceRow();
        assertEquals(vt.getLong(0), 1);
        assertTrue(vt.getString(1).equals("VoltDB:Good"));
        vt.advanceRow();
        assertEquals(vt.getLong(0), 2);
        if (isHSQL()) {
            assertTrue(vt.getString(1).contains("Memsql:Bad"));
        }
        else {
            assertTrue(vt.getString(1).equals("Memsql:Bad"));
        }

        // Test inlined varchar/varbinary value produced by CASE WHEN.
        // This is regression coverage for ENG-6666.
        sql = "INSERT INTO INLINED_VC_VB_TABLE (ID, VC1, VC2, VB1, VB2) " +
            "VALUES (72, 'FOO', 'BAR', 'DEADBEEF', 'CDCDCDCD');";
        cl.callProcedure("@AdHoc", sql);
        sql = "SELECT CASE WHEN ID > 11 THEN VC1 ELSE VC2 END FROM INLINED_VC_VB_TABLE WHERE ID = 72;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        vt.advanceRow();
        assertEquals("FOO", vt.getString(0));

        sql = "SELECT CASE WHEN ID > 11 THEN VB1 ELSE VB2 END FROM INLINED_VC_VB_TABLE WHERE ID = 72;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        vt.advanceRow();
        assertTrue(VoltTable.varbinaryToPrintableString(vt.getVarbinary(0)).contains("DEADBEEF"));

        cl.callProcedure("R1.insert", 3, "ORACLE",  8, 8.0, new Timestamp(1000000000000L));
        // Test nested case when
        sql = "SELECT ID, CASE WHEN num < 5 THEN num * 5 " +
                "WHEN num < 10 THEN CASE WHEN num > 7 THEN num * 10 ELSE num * 8 END " +
                "END FROM R1 ORDER BY 1;";
        validateTableOfLongs(cl, sql, new long[][] {{1, 5},{2, 40}, {3, 80}});


        // Test case when without ELSE clause
        sql = "SELECT ID, CASE WHEN num > 3 AND num < 5 THEN 4 " +
                "WHEN num >=5 THEN num END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(VoltType.INTEGER, vt.getColumnType(1));
        validateTableOfLongs(vt, new long[][] {{1, Long.MIN_VALUE},{2,5}, {3, 8}});

        sql = "SELECT ID, CASE WHEN num > 3 AND num < 5 THEN 4 " +
                "WHEN num >=5 THEN num*10 END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(VoltType.BIGINT, vt.getColumnType(1));
        validateTableOfLongs(vt, new long[][] {{1, Long.MIN_VALUE},{2,50}, {3, 80}});

        // Test NULL
        cl.callProcedure("R1.insert", 4, "DB2",  null, null, new Timestamp(1000000000000L));
        sql = "SELECT ID, CASE WHEN num < 3 THEN num/2 ELSE num + 10 END FROM R1 ORDER BY 1;";
        vt = cl.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(VoltType.INTEGER, vt.getColumnType(1));
        validateTableOfLongs(vt, new long[][] {{1, 0},{2, 15}, {3, 18}, {4, Long.MIN_VALUE}});

    }

    public void testCaseWhenLikeDecodeFunction() throws Exception {
        System.out.println("STARTING test Case When like decode function...");
        Client cl = getClient();
        String sql;

        //      ID, DESC,   NUM, FLOAT, TIMESTAMP
        cl.callProcedure("R1.insert", 1, "VoltDB", 1, 1.0, new Timestamp(1000000000000L));
        cl.callProcedure("R1.insert", 2, "MySQL",  5, 5.0, new Timestamp(1000000000000L));

        sql = "SELECT ID, CASE num WHEN 3 THEN 3*2 WHEN 1 THEN 0 ELSE 10 END FROM R1 ORDER BY 1;";
        validateTableOfLongs(cl, sql, new long[][] {{1, 0},{2, 10}});

        // No ELSE clause
        sql = "SELECT ID, CASE num WHEN 1 THEN 10 WHEN 2 THEN 1 END FROM R1 ORDER BY 1;";
        validateTableOfLongs(cl, sql, new long[][] {{1, 10},{2, Long.MIN_VALUE}});

        // Test NULL
        cl.callProcedure("R1.insert", 3, "Oracle",  null, null, new Timestamp(1000000000000L));
        sql = "SELECT ID, CASE num WHEN 5 THEN 50 ELSE num + 10 END FROM R1 ORDER BY 1;";
        validateTableOfLongs(cl, sql, new long[][] {{1, 11},{2, 50}, {3, Long.MIN_VALUE}});
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestComparisonOperatorsSuite.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        try {
            setUpSchema(project);
            // CONFIG #1: Local Site/Partitions running on JNI backend
            config = new LocalCluster("try-voltdbBackend.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            // alternative to enable for debugging */ config = new LocalCluster("IPC-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_IPC);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);

            // CONFIG #2: HSQL
            config = new LocalCluster("try-hsqlBackend.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);

        }
        catch(IOException excp) {
            assertFalse(true);
        }
        // no clustering tests for functions

        return builder;
    }
}
