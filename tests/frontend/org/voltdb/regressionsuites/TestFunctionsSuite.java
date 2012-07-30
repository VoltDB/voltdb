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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.sql.Timestamp;

import org.voltdb.BackendTarget;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;

/**
 * Tests for SQL that was recently (early 2012) unsupported.
 */

public class TestFunctionsSuite extends RegressionSuite {

    /**
     * Inner class procedure to see if we can invoke it.
     */
    public static class InnerProc extends VoltProcedure {
        public long run() {
            return 0L;
        }
    }

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

    public void testExtract() throws Exception
    {
        System.out.println("STARTING testExtract");
        Client client = getClient();
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException("Failed with response: " + clientResponse.getStatusString());
                }
            }
        };
        /*
        CREATE TABLE P1 (
                ID INTEGER DEFAULT '0' NOT NULL,
                DESC VARCHAR(300),
                NUM INTEGER,
                RATIO FLOAT,
                PAST TIMESTAMP DEFAULT NULL,
                PRIMARY KEY (ID)
                );
        */
        client.callProcedure(callback, "P1.insert", 0, "X0", 10, 1.1, new Timestamp(100000000L));
        client.drain();
        ClientResponse cr = null;
        VoltTable r = null;
        long result;

        cr = client.callProcedure("@AdHoc",
                                  "select " +
                                  "EXTRACT(YEAR FROM PAST) " +
                                  //"EXTRACT(YEAR FROM PAST), EXTRACT(MONTH FROM PAST), EXTRACT(DAY FROM PAST), " +
                                  //"EXTRACT(DAY_OF_WEEK FROM PAST), EXTRACT(DAY_OF_YEAR FROM PAST), " +
                                  //"EXTRACT(WEEK_OF_YEAR FROM PAST), " +
                                  //"EXTRACT(QUARTER FROM PAST), EXTRACT(HOUR FROM PAST), EXTRACT(MINUTE FROM PAST), EXTRACT(SECOND FROM PAST) " +
                                  "from P1;");

        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        int columnIndex = 0;

        /*
        final int EXPECTED_YEAR = 2001;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_YEAR, result);

        final int EXPECTED_MONTH = 9;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_MONTH, result);

        final int EXPECTED_DAY = 9;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DAY, result);

        final int EXPECTED_DOW = 6;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOW, result);

        final int EXPECTED_DOY = 400;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_DOY, result);

//        final int EXPECTED_WOY = 400;
//        result = r.getLong(columnIndex++);
//        assertEquals(EXPECTED_WOY, result);

        final int EXPECTED_QUARTER = 3;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_QUARTER, result);

        final int EXPECTED_HOUR = 1;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_HOUR, result);

        final int EXPECTED_MINUTE = 46;
        result = r.getLong(columnIndex++);
        assertEquals(EXPECTED_MINUTE, result);

        final double EXPECTED_SECONDS = 40.0;
        double floatresult = r.getDouble(columnIndex++);
        assertEquals(EXPECTED_SECONDS, floatresult);
*/
    }

    public void testAbs() throws Exception
    {
        System.out.println("STARTING testAbs");
        Client client = getClient();
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException("Failed with response: " + clientResponse.getStatusString());
                }
            }
        };
        /*
        CREATE TABLE P1 (
                ID INTEGER DEFAULT '0' NOT NULL,
                DESC VARCHAR(300),
                NUM INTEGER,
                RATIO FLOAT,
                PRIMARY KEY (ID)
                );
        */
        for(int id=7; id < 15; id++) {
            client.callProcedure(callback, "P1.insert", - id, "X"+String.valueOf(id), 10, 1.1, new Timestamp(100000000L));
            client.drain();
        }
        ClientResponse cr = null;
        VoltTable r = null;

        cr = client.callProcedure("WHERE_ABS");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong());

    }

    public void testSubstring() throws Exception
    {
        System.out.println("STARTING testSubstring");
        Client client = getClient();
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException("Failed with response: " + clientResponse.getStatusString());
                }
            }
        };
        /*
        CREATE TABLE P1 (
                ID INTEGER DEFAULT '0' NOT NULL,
                DESC VARCHAR(300),
                NUM INTEGER,
                RATIO FLOAT,
                PRIMARY KEY (ID)
                );
        */
        for(int id=7; id < 15; id++) {
            client.callProcedure(callback, "P1.insert", - id, "X"+String.valueOf(id), 10, 1.1, new Timestamp(100000000L));
            client.drain();
        }
        ClientResponse cr = null;
        VoltTable r = null;

        // test where support
        cr = client.callProcedure("WHERE_SUBSTRING2");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong());

        cr = client.callProcedure("WHERE_SUBSTRING3");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        assertEquals(5, r.asScalarLong());

        // Test select support
        cr = client.callProcedure("DISPLAY_SUBSTRING");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        assertEquals("12", r.getString(0));

        // Test ORDER BY by support
        cr = client.callProcedure("ORDER_SUBSTRING");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        long value = r.getLong(0);
        assertEquals(5, value);

        // Test GROUP BY by support
        cr = client.callProcedure("AGG_OF_SUBSTRING");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        r.advanceRow();
        assertEquals("10", r.getString(0));

        // Test null propagation
        cr = client.callProcedure("INSERT_NULL", 99);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("INSERT_NULL", 98);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("INSERT_NULL", 97);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("INSERT_NULL", 96);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("INSERT_NULL", 95);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        long resultA;
        long resultB;

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where DESC >= 'X11'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where SUBSTRING (DESC FROM 2) >= '11'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not DESC >= 'X12'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not SUBSTRING( DESC FROM 2) >= '12'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where DESC >= 'X2'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where SUBSTRING(DESC FROM 2 FOR 1) >= '2'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not SUBSTRING( SUBSTRING (DESC FROM 2) FROM 1 FOR 1) < '2'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertEquals(resultA, resultB);

        boolean caught = false;

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not SUBSTRING( DESC FROM 2) > 9");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("incompatible data type") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not SUBSTRING (1 FROM 2) > 9");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("incompatible data type") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not SUBSTRING (1 FROM DESC) > '9'");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("incompatible data type") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not SUBSTRING (DESC FROM DESC) > 'ABC'");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("incompatible data type") != -1);
            caught = true;
        }
        assertTrue(caught);

    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestFunctionsSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestFunctionsSuite.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "DESC VARCHAR(300), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PAST TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); ";
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        project.addPartitionInfo("P1", "ID");

        project.addStmtProcedure("WHERE_ABS", "select count(*) from P1 where ABS(ID) > 9");
        project.addStmtProcedure("WHERE_ABSFF", "select count(*) from P1 where ABS(ID - 0.4) > 9.5");
        project.addStmtProcedure("WHERE_ABSIF", "select count(*) from P1 where ABS(ID) > 9.5");
        project.addStmtProcedure("WHERE_ABSFI", "select count(*) from P1 where ABS(ID + 0.4) > 9");
        project.addStmtProcedure("WHERE_ABSWEAK", "select count(*) from P1 where ABS(ID - 0.4) > ABS(9.5)");

        project.addStmtProcedure("DISPLAY_ABS", "select ABS(ID)-2 from P1 where ID >= -7");
        project.addStmtProcedure("ORDER_ABS", "select ID+12 from P1 order by ABS(ID)");
        // GROUP BY with complex expressions not yet supported
        // project.addStmtProcedure("GROUP_ABS", "select MIN(ID+17) from P1 group by ABS(ID+12) order by ABS(ID+12)");
        project.addStmtProcedure("AGG_OF_ABS", "select MIN(ABS(ID)-2) from P1");
        // RuntimeException seems to stem from parser failure similar to ENG-2901
        // project.addStmtProcedure("ABS_OF_AGG", "select ABS(MIN(ID+9)) from P1");

        project.addStmtProcedure("WHERE_SUBSTRING2", "select count(*) from P1 where SUBSTRING (DESC FROM 2) > '12'");
        project.addStmtProcedure("WHERE_SUBSTRING3", "select count(*) from P1 where not SUBSTRING (DESC FROM 2 FOR 1) > '13'");

        // Test select support
        project.addStmtProcedure("DISPLAY_SUBSTRING", "select SUBSTRING (DESC FROM 2) from P1 where ID = -12");

        // Test ORDER BY by support
        project.addStmtProcedure("ORDER_SUBSTRING", "select ID+15 from P1 order by SUBSTRING (DESC FROM 2)");

        // Test GROUP BY by support
        project.addStmtProcedure("AGG_OF_SUBSTRING", "select MIN(SUBSTRING (DESC FROM 2)) from P1 where ID < -7");


        project.addStmtProcedure("INSERT_NULL", "insert into P1 values (?, null, null, null, null)");
        // project.addStmtProcedure("UPS", "select count(*) from P1 where UPPER(DESC) > 'L'");

        // CONFIG #1: Local Site/Partitions running on JNI backend
        config = new LocalCluster("fixedsql-threesite.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CONFIG #2: HSQL
        config = new LocalCluster("fixedsql-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // no clustering tests for functions

        return builder;
    }
}
