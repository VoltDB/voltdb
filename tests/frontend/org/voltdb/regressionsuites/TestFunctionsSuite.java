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
            client.callProcedure(callback, "P1.insert", - id, "X"+String.valueOf(id), 10, 1.1);
            client.drain();
        }
        ClientResponse cr = null;
        VoltTable r = null;

        cr = client.callProcedure("WHERE_ABS");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        r = cr.getResults()[0];
        assertTrue(r.asScalarLong() == 5);

        try {
            // test decimal support and non-column expressions
            cr = client.callProcedure("WHERE_ABSFF");
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
            r = cr.getResults()[0];
            assertTrue(r.asScalarLong() == 5);
        } catch (ProcCallException hsqlFailed) {
            // Give HSQLDB a pass on this query.
            String msg = hsqlFailed.getMessage();
            assertTrue(msg.matches(".*ExpectedProcedureException.*HSQLDB.*"));
        }

        // Test type promotions
        cr = client.callProcedure("WHERE_ABSIF");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        r = cr.getResults()[0];
        assertTrue(r.asScalarLong() == 5);

        try {
            cr = client.callProcedure("WHERE_ABSFI");
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
            r = cr.getResults()[0];
        assertTrue(r.asScalarLong() == 5);
        } catch (ProcCallException hsqlFailed) {
            // Give HSQLDB a pass on this query.
            String msg = hsqlFailed.getMessage();
            assertTrue(msg.matches(".*ExpectedProcedureException.*HSQLDB.*"));
        }


        // Test application to weakly typed NUMERIC constants
        try {
            cr = client.callProcedure("WHERE_ABSWEAK");
            assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
            r = cr.getResults()[0];
            assertTrue(r.asScalarLong() == 5);
        } catch (ProcCallException hsqlFailed) {
            // Give HSQLDB a pass on this query.
            String msg = hsqlFailed.getMessage();
            assertTrue(msg.matches(".*ExpectedProcedureException.*HSQLDB.*"));
        }

        cr = client.callProcedure("DISPLAY_ABS");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        r = cr.getResults()[0];
        assertTrue(r.asScalarLong() == 5);

        cr = client.callProcedure("ORDER_ABS");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        r = cr.getResults()[0];
        r.advanceRow();
        long value = r.getLong(0);
        assertTrue(value == 5);
/*
        cr = client.callProcedure("GROUP_ABS");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        r = cr.getResults()[0];
        assertTrue(r.asScalarLong() == 5);
*/
        cr = client.callProcedure("AGG_OF_ABS");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        r = cr.getResults()[0];
        assertTrue(r.asScalarLong() == 5);
/*
        cr = client.callProcedure("ABS_OF_AGG");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        r = cr.getResults()[0];
        assertTrue(r.asScalarLong() == 5);
*/
        // Test null propagation
        cr = client.callProcedure("INSERT_NULL", 99);
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        cr = client.callProcedure("INSERT_NULL", 98);
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        cr = client.callProcedure("INSERT_NULL", 97);
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        cr = client.callProcedure("INSERT_NULL", 96);
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        cr = client.callProcedure("INSERT_NULL", 95);
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

        long resultA;
        long resultB;

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where NUM > 9");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where ABS(NUM) > 9");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertTrue(resultA == resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where ABS(0-NUM) > 9");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertTrue(resultA == resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not NUM > 9");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        r = cr.getResults()[0];
        resultA = r.asScalarLong();

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not ABS(0-NUM) > 9");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertTrue(resultA == resultB);

        cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not ABS(NUM) > 9");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        r = cr.getResults()[0];
        resultB = r.asScalarLong();
        assertTrue(resultA == resultB);

        boolean caught = false;

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not ABS(DESC) > 9");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("incompatible data type") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select count(*) from P1 where not ABS(DESC) > 'ABC'");
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
        project.addSchema(Insert.class.getResource("fixed-sql-ddl.sql"));
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
        project.addStmtProcedure("INSERT_NULL", "insert into P1 values (?, null, null, null)");
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
