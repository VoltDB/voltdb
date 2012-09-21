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

import org.voltdb.BackendTarget;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;

/**
 * Tests for SQL that was recently (early 2012) unsupported.
 */

public class TestFunctionsForVoltDBSuite extends RegressionSuite {

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

    public void testExplicitErrorUDF() throws Exception
    {
        System.out.println("STARTING testExplicitErrorUDF");
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

        // Exercise basic syntax without runtime invocation.
        cr = client.callProcedure("@AdHoc", "select SQL_ERROR(123) from P1 where ID = 0");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

        cr = client.callProcedure("@AdHoc", "select SQL_ERROR('abc') from P1 where ID = 0");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

        cr = client.callProcedure("@AdHoc", "select SQL_ERROR(123, 'abc') from P1 where ID = 0");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

        boolean caught = false;

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select SQL_ERROR(123, 'abc') from P1");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("abc") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select SQL_ERROR(123.5) from P1");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("Specific error code") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select SQL_ERROR('abc') from P1");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("abc") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            // This wants to be a statement compile-time error.
            cr = client.callProcedure("@AdHoc", "select SQL_ERROR(123, 123) from P1");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("INTEGER") != -1); // TODO match a more explicit pattern
            caught = true;
        }
        assertTrue(caught);

    }

    public void testOctetLength() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING OCTET_LENGTH");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "贾鑫Vo", 10, 1.1);
        cr = client.callProcedure("P1.insert", 2, "Xin", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("OCTET_LENGTH", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(8, result.getLong(1));

        cr = client.callProcedure("OCTET_LENGTH", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(3, result.getLong(1));

        // null case
        cr = client.callProcedure("OCTET_LENGTH", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(VoltType.NULL_BIGINT,result.getLong(1));
    }

    // this test is put here instead of TestFunctionSuite, because HSQL uses
    // a different null case standard with standard sql
    public void testPosition() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING Position");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "贾鑫Vo", 10, 1.1);
        cr = client.callProcedure("P1.insert", 2, "Xin@Volt", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("POSITION","Vo", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(3, result.getLong(1));

        cr = client.callProcedure("POSITION","DB", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(0, result.getLong(1));

        cr = client.callProcedure("POSITION","Vo", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(5, result.getLong(1));

        // null case
        cr = client.callProcedure("POSITION","Vo", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(VoltType.NULL_BIGINT,result.getLong(1));
    }

    // this test is put here instead of TestFunctionSuite, because HSQL uses
    // a different null case standard with standard sql
    public void testCharLength() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING Char length");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "贾鑫Vo", 10, 1.1);
        cr = client.callProcedure("P1.insert", 2, "Xin@Volt", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("CHAR_LENGTH", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(4, result.getLong(1));

        cr = client.callProcedure("CHAR_LENGTH", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(8, result.getLong(1));

        // null case
        cr = client.callProcedure("CHAR_LENGTH", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(VoltType.NULL_BIGINT,result.getLong(1));
    }

    public void testDECODE() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING DECODE");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "IBM", 10, 1.1);
        cr = client.callProcedure("P1.insert", 2, "Microsoft", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, "Hewlett Packard", 10, 1.1);
        cr = client.callProcedure("P1.insert", 4, "Gateway", 10, 1.1);
        cr = client.callProcedure("P1.insert", 5, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // match 1st condition
        cr = client.callProcedure("DECODE", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("zheng",result.getString(1));

        // match 2nd condition
        cr = client.callProcedure("DECODE", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("li",result.getString(1));

        // match 3rd condition
        cr = client.callProcedure("DECODE", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("at",result.getString(1));

        // match 4th condition
        cr = client.callProcedure("DECODE", 4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("VoltDB",result.getString(1));

        // null case
        cr = client.callProcedure("DECODE", 5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("where",result.getString(1));
    }

    public void testDECODENoDefault() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING DECODE No Default");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "zheng", 10, 1.1);
        cr = client.callProcedure("P1.insert", 2, "li", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // null case
        cr = client.callProcedure("DECODEND", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(null,result.getString(1));
    }

    public void testDECODEVeryLong() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING DECODE Exceed Limit");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "zheng", 10, 1.1);
        cr = client.callProcedure("P1.insert", 2, "li", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // null case
        cr = client.callProcedure("DECODEVERYLONG", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("where",result.getString(1));
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestFunctionsForVoltDBSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestFunctionsForVoltDBSuite.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "DESC VARCHAR(300), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PRIMARY KEY (ID) ); ";
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        project.addPartitionInfo("P1", "ID");
        // Test DECODE
        project.addStmtProcedure("DECODE", "select desc,  DECODE (desc,'IBM','zheng'," +
                        "'Microsoft','li'," +
                        "'Hewlett Packard','at'," +
                        "'Gateway','VoltDB'," +
                        "'where') from P1 where id = ?");
        project.addStmtProcedure("DECODEND", "select desc,  DECODE (desc,'zheng','a') from P1 where id = ?");
        project.addStmtProcedure("DECODEVERYLONG", "select desc,  DECODE (desc,'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'where') from P1 where id = ?");
        // Test OCTET_LENGTH
        project.addStmtProcedure("OCTET_LENGTH", "select desc,  OCTET_LENGTH (desc) from P1 where id = ?");
        // Test POSITION and CHAR_LENGTH
        project.addStmtProcedure("POSITION", "select desc, POSITION (? IN desc) from P1 where id = ?");
        project.addStmtProcedure("CHAR_LENGTH", "select desc, CHAR_LENGTH (desc) from P1 where id = ?");

        // CONFIG #1: Local Site/Partition running on JNI backend
        config = new LocalCluster("fixedsql-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CONFIG #2: Local Site/Partitions running on JNI backend
        config = new LocalCluster("fixedsql-threesite.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
/*

        // CONFIG #2: HSQL -- disabled, the functions being tested are not HSQL compatible
        config = new LocalCluster("fixedsql-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

*/
        // no clustering tests for functions

        return builder;
    }
}
