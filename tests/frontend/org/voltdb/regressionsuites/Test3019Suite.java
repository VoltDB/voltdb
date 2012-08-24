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
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;

/**
 * Tests for SQL that was recently (early 2012) unsupported.
 */

public class Test3019Suite extends RegressionSuite {

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
                PAST TIMESTAMP DEFAULT NULL,
                PRIMARY KEY (ID)
                );
        */
        for(int id=8; id < 11; id++) {
            //client.callProcedure(callback, "P1.insert", - id, "Xin"+String.valueOf(id), 10, 1.1, new Timestamp(100000000L));
            //client.callProcedure(callback, "P1.insert", - id, "贾"+String.valueOf(id), 10, 1.1, new Timestamp(100000000L));
            //client.drain();
        }
        client.callProcedure(callback, "P1.insert", 1, "贾鑫Vo", 10, 1.1, new Timestamp(100000000L));
        client.callProcedure(callback, "P1.insert", 2, "Xin@Volt", 10, 1.1, new Timestamp(100000000L));
        ClientResponse cr = null;
        VoltTable r = null;

        cr = client.callProcedure("CHAR_LENGTH");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        System.err.println("[CHAR_LENGTH] result:\n" + r);

        cr = client.callProcedure("OCTET_LENGTH");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        System.err.println("[OCTET_LENGTH] result:\n" + r);


        cr = client.callProcedure("POSITION","Vo");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        System.err.println("[POSITION-after] result:\n" + r);


        // Test application to weakly typed NUMERIC constants
//        try {
//            cr = client.callProcedure("WHERE_ABSWEAK");
//            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
//            r = cr.getResults()[0];
//            assertEquals(5, r.asScalarLong());
//        } catch (ProcCallException hsqlFailed) {
//            // Give HSQLDB a pass on this query.
//            String msg = hsqlFailed.getMessage();
//            assertTrue(msg.matches(".*ExpectedProcedureException.*HSQLDB.*"));
//        }
    }


    //
    // JUnit / RegressionSuite boilerplate
    //
    public Test3019Suite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(Test3019Suite.class);
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

        project.addStmtProcedure("POSITION", "select desc, POSITION (? IN desc) from P1");
        project.addStmtProcedure("OCTET_LENGTH", "select desc,  OCTET_LENGTH (desc) from P1");
        project.addStmtProcedure("CHAR_LENGTH", "select desc, CHAR_LENGTH (desc) from P1");

        project.addStmtProcedure("INSERT_NULL", "insert into P1 values (?, null, null, null, null)");
        // project.addStmtProcedure("UPS", "select count(*) from P1 where UPPER(DESC) > 'L'");

        // CONFIG #1: Local Site/Partitions running on JNI backend
        config = new LocalCluster("fixedsql-threesite.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CONFIG #2: HSQL
//        config = new LocalCluster("fixedsql-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
//        success = config.compile(project);
//        assertTrue(success);
//        builder.addServerConfig(config);

        // no clustering tests for functions

        return builder;
    }
}
