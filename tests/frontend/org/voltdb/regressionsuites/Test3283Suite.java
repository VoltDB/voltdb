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
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;

/**
 * Tests for SQL that was recently (early 2012) unsupported.
 */

public class Test3283Suite extends RegressionSuite {

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
        client.callProcedure(callback, "P1.insert", 1, "贾鑫V", " NB",10);
        client.callProcedure(callback, "P1.insert", 2, "Xin", " @Volt", 10);
        ClientResponse cr = null;
        VoltTable r = null;

        cr = client.callProcedure("RIGHT", 0, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        r = cr.getResults()[0];
        System.err.println("[RIGHT- 0] result:\n" + r);

    }


    //
    // JUnit / RegressionSuite boilerplate
    //
    public Test3283Suite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(Test3283Suite.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "VCA VARCHAR(300), " +
                "VCB VARCHAR(300), " +
                "NUM INTEGER, " +
                "PRIMARY KEY (ID) ); ";
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        project.addPartitionInfo("P1", "ID");

//        project.addStmtProcedure("CONCAT", "select id, CONCAT(VCA,VCB) from P1 where id = ?");
//        project.addStmtProcedure("CONCAT", "select id, VCA||VCB from P1 where id = ?");
          project.addStmtProcedure("LEFT", "select id, LEFT(VCA,?) from P1 where id = ?");
          project.addStmtProcedure("RIGHT", "select id, RIGHT(VCA,?) from P1 where id = ?");
//        project.addStmtProcedure("LTRIM", "select LTRIM(VCA) from P1 where id = ?");
//        project.addStmtProcedure("RTRIM", "select RTRIM(VCA) from P1 where id = ?");
//        project.addStmtProcedure("LTRIM", "select LTRIM(VCA) from P1 where id = ?");
          project.addStmtProcedure("SPACE", "select SPACE(2) from P1 where id = ?");

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
