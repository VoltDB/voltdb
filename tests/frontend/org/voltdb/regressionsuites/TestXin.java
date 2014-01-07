/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

/**
 * Tests for SQL that was recently (early 2012) unsupported.
 */

public class TestXin extends RegressionSuite {


    public void validateSchema(VoltTable result, VoltTable expected)
    {
        assertEquals(expected.getColumnCount(), result.getColumnCount());
        for (int i = 0; i < result.getColumnCount(); i++) {
            assertEquals("Failed name column: " + i, expected.getColumnName(i), result.getColumnName(i));
            assertEquals("Failed type column: " + i, expected.getColumnType(i), result.getColumnType(i));
        }
    }

    public void testDMLin() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING DML in keyword");
        Client client = getClient();
        VoltTable result = null;

        VoltTable[] results = null;
        // table
        //
        results = client.callProcedure("@GetPartitionKeys", "INTEGER").getResults();
        System.out.println("Test statistics table: " + results[0].toString());

    }


    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestXin(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestXin.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
//                "CREATE TABLE P1 ( " +
//                "ID INTEGER NOT NULL, " +
//                "var1 VARCHAR(300) Default 'NULL' , " +
//                "RATIO FLOAT Default NULL, " +
//                "tm timestamp, " +
//                "PRIMARY KEY (ID) ); " +
//                "PARTITION TABLE P1 ON COLUMN ID; " +
//                "create index idx_c1 on p1 (c1);" +
//                "create index idx_2 on r1 (id, c1);" +

                        "CREATE TABLE NEW_ORDER (\n" +
                        "  NO_W_ID SMALLINT DEFAULT '0' NOT NULL\n" +
                        ");\n" +
                ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        // CONFIG #1: Local Site/Partition running on JNI backend
        config = new LocalCluster("fixedsql-onesite.jar", 3, 2, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CONFIG #2: HSQL -- disabled, the functions being tested are not HSQL compatible
//        config = new LocalCluster("fixedsql-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
//        success = config.compile(project);
//        assertTrue(success);
//        builder.addServerConfig(config);

        // no clustering tests for functions

        return builder;
    }
}