/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.util.Collections;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

import junit.framework.Test;

public class TestUDFSuite extends RegressionSuite {

    public void testFunctionNameCaseInsensitivity() throws IOException, ProcCallException {
        Client client = getClient();
        ClientResponse cr = client.callProcedure("@AdHoc",
                "CREATE FUNCTION testfunc FROM METHOD org.voltdb_testfuncs.IntFunction.constantIntFunction;");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        verifyStmtFails(client, "CREATE FUNCTION testFUNC FROM METHOD org.voltdb_testfuncs.IntFunction.constantIntFunction;",
                "Function \"testfunc\" is already defined");
        cr = client.callProcedure("@AdHoc", "DROP FUNCTION testfunc;");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
    }

    public void testAddRemoveUDF() throws IOException, ProcCallException {
        Client client = getClient();
        String[] functionNamesToTest = new String[] {"testfunc", "TESTFUNC", "testFunc"};
        String[] methodNamesToTest = new String[] {"constantIntFunction", "unaryIntFunction", "generalIntFunction"};
        assertSuccessfulDML(client, "INSERT INTO T VALUES (1, 'abc');");
        assertSuccessfulDML(client, "INSERT INTO T VALUES (2, 'def');");
        for (int i = 0; i < methodNamesToTest.length; i++) {
            String methodName = methodNamesToTest[i];
            for (String functionName : functionNamesToTest) {
                verifyStmtFails(client,
                        String.format("SELECT %s(%s) FROM T;", functionName,
                                String.join(", ", Collections.nCopies(i, "a"))),
                        "user lacks privilege or object not found: TESTFUNC");
                try {
                    client.callProcedure("@AdHoc",
                        String.format("CREATE FUNCTION %s FROM METHOD org.voltdb_testfuncs.IntFunction.%s;", functionName, methodName));
                } catch (ProcCallException pce) {
                    pce.printStackTrace();
                    fail(String.format("Should be able to CREATE FUNCTION %s FROM METHOD org.voltdb_testfuncs.IntFunction.%s;",
                            functionName, methodName));
                }
                VoltTable[] results = client.callProcedure("@AdHoc",
                        String.format("SELECT %s(%s) FROM T;", functionName,
                                String.join(", ", Collections.nCopies(i, "a")))).getResults();
                assert(results != null);
                assertEquals(1, results.length);
                VoltTable t = results[0];
                assertContentOfTable(new Object[][] {{null}, {null}}, t);
                try {
                    client.callProcedure("@AdHoc",
                        String.format("DROP FUNCTION %s;", functionName));
                } catch (ProcCallException pce) {
                    pce.printStackTrace();
                    fail(String.format("Should be able to drop function %s", functionName));
                }
                verifyStmtFails(client,
                        String.format("SELECT %s(%s) FROM T;", functionName,
                                String.join(", ", Collections.nCopies(i, "a"))),
                        "user lacks privilege or object not found: TESTFUNC");
            }
        }
    }

    public TestUDFSuite(String name) {
        super(name);
    }

    static Class<?>[] UDF_CLASSES = {org.voltdb_testfuncs.IntFunction.class};

    static public Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestUDFSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

        final String literalSchema =
                "CREATE TABLE T (a INT, b VARCHAR(10));";

        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 2 Local Sites/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        LocalCluster config = new LocalCluster("udf-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        //* enable for simplified config */ config = new LocalCluster("matview-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        // build the jarfile
        assertTrue(config.compile(project));
        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 3-node k=1 cluster
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("udf-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        return builder;
    }

}
