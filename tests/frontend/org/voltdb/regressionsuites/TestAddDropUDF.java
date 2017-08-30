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
import java.util.Set;

import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

import junit.framework.Test;

public class TestAddDropUDF extends RegressionSuite {
    private void addFunction(Client client, String functionName, String methodName) throws IOException, ProcCallException {
        try {
            client.callProcedure("@AdHoc",
                String.format("CREATE FUNCTION %s FROM METHOD org.voltdb_testfuncs.%s;", functionName, methodName));
        } catch (ProcCallException pce) {
            pce.printStackTrace();
            fail(String.format("Should be able to CREATE FUNCTION %s FROM METHOD org.voltdb_testfuncs.%s;",
                    functionName, methodName));
        }
    }

    private void dropFunction(Client client, String functionName) throws IOException, ProcCallException {
        try {
            client.callProcedure("@AdHoc",
                String.format("DROP FUNCTION %s;", functionName));
        } catch (ProcCallException pce) {
            pce.printStackTrace();
            fail(String.format("Should be able to drop function %s", functionName));
        }
    }

    public void testFunctionNameCaseInsensitivity() throws IOException, ProcCallException {
        Client client = getClient();
        addFunction(client, "testfunc", "BasicTestUDFSuite.constantIntFunction");
        verifyStmtFails(client, "CREATE FUNCTION testFUNC FROM METHOD org.voltdb_testfuncs.BasicTestUDFSuite.constantIntFunction;",
                "Function \"testfunc\" is already defined");
        dropFunction(client, "testfunc");
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
                System.out.printf("Calling function named %s from method %s.%s\n",
                                  functionName, "BasicTestUDFSuite", methodName);
                verifyStmtFails(client,
                        String.format("SELECT %s(%s) FROM T;", functionName,
                                String.join(", ", Collections.nCopies(i, "a"))),
                        "user lacks privilege or object not found: TESTFUNC");
                System.out.printf("Adding function %s named from method %s.%s\n",
                                  functionName, "BasicTestUDFSuite", methodName);
                addFunction(client, functionName, "BasicTestUDFSuite." + methodName);
                VoltTable[] results = client.callProcedure("@AdHoc",
                        String.format("SELECT %s(%s) FROM T;", functionName,
                                String.join(", ", Collections.nCopies(i, "a")))).getResults();
                assert(results != null);
                assertEquals(1, results.length);
                VoltTable t = results[0];
                assertContentOfTable(new Object[][] {{i}, {i}}, t);
                System.out.printf("Dropping function named %s from method %s.%s\n",
                                  functionName, "BasicTestUDFSuite", methodName);
                dropFunction(client, functionName);
                verifyStmtFails(client,
                        String.format("SELECT %s(%s) FROM T;", functionName,
                                String.join(", ", Collections.nCopies(i, "a"))),
                        "user lacks privilege or object not found: TESTFUNC");
            }
        }
    }

    String catalogMatchesCompilerFunctionSet(Client client) throws NoConnectionsException, IOException, ProcCallException {
        StringBuffer success = new StringBuffer();
        Set<String> dfns = FunctionForVoltDB.getAllUserDefinedFunctionNamesForDebugging();
        VoltTable vt = client.callProcedure("@SystemCatalog", "functions").getResults()[0];
        if (dfns.size() != vt.getRowCount()) {
            success.append(String.format("Compiler set has %d elements, catalog has %d functions\n",
                                         dfns.size(), vt.getRowCount()));
        } else {
            return "";
        }
        success.append("Catalog:\n");
        while (vt.advanceRow()) {
            String name = vt.getString("FUNCTION_NAME");
            success.append("    ")
                   .append(name);
            if ( ! dfns.contains(name)) {
                success.append("(*)");
            }
            success.append("\n");
        }
        success.append("FunctionForVolt:\n");
        for (String dfn : dfns) {
            success.append("    " + dfn + "\n");
        }
        return success.toString();
    }

    public void testDropFunction() throws Exception {
        String catalogError;
        Client client = getClient();

        ClientResponse cr;
        cr = client.callProcedure("@AdHoc", "create table R1 ( BIG BIGINT );");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        cr = client.callProcedure("@AdHoc", "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        cr = client.callProcedure("@AdHoc", "create procedure proc as select ADD2biginT(BIG, BIG) from R1;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        // This should fail, since we can't use UDFs in index expressions.
        try {
            FunctionForVoltDB.logTableState("Before creating bad index");
            cr = client.callProcedure("@AdHoc", "create index alphidx on R1 ( add2bigint(BIG, BIG) );");
            fail("Should not be able to create index with UDF.");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Index \"ALPHIDX\" with user defined function calls is not supported"));
        }

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        cr = client.callProcedure("@AdHoc", "create view vvv as select BIG, COUNT(*), MAX(BIG) from R1 group by BIG");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        // This should fail, since we can't use UDFs in materialized views.
        try {
            cr = client.callProcedure("@AdHoc", "create view alphview as select BIG, COUNT(*), MAX(ADD2BIGINT(BIG, BIG)) from R1 group by BIG;");
            fail("Should not be able to create materialized view with UDF.");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Materialized view \"ALPHVIEW\" with user defined function calls is not supported"));
        }

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        // This should fail because the procedure proc depends on add2bigint.
        try {
            cr = client.callProcedure("@AdHoc", "drop function add2bigint");
            fail("Should not be able to drop add2bigint because proc depends on it.");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Failed to plan for statement (sql) \"select ADD2biginT(BIG, BIG) from R1;\"."));
        }

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        //
        // The procedure should still exist, because the drop function failed.  We can call it in
        // proc and in adhoc sql.
        //
        cr = client.callProcedure("proc");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        FunctionForVoltDB.logTableState("After proc creation");

        cr = client.callProcedure("@AdHoc", "select add2bigint(big, big) from R1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // Drop the procedure.  All should be well.
        cr = client.callProcedure("@AdHoc", "drop procedure proc");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        // This should work because nothing depends on the function now.
        cr = client.callProcedure("@AdHoc", "drop function add2BIGINT");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        // This should fail because we dropped the procedure.
        try {
            cr = client.callProcedure("@AdHoc", "create procedure proc as select ADD2biginT(BIG, BIG) from R1;");
            fail("Should not be able to recreate proc.");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("user lacks privilege or object not found: ADD2BIGINT"));
        }

        // See if we can do it all over again.
        cr = client.callProcedure("@AdHoc", "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        cr = client.callProcedure("@AdHoc", "create procedure proc as select ADD2biginT(BIG, BIG) from R1;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        cr = client.callProcedure("proc");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        cr = client.callProcedure("@AdHoc", "select add2bigint(big, big) from R1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        cr = client.callProcedure("@AdHoc", "drop procedure proc if exists");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // Check tuple limit delete.
        try {
            cr = client.callProcedure("@AdHoc", "create table R2 ( id bigint, "
                                                 + "limit partition rows 100 "
                                                 + "execute ( delete from r2 "
                                                 + "where add2bigint(id, id) < 100 ) )");
            fail("tuple limit delete with a call to a user defined function should not compile.");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("user defined function calls are not supported: \"add2bigint\""));
        }

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        // Drop everything.  RegressionSuite seems to want this.
        cr = client.callProcedure("@AdHoc", "drop function add2bigint;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "drop view VVV");

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        cr = client.callProcedure("@AdHoc", "drop table r1 if exists");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        cr = client.callProcedure("@AdHoc", "drop table r2 if exists");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

    }

    public TestAddDropUDF(String name) {
        super(name);
    }

    static Class<?>[] UDF_CLASSES = {org.voltdb_testfuncs.BasicTestUDFSuite.class};

    static public Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAddDropUDF.class);

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
