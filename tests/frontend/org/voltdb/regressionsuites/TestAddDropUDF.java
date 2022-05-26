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

    public void testFunctionNameCaseInsensitivity() throws Exception {
        Client client = getClient();
        dropEverything(client);
        addFunction(client, "testfunc", "BasicTestUDFSuite.constantIntFunction");
        verifyStmtFails(client, "CREATE FUNCTION testFUNC FROM METHOD org.voltdb_testfuncs.BasicTestUDFSuite.constantIntFunction;",
                "Function \"testfunc\" is already defined");
        dropFunction(client, "testfunc");
    }

    public void testAddRemoveUDF() throws Exception {
        Client client = getClient();
        dropEverything(client);
        client.callProcedure("@AdHoc", "CREATE TABLE T (a INT, b VARCHAR(10));");
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
                        "object not found: TESTFUNC");
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
                        "object not found: TESTFUNC");
            }
        }
        dropEverything(client);
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

        dropEverything(client);
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
        verifyStmtFails(client,
                "create index alphidx on R1 ( add2bigint(BIG, BIG) );",
                "Index \"ALPHIDX\" cannot contain calls to user defined functions");

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        cr = client.callProcedure("@AdHoc", "create view vvv as select BIG, COUNT(*), MAX(BIG) from R1 group by BIG");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        // This should fail, since we can't use UDFs in materialized views.
        verifyStmtFails(client,
                "create view alphview as select BIG, COUNT(*), MAX(ADD2BIGINT(BIG, BIG)) from R1 group by BIG;",
                "Materialized view \"ALPHVIEW\" cannot contain calls to user defined functions");

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        // This should fail because the procedure proc depends on add2bigint.
        verifyStmtFails(client,
                "drop function add2bigint",
                "Cannot drop user defined function \"add2bigint\".  The statement proc.sql0 depends on it");

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

        // This should fail because we dropped the function.
        verifyStmtFails(client,
                "create procedure proc as select ADD2biginT(BIG, BIG) from R1;",
                "object not found: ADD2BIGINT");

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

        catalogError = catalogMatchesCompilerFunctionSet(client);
        assertEquals("", catalogError);

        // Drop everything.  RegressionSuite seems to want this.
        dropEverything(client);
    }

    /**
     * Verify that we can:
     * <ol>
     *   <li>Create a function.</li>
     *   <li>Create a procedure which uses the function.</li>
     *   <li>Call the procedure.</li>
     *   <li>Catch a failed drop function, because the procedure needs it.</li>
     *   <li>Call the procedure again.</li>
     *   <li>Drop the procedure.</li>
     *   <li>Catch the failure when calling the dropped procedure,</li>
     *   <li>Create the procedure again, to test that the failed drop function
     *       did not really drop the function.</li>
     *   <li>Call the procedure again.</li>
     *   <li>Drop the procedure.</li>
     *   <li>Drop the function.</li>
     * </ol>
     * Note that the procedure must be named "p" for this test.
     *
     * @param client
     * @param funcCreate
     * @param procCreate
     * @param dropFunc
     * @param errMessage
     * @param dropProc
     * @throws Exception
     */
    private void checkDropFunction(Client client,
                                   String funcCreate,
                                   String procCreate,
                                   String procName,
                                   String dropFunc,
                                   String errMessage,
                                   String dropProc) throws Exception {
        ClientResponse cr;
        // Define a function and a procedure which depend on the function.
        // These should both succeed.
        cr = client.callProcedure("@AdHoc", funcCreate);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc", procCreate);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(procName);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // Try to drop the function.  This should fail.
        verifyStmtFails(client, dropFunc, errMessage);

        // Verify that the procedure is not dropped by calling it.
        cr = client.callProcedure(procName);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // Drop the procedure;
        cr = client.callProcedure("@AdHoc", dropProc);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // Create the procedure again, to verify that the function has
        // not been dropped.
        cr = client.callProcedure("@AdHoc", procCreate);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // Call it to make sure it was created.
        cr = client.callProcedure(procName);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // Drop the procedure (again);
        cr = client.callProcedure("@AdHoc", dropProc);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // Now we should be able to drop the function.
        cr = client.callProcedure("@AdHoc", dropFunc);
    }

    private String makeErrorMessage(String format,
                                    String procName,
                                    String stmtName) {
        return String.format(format, procName, stmtName);
    }

    private void checkDropFunctionWithInsertMaybe(Client client,
                                   String funcCreate,
                                   String procName,
                                   String procDDLCreateBody,
                                   String dropFunc,
                                   String errMessagePattern) throws Exception {
        // First, make some DDL strings..
        String procCreateDDL = String.format("create procedure %s as %s;",
                                              procName,
                                              procDDLCreateBody);
        String dropProc = String.format("drop procedure %s;", procName);
        String errMessage;
        // Then check this DDL procedure.
        errMessage = makeErrorMessage(errMessagePattern, procName, "sql0");
        checkDropFunction(client,
                funcCreate,
                procCreateDDL,
                procName,
                dropFunc,
                errMessage,
                dropProc);
        // Check insert into select if necessary.  The statement
        // name will be the same as the previous one, so the
        // error message does not need to change.
        //
        // This is not possible with a Java stored procedure.
        if (procDDLCreateBody.startsWith("select")) {
            String insertProcCreate
                = String.format("create procedure %s as insert into t1 %s;",
                                procName,
                                procDDLCreateBody);
            checkDropFunction(client,
                    funcCreate,
                    insertProcCreate,
                    procName,
                    dropFunc,
                    errMessage,
                    dropProc);
        }
    }

    public void testUDFProcedureDependences() throws Exception {
        Client client = getClient();
        ClientResponse cr;

        dropEverything(client);

        cr = client.callProcedure("@AdHoc", "create table t1 ( id bigint );");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        ////////////////////////////////////////////////////////////////////////
        //
        // Check select queries.
        //
        ////////////////////////////////////////////////////////////////////////
        // Check for UDFs in the display list.
        // All of these calls to checkDropFunctionWithInsertMaybe take
        // some strings which are parts of the function call.  For each
        // of these call we have a stored procedure defined.  We create a
        // procedure, either with DDL or from the class, both of which
        // use the UDF.  We then try to drop the UDF and hope we get an
        // error message.  If the DDL statement is a select we try to
        // use that select text in an insert from select statement.
        //
        // See checkDropFunctionWithInsertMaybe for more details.
        //
        checkDropFunctionWithInsertMaybe(client,
                // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInDisplayList",
                // procDDLCreateBody
                "select add2bigint(id, id) from t1;",
                // dropFunc
                "drop function add2bigint",
                // errMsg.  The two %s are replaced by the procName and stmtName.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");
        // Check for UDFs in partition by in window functions
         checkDropFunctionWithInsertMaybe(client,
                // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInWindowPartition",
                // procDDLCreateBody
                "select sum(id) over (partition by add2bigint(id, id)) from t1;",
                // dropFunc
                "drop function add2bigint",
                // errMsg.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");

        // Check for UDFs in order by in window functions
         checkDropFunctionWithInsertMaybe(client,
                // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInWindowOrderBy",
                // procDDLCreateBody
                "select rank() over (order by add2bigint(id, id)) from t1;",
                // dropFunc
                "drop function add2bigint",
                // errMsg.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");
        // Check for UDFs in a where clause
        checkDropFunctionWithInsertMaybe(client,
                // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInWhere",
                // procDDLCreateBody
                "select id from t1 where add2bigint(id, id) > 0;",
                // dropFunc
                "drop function add2bigint",
                // errMsg.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");

        // Check for UDFs in the a join tree.
        checkDropFunctionWithInsertMaybe(client,
                // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInJoinTree",
                // procDDLCreateBody
                "select l.id from t1 as l join t1 as r on add2bigint(l.id, r.id) > 0;",
                // dropFunc
                "drop function add2bigint",
                // errMsg.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");

        // Check for UDFs in an order by.
         checkDropFunctionWithInsertMaybe(client,
                 // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInOrderBy",
                // procDDLCreateBody
                "select id from t1 order by add2bigint(id+1, id+2);",
                // dropFunc
                "drop function add2bigint",
                // errMsg.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");

        // Check for UDFs in group by expressions
        checkDropFunctionWithInsertMaybe(client,
                // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInGroupBy",
                // procDDLCreateBody
                "select sum(add2bigint(id+1, id+2)) from t1 group by add2bigint(id+1, id+2);",
                // dropFunc
                "drop function add2bigint",
                // errMsg.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");

        // Check for UDFs in having expressions
        checkDropFunctionWithInsertMaybe(client,
                // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInHaving",
                // procDDLCreateBody
                "select sum(id) from t1 group by id having sum(add2bigint(id, id)) > 0",
                // dropFunc
                "drop function add2bigint",
                // errMsg.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");

        ////////////////////////////////////////////////////////////////////////
        //
        // Check for UDFs in update statements.
        //
        ////////////////////////////////////////////////////////////////////////
        // Check for UDFs in set rhs in update expressions.
        checkDropFunctionWithInsertMaybe(client,
                // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInUpdateSet",
                // procDDLCreateBody
                "update t1 set id = add2bigint(id, id);",
                // dropFunc
                "drop function add2bigint",
                // errMsg.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");

        // Check for UDFs in where expressions in update expressions
        checkDropFunctionWithInsertMaybe(client,
                // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInUpdateWhere",
                // procDDLCreateBody
                "update t1 set id = id+1 where add2bigint(id, id) > 0;",
                // dropFunc
                "drop function add2bigint",
                // errMsg.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");
        ////////////////////////////////////////////////////////////////////////
        //
        // Check for UDFs in delete statements.
        //
        ////////////////////////////////////////////////////////////////////////
        // Check for UDFs in where expressions in delete expressions
        checkDropFunctionWithInsertMaybe(client,
                // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInDeleteWhere",
                // procDDLCreateBody
                "delete from t1 where add2bigint(id, id) > 0;",
                // dropFunc
                "drop function add2bigint",
                // errMsg.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");
        // Check for UDFs in order by in delete expressions
        checkDropFunctionWithInsertMaybe(client,
                // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInDeleteWhere",
                // procDDLCreateBody
                "delete from t1 order by id, add2bigint(id, id) limit 100;",
                // dropFunc
                "drop function add2bigint",
                // errMsg.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");
        ////////////////////////////////////////////////////////////////////////
        //
        // Check for UDFs in insert statements.
        //
        ////////////////////////////////////////////////////////////////////////
        // Check for UDFs in subqueries of insert.
        checkDropFunctionWithInsertMaybe(client,
                // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInInsertSubquery",
                // procDDLCreateBody
                "insert into t1 select add2bigint(id, id) from t1;",
                // dropFunc
                "drop function add2bigint",
                // errMsg.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");
        checkDropFunctionWithInsertMaybe(client,
                // funcCreate
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                // procName
                "UDFInInsertValues",
                // procDDLCreateBody
                "insert into t1 values (add2bigint(100, 101));",
                // dropFunc
                "drop function add2bigint",
                // errMsg.
                "Cannot drop user defined function \"add2bigint\".  The statement %s.%s depends on it.");

        ////////////////////////////////////////////////////////////////////////
        //
        // Check for UDFs in set expressions (UNION, INTERSECTION, EXCEPT)
        //
        // Since these are not that different as java stored procedures and
        // DDL procedures, we only test DDL procedures.
        //
        ////////////////////////////////////////////////////////////////////////
        // Check for UDFs in the left side of a union
        checkDropFunction(client,
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                "create procedure p as " +
                  "select sum(add2bigint(id+1, id+2)) from t1 group by add2bigint(id+1, id+2) " +
                  "union select id from t1",
                "p",
                "drop function add2bigint",
                "Cannot drop user defined function \"add2bigint\".  The statement p.sql0 depends on it.",
                "drop procedure p");

        // Check for UDFs in the left side of an intersection
        checkDropFunction(client,
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                "create procedure p as " +
                  "select sum(add2bigint(id+1, id+2)) from t1 group by add2bigint(id+1, id+2) " +
                  "intersect select id from t1",
                "p",
                "drop function add2bigint",
                "Cannot drop user defined function \"add2bigint\".  The statement p.sql0 depends on it.",
                "drop procedure p");

        // Check for UDFs in the left side of an except
        checkDropFunction(client,
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                "create procedure p as " +
                  "select sum(add2bigint(id+1, id+2)) from t1 group by add2bigint(id+1, id+2) " +
                  "except select id from t1",
                "p",
                "drop function add2bigint",
                "Cannot drop user defined function \"add2bigint\".  The statement p.sql0 depends on it.",
                "drop procedure p");

        // Check for UDFs in the right side of a union
        checkDropFunction(client,
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                "create procedure p as " +
                  "select id from t1 " +
                  "union select sum(add2bigint(id+1, id+2)) from t1 group by add2bigint(id+1, id+2)",
                "p",
                "drop function add2bigint",
                "Cannot drop user defined function \"add2bigint\".  The statement p.sql0 depends on it.",
                "drop procedure p");

        // Check for UDFs in the right side of an intersection
        checkDropFunction(client,
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                "create procedure p as " +
                  "select id from t1 " +
                  "intersect select sum(add2bigint(id+1, id+2)) from t1 group by add2bigint(id+1, id+2)",
                "p",
                "drop function add2bigint",
                "Cannot drop user defined function \"add2bigint\".  The statement p.sql0 depends on it.",
                "drop procedure p");

        // Check for UDFs in the right side of an intersection
        checkDropFunction(client,
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                "create procedure p as " +
                  "select id from t1 " +
                  "except select sum(add2bigint(id+1, id+2)) from t1 group by add2bigint(id+1, id+2)",
                "p",
                "drop function add2bigint",
                "Cannot drop user defined function \"add2bigint\".  The statement p.sql0 depends on it.",
                "drop procedure p");
        ////////////////////////////////////////////////////////////////////////
        //
        // Check for multi-statement DDL procedures.
        //
        ////////////////////////////////////////////////////////////////////////
        checkDropFunction(client,
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                "create procedure p as begin " +
                  "select sum(add2bigint(id+1, id+2)) from t1 group by add2bigint(id+1, id+2); "
                  + "select id from t1; "
                  + "end;",
                "p",
                "drop function add2bigint",
                "Cannot drop user defined function \"add2bigint\".  The statement p.sql0 depends on it.",
                "drop procedure p");

        // This is like the previous, but the UDF is the second statement.
        // So, the error message changes.
        checkDropFunction(client,
                "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;",
                "create procedure p as begin " +
                  "select id from t1; "
                  + "select sum(add2bigint(id+1, id+2)) from t1 group by add2bigint(id+1, id+2); "
                  + "end;",
                "p",
                "drop function add2bigint",
                "Cannot drop user defined function \"add2bigint\".  The statement p.sql1 depends on it.",
                "drop procedure p");
        dropEverything(client);
    }

    public TestAddDropUDF(String name) {
        super(name);
    }

    static final Class<?>[] UDF_CLASSES = {org.voltdb_testfuncs.BasicTestUDFSuite.class};

    static public Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAddDropUDF.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

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
