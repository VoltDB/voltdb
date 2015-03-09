/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

/**
 * Junit tests for UPSERT
 */

public class TestSqlUpsertSuite extends RegressionSuite {

    public void testUpsertProcedure() throws IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable vt = null;

        String[] tables = {"R1", "P1", "R2", "P2"};
        for (String tb : tables) {
            String upsertProc = tb + ".upsert";
            String query = "select ID, wage, dept from " + tb + " order by ID, dept";

            vt = client.callProcedure(upsertProc, 1, 1, 1).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}});

            vt = client.callProcedure(upsertProc, 2, 1, 1).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}, {2, 1, 1}});

            vt = client.callProcedure(upsertProc, 2, 2, 1).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}, {2, 2, 1}});

            vt = client.callProcedure(upsertProc, 1, 1, 1).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}, {2, 2, 1}});

            vt = client.callProcedure(upsertProc, 1, 1, 2).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            if (tb.equals("R1") || tb.equals("P1")) {
                validateTableOfLongs(vt, new long[][] {{1,1,2}, {2, 2, 1}});
            } else {
                // multiple cols primary keys
                validateTableOfLongs(vt, new long[][] {{1,1,1}, {1,1,2}, {2, 2, 1}});
            }
        }
    }

    public void testUpsertAdHoc() throws IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable vt = null;

        String[] tables = {"R1", "P1", "R2", "P2"};
        for (String tb : tables) {
            String query = "select ID, wage, dept from " + tb + " order by ID, dept";

            // Insert here is on purpose for testing the cached AdHoc feature
            vt = client.callProcedure("@AdHoc", String.format(
                    "Insert into %s values(%d, %d, %d)", tb, 1, 1, 1)).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}});

            vt = client.callProcedure("@AdHoc", String.format(
                    "Upsert into %s values(%d, %d, %d)", tb, 2, 1, 1)).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}, {2, 1, 1}});

            vt = client.callProcedure("@AdHoc", String.format(
                    "Upsert into %s values(%d, %d, %d)", tb, 2, 2, 1)).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}, {2, 2, 1}});

            vt = client.callProcedure("@AdHoc", String.format(
                    "Upsert into %s values(%d, %d, %d)", tb, 1, 1, 1)).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}, {2, 2, 1}});

            vt = client.callProcedure("@AdHoc", String.format(
                    "Upsert into %s values(%d, %d, %d)", tb, 1, 1, 2)).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            if (tb.equals("R1") || tb.equals("P1")) {
                validateTableOfLongs(vt, new long[][] {{1,1,2}, {2, 2, 1}});
            } else {
                // multiple cols primary keys
                validateTableOfLongs(vt, new long[][] {{1,1,1}, {1,1,2}, {2, 2, 1}});
            }
        }
    }

    public void testUpsertAdHocComplex() throws IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable vt = null;

        // Test AdHoc UPSERT with default value and random column order values
        String[] tables = {"R1", "P1", "R2", "P2"};
        for (String tb : tables) {
            String query = "select ID, wage, dept from " + tb + " order by ID, dept";

            // Insert here is on purpose for testing the cached AdHoc feature
            vt = client.callProcedure("@AdHoc", String.format(
                    "Insert into %s values(%d, %d, %d)", tb, 1, 1, 1)).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}});

            // test UPSERT with default value
            vt = client.callProcedure("@AdHoc", String.format(
                    "Upsert into %s (id, dept) values (%d, %d)", tb, 2, 1)).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}, {2, 1, 1}});

            // test UPSERT with column name in random order
            vt = client.callProcedure("@AdHoc", String.format(
                    "Upsert into %s (dept, wage, id) values(%d, %d, %d)", tb, 1, 2, 2)).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}, {2, 2, 1}});

            // test UPSERT with default value
            vt = client.callProcedure("@AdHoc", String.format(
                    "Upsert into %s (dept, id) values(%d, %d)", tb, 1, 1)).getResults()[0];
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,1,1}, {2, 2, 1}});
        }

        // Test AdHoc UPSER with SELECT
        vt = client.callProcedure("@AdHoc", String.format(
                "Upsert into R1 (dept, id) SELECT dept, id+1 FROM R2 order by 1, 2")).getResults()[0];
        vt = client.callProcedure("@AdHoc",
                "select ID, wage, dept from R1 order by ID, dept").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{1,1,1}, {2, 1, 1}, {3, 1, 1}});

        // Without the order by in the SELECT clause, the result is content non-deterministic.
        // This is different with INSERT INTO SELECT.
        vt = client.callProcedure("@AdHoc", String.format(
                "Upsert into P1 (dept, id) SELECT id, dept FROM P2 order by 1, 2 ")).getResults()[0];
        vt = client.callProcedure("@AdHoc",
                "select ID, wage, dept from P1 order by ID, dept").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{1, 1, 2}, {2, 2, 1}});

    }

    public void testUpsertWithoutPrimaryKey() throws IOException, ProcCallException {
        Client client = getClient();

        String[] tables = {"UR1", "UP1", "UR2", "UP2", "UR3", "UP3"};
        for (String tb : tables) {
            String upsertProc = tb + ".upsert";
            String errorMsg = "Procedure " + upsertProc + " was not found";
            verifyProcFails(client, errorMsg, upsertProc, 1, 1, 2);

            errorMsg = "Unsupported UPSERT table without primary key: UPSERT";
            verifyStmtFails(client, "Upsert into " + tb + " values(1, 1, 2)", errorMsg);
        }

        String errorMsg = "UPSERT statement manipulates data in a non-deterministic way";
        verifyStmtFails(client, "Upsert into P1 (dept, id) SELECT id, dept FROM P2", errorMsg);
        verifyStmtFails(client, "Upsert into P1 (dept, id) SELECT id, dept FROM P2 order by 2",
                errorMsg);

        // also validate the partition to partition UPSERT
        errorMsg = "Partitioning could not be determined for UPSERT INTO ... SELECT statement.  "
                + "Please ensure that statement does not attempt to copy row data "
                + "from one partition to another, which is unsupported.";
        verifyStmtFails(client, "Upsert into P1 (dept, id) SELECT dept, id  FROM P2 order by 1, 2 ",
                errorMsg);
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestSqlUpsertSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestSqlUpsertSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE R1 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER DEFAULT 1, " +
                "DEPT INTEGER, " +
                "PRIMARY KEY (ID) );" +

                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER DEFAULT 1, " +
                "DEPT INTEGER NOT NULL, " +
                "PRIMARY KEY (ID) );" +
                "PARTITION TABLE P1 ON COLUMN ID;" +

                "CREATE TABLE R2 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER DEFAULT 1, " +
                "DEPT INTEGER NOT NULL, " +
                "PRIMARY KEY (ID, DEPT) );" +

                "CREATE TABLE P2 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER DEFAULT 1, " +
                "DEPT INTEGER NOT NULL, " +
                "PRIMARY KEY (ID, DEPT) );" +
                "PARTITION TABLE P2 ON COLUMN DEPT;" +

                // Unsupported schema
                "CREATE TABLE UR1 ( " +
                "ID INTEGER NOT NULL, " +
                "WAGE INTEGER DEFAULT 1, " +
                "DEPT INTEGER);" +

                "CREATE TABLE UR2 ( " +
                "ID INTEGER NOT NULL UNIQUE, " +
                "WAGE INTEGER DEFAULT 1, " +
                "DEPT INTEGER);" +

                "CREATE TABLE UP1 ( " +
                "ID INTEGER NOT NULL, " +
                "WAGE INTEGER DEFAULT 1, " +
                "DEPT INTEGER);" +
                "PARTITION TABLE UP1 ON COLUMN ID;" +

                "CREATE TABLE UP2 ( " +
                "ID INTEGER NOT NULL UNIQUE, " +
                "WAGE INTEGER DEFAULT 1, " +
                "DEPT INTEGER);" +
                "PARTITION TABLE UP2 ON COLUMN ID;" +

                // Export table
                "CREATE TABLE UR3 ( " +
                "ID INTEGER NOT NULL, " +
                "WAGE INTEGER DEFAULT 1, " +
                "DEPT INTEGER);" +
                "EXPORT TABLE UR3;" +

                "CREATE TABLE UP3 ( " +
                "ID INTEGER NOT NULL, " +
                "WAGE INTEGER DEFAULT 1, " +
                "DEPT INTEGER);" +
                "PARTITION TABLE UP3 ON COLUMN ID;" +
                "EXPORT TABLE UP3;" +

                ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        boolean success;
        config = new LocalCluster("sqlupsert-onesite.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        //*/ Cluster
        config = new LocalCluster("sqlupsert-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
        //*/

        return builder;
    }

}
