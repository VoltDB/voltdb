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
        final long modifiedOneTuple = 1;
        // Test AdHoc UPSERT with default value and random column order values
        String[] tables = {"R1", "P1", "R2", "P2"};
        for (String tb : tables) {
            String query = "select ID, wage, dept from " + tb + " order by ID, dept";

            // Insert here is on purpose for testing the cached AdHoc feature
            validateDMLTupleCount(client,
                    String.format("Insert into %s values(%d, %d, %d)", tb, 1, 1, 1),
                    modifiedOneTuple);
            validateTableOfLongs(client, query, new long[][] {{1,1,1}});

            // test UPSERT with default value
            validateDMLTupleCount(client,
                    String.format("Upsert into %s (id, dept) values (%d, %d)", tb, 2, 1),
                    modifiedOneTuple);
            validateTableOfLongs(client, query, new long[][] {{1,1,1}, {2,1,1}});

            // test UPSERT with column name in random order
            validateDMLTupleCount(client,
                    String.format("Upsert into %s (dept, wage, id) values(%d, %d, %d)", tb, 1, 2, 2),
                    modifiedOneTuple);
            validateTableOfLongs(client, query, new long[][] {{1,1,1}, {2,2,1}});

            // test UPSERT with default value

            // trivial no-op case.
            validateDMLTupleCount(client,
                    String.format("Upsert into %s (dept, id) values(%d, %d)", tb, 1, 1),
                    modifiedOneTuple);
            validateTableOfLongs(client, query, new long[][] {{1,1,1}, {2,2,1}});

            // case that goes differently for single or compound primary keys
            validateDMLTupleCount(client,
                    String.format("Upsert into %s (dept, id) values(%d, %d)", tb, 3, 1),
                    modifiedOneTuple);
            if (tb.contains("2")) {
                validateTableOfLongs(client, query, new long[][] {{1,1,1}, {1,1,3}, {2,2,1}});
                // Delete the original row to compensate for single/compound key differences.
                // Assuming that we continue to use deterministic querying to work around
                // differences in the stored tuple order and partition response order,
                // this restores a consistent baseline for follow-on test cases.
                validateDMLTupleCount(client,
                    String.format("Delete from %s where id = %s and dept = %s", tb, 1, 1),
                    modifiedOneTuple);
            }
            validateTableOfLongs(client, query, new long[][] {{1,1,3}, {2,2,1}});

            // Try that again but with a new row.
            validateDMLTupleCount(client,
                    String.format("Upsert into %s (dept, id) values(%d, %d)", tb, 3, 4),
                    modifiedOneTuple);
            validateTableOfLongs(client, query, new long[][] {{1,1,3}, {2,2,1}, {4,1,3}});
            // negative test UPSERT neglecting to specify a value for a primary key (component)
            try {
                vt = client.callProcedure("@AdHoc",
                        String.format("Upsert into %s (wage) values(%d)", tb, 5)
                        ).getResults()[0];
                fail("Should have thrown a planner exception on upsert with missing primary key.\n" +
                        "Instead, the upsert return value was:\n" +
                        vt.toString() +
                        " and the check query now returns:\n" +
                        client.callProcedure("@AdHoc", query).getResults()[0].toString());
            }
            catch (ProcCallException pce) {
                String msg = pce.toString();
                //* enable to debug */ System.out.println("DEBUG: OK got PCE:" + msg);
                assertTrue(msg.contains("\" must specify a value for primary key \""));
            }
            // test UPSERT with an existing row safely having a required non-defaulted value.
            // The unsafe new row variant of this test is run below specifically on table R2
            // outside this table loop because it's a pain to get all of the positive and
            // negative cases and their side effects right for all the different table schema.
            // Most of them degenerate into uninteresting minor variants of existing positive
            // and negative test cases already covered in this suite.
            // Various modes of failure here were one symptom of ENG-10072.
            // Expect a failure here on tables where DEPT is a required primary key component.
            if (tb.contains("1")) {
                validateDMLTupleCount(client,
                        String.format("Upsert into %s (wage, id) values(%d, %d)", tb, 6, 2),
                        modifiedOneTuple);
            }
            else {
                try {
                    vt = client.callProcedure("@AdHoc",
                            String.format("Upsert into %s (wage, id) values(%d, %d)", tb, 6, 2)
                            ).getResults()[0];
                    fail("Should have thrown a planner exception on upsert with missing primary key.\n" +
                            "Instead, the upsert return value was:\n" +
                            vt.toString() +
                            " and the check query now returns:\n" +
                            client.callProcedure("@AdHoc", query).getResults()[0].toString());
                }
                catch (ProcCallException pce) {
                    String msg = pce.toString();
                    //* enable to debug */ System.out.println("DEBUG: OK got PCE:" + msg);
                    assertTrue(msg.contains("\" must specify a value for primary key \""));
                }
                // Compensate for failure in compound key schemas by providing compund key.
                // Providing the required DEPT id does not exercise the interesting code path.
                // For that, there would need to be another variant of the schema with a
                // non-nullable non-defaulted value OUTSIDE the compound primary key, but that's
                // not a very interesting variant of the single primary key cases in the
                // existing list of test tables.
                // This statement is just to re-establish a consistent baseline data set
                // for later test queries.
                validateDMLTupleCount(client,
                        String.format("Upsert into %s (wage, id, dept) values(%d, %d, %d)", tb, 6, 2, 1),
                        modifiedOneTuple);
            }
            validateTableOfLongs(client, query, new long[][] {{1,1,3}, {2,6,1}, {4,1,3}});
        }

        // negative test UPSERT with non-existing row and not providing a non-defaultable value
        try {
            vt = client.callProcedure("@AdHoc", "Upsert into P1 (wage, id) values(8, 9)").getResults()[0];
            fail("Should have thrown a sql exception on upsert of a new row " +
                 "without a required non-nullable column value.\n" +
                 "Instead, the upsert return value was:\n" +
                 vt.toString() +
                 " and the check query now returns:\n" +
                 client.callProcedure("@AdHoc",
                         "select ID, wage, dept from P1 order by ID, dept").getResults()[0].toString());
        }
        catch (ProcCallException pce) {
            String msg = pce.toString();
            //* enable to debug */ System.out.println("DEBUG: OK got PCE:" + msg);
            assertTrue(msg.contains("CONSTRAINT VIOLATION"));
        }

        // Test AdHoc UPSERT with SELECT
        validateDMLTupleCount(client,
                "Upsert into R1 (dept, id) SELECT dept+10, id+1 FROM R2 order by 1, 2",
                3); // expect 1 update + 2 inserts
        validateTableOfLongs(client, "select ID, wage, dept from R1 order by ID, dept",
                //            original merged    new       original new
                new long[][] {{1,1,3}, {2,6,13}, {3,1,11}, {4,1,3}, {5,1,13}});

        // Note: Without the order by in the SELECT clause, the result is content non-deterministic.
        // This is different from INSERT INTO SELECT.
        // Also: the last two rows from the select have the same ID value,
        // {1,X,2}, {3,X,1}, {3,X,4}
        // so they operate on the same tuple -- inserting and then updating it.
        // BOTH operations get included in the so-called modified tuple count.
        // This is a LITTLE surprising, but it's arguably consistent with
        // counting 1 modified tuple in the case where an UPDATE statement
        // sets columns to their existing values with no detectable effect.
        validateDMLTupleCount(client,
                "Upsert into P1 (dept, id) SELECT id, dept FROM P2 order by 1, 2 ",
                3); // expect 2 updates + 1 insert
        validateTableOfLongs(client, "select ID, wage, dept from P1 order by ID, dept",
                //            merged   original new      original
                new long[][] {{1,1,2}, {2,6,1}, {3,1,4}, {4,1,3}});

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

    public void testUpsertWithSubquery() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable vt = null;

        vt = client.callProcedure("@AdHoc", String.format(
                "Insert into %s values(%d, %d, %d)", "R2", 1, 1, 1)).getResults()[0];
        vt = client.callProcedure("@AdHoc", String.format(
                "Insert into %s values(%d, %d, %d)", "R2", 2, 2, 2)).getResults()[0];
        vt = client.callProcedure("@AdHoc", String.format(
                "Insert into %s values(%d, %d, %d)", "R2", 3, 3, 3)).getResults()[0];

        String[] tables = {"R1"};

        for (String tb : tables) {
            String query = "select ID, wage, dept from " + tb + " order by ID, dept";
            String upsert = "UPSERT INTO " + tb + " (ID, WAGE, DEPT) " +
                    "SELECT ID, WAGE, DEPT FROM R2 WHERE NOT EXISTS (SELECT 1 FROM " + tb +
                    " WHERE " + tb + ".DEPT = R2.DEPT) ORDER BY 1, 2, 3;";

            // This row should stay as is - not in the result set of the UPSERT'S SELECT
            vt = client.callProcedure("@AdHoc", String.format(
                    "Insert into %s values(%d, %d, %d)", tb, 1, 2, 1)).getResults()[0];
            // This row should be updated - in the TB and in the result set of the UPSERT'S SELECT
            vt = client.callProcedure("@AdHoc", String.format(
                    "Insert into %s values(%d, %d, %d)", tb, 3, 3, 1)).getResults()[0];
            // The R2 (2,2,2) should be inserted to TB

            vt = client.callProcedure("@AdHoc", upsert).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{2}});

            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,2,1}, {2,2,2}, {3,3,3}});

            // insert
            upsert = "UPSERT INTO " + tb + " (ID, WAGE, DEPT) " +
                    "VALUES((SELECT MAX(ID) + 5 FROM R2), 0, 0);";
            vt = client.callProcedure("@AdHoc", upsert).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1}});
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,2,1}, {2,2,2}, {3,3,3}, {8, 0, 0} });

            // update
            upsert = "UPSERT INTO " + tb + " (ID, WAGE, DEPT) " +
                    "VALUES((SELECT MAX(ID) + 5 FROM R2), 1, 1);";
            vt = client.callProcedure("@AdHoc", upsert).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1}});
            vt = client.callProcedure("@AdHoc", query).getResults()[0];
            validateTableOfLongs(vt, new long[][] {{1,2,1}, {2,2,2}, {3,3,3}, {8, 1, 1} });

            // Upsert with a non-scalar subquery expression
            String expectedMsg = "More than one row returned by a scalar/row subquery";
            verifyStmtFails(client, "UPSERT INTO " + tb + " (ID, WAGE, DEPT) " +
                    "VALUES((SELECT ID FROM R2), 1, 1)", expectedMsg);
        }

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

                // Stream table
                "CREATE STREAM UR3 ( " +
                "ID INTEGER NOT NULL, " +
                "WAGE INTEGER DEFAULT 1, " +
                "DEPT INTEGER);" +

                "CREATE STREAM UP3 PARTITION ON COLUMN ID ( " +
                "ID INTEGER NOT NULL, " +
                "WAGE INTEGER DEFAULT 1, " +
                "DEPT INTEGER);" +

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
