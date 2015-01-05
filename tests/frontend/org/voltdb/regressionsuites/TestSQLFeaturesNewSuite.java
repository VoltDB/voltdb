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

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.BatchedMultiPartitionTest;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.TruncateTable;

public class TestSQLFeaturesNewSuite extends RegressionSuite {
    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        TruncateTable.class
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestSQLFeaturesNewSuite(String name) {
        super(name);
    }

    private void loadTableForTruncateTest(Client client, String[] procs) throws Exception {
        for (String proc: procs) {
            client.callProcedure(proc, 1,  1,  1.1, "Luke",  "WOBURN");
            client.callProcedure(proc, 2,  2,  2.1, "Leia",  "Bedfor");
            client.callProcedure(proc, 3,  30,  3.1, "Anakin","Concord");
            client.callProcedure(proc, 4,  20,  4.1, "Padme", "Burlington");
            client.callProcedure(proc, 5,  10,  2.1, "Obiwan","Lexington");
            client.callProcedure(proc, 6,  30,  3.1, "Jedi",  "Winchester");
        }
    }

    public void testTruncateTable() throws Exception {
        System.out.println("STARTING TRUNCATE TABLE......");
        Client client = getClient();
        VoltTable vt = null;

        String[] procs = {"RTABLE.insert", "PTABLE.insert"};
        String[] tbs = {"RTABLE", "PTABLE"};
        // Insert data
        loadTableForTruncateTest(client, procs);

        for (String tb: tbs) {
            vt = client.callProcedure("@AdHoc", "select count(*) from " + tb).getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {6});
        }

        if (isHSQL()) {
            return;
        }

        Exception e = null;
        try {
            client.callProcedure("TruncateTable");
        } catch (ProcCallException ex) {
            System.out.println(ex.getMessage());
            e = ex;
            assertTrue(ex.getMessage().contains("CONSTRAINT VIOLATION"));
        } finally {
            assertNotNull(e);
        }
        for (String tb: tbs) {
            vt = client.callProcedure("@AdHoc", "select count(*) from " + tb).getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {6});

            client.callProcedure("@AdHoc", "INSERT INTO "+ tb +" VALUES (7,  30,  1.1, 'Jedi','Winchester');");

            vt = client.callProcedure("@AdHoc", "select count(ID) from " + tb).getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {7});


            vt = client.callProcedure("@AdHoc", "Truncate table " + tb).getResults()[0];

            vt = client.callProcedure("@AdHoc", "select count(*) from " + tb).getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {0});

            client.callProcedure("@AdHoc", "INSERT INTO "+ tb +" VALUES (7,  30,  1.1, 'Jedi','Winchester');");
            vt = client.callProcedure("@AdHoc", "select ID from " + tb).getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {7});

            vt = client.callProcedure("@AdHoc", "Truncate table " + tb).getResults()[0];
        }

        // insert the data back
        loadTableForTruncateTest(client, procs);
        String nestedLoopIndexJoin = "select count(*) from rtable r join ptable p on r.age = p.age";

        // Test nested loop index join
        for (String tb: tbs) {
            vt = client.callProcedure("@AdHoc", "select count(*) from " + tb).getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {6});
        }

        vt = client.callProcedure("@Explain", nestedLoopIndexJoin).getResults()[0];
        System.err.println(vt);
        assertTrue(vt.toString().contains("NESTLOOP INDEX INNER JOIN"));
        assertTrue(vt.toString().contains("inline INDEX SCAN of \"PTABLE\""));
        assertTrue(vt.toString().contains("SEQUENTIAL SCAN of \"RTABLE\""));

        vt = client.callProcedure("@AdHoc",nestedLoopIndexJoin).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {8});

        vt = client.callProcedure("@AdHoc", "Truncate table ptable").getResults()[0];
        vt = client.callProcedure("@AdHoc", "select count(*) from ptable").getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {0});

        vt = client.callProcedure("@AdHoc",nestedLoopIndexJoin).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {0});
    }

    public void testTableLimitAndPercentage() throws Exception {
        System.out.println("STARTING TABLE LIMIT AND PERCENTAGE FULL TEST......");
        Client client = getClient();
        VoltTable vt = null;
        if(isHSQL()) {
            return;
        }

        // When table limit feature is fully supported, there needs to be more test cases.
        // generalize this test within a loop, maybe.
        // Test max row 0
        vt = client.callProcedure("@AdHoc", "select count(*) from CAPPED0").getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {0});

        verifyProcFails(client, "CONSTRAINT VIOLATION\\s*Table CAPPED0 exceeds table maximum row count 0",
                "CAPPED0.insert", 0, 0, 0);

        vt = client.callProcedure("@AdHoc", "select count(*) from CAPPED0").getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {0});

        // Test @Statistics TABLE
        validStatisticsForTableLimitAndPercentage(client, "CAPPED0", 0, 0);

        // Test max row 2
        vt = client.callProcedure("CAPPED2.insert", 0, 0, 0).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        validStatisticsForTableLimitAndPercentage(client, "CAPPED2", 2, 50);
        vt = client.callProcedure("CAPPED2.insert", 1, 1, 1).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        validStatisticsForTableLimitAndPercentage(client, "CAPPED2", 2, 100);

        verifyProcFails(client, "CONSTRAINT VIOLATION\\s*Table CAPPED2 exceeds table maximum row count 2",
                "CAPPED2.insert", 2, 2, 2);

        vt = client.callProcedure("@AdHoc", "select count(*) from CAPPED2").getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {2});

        // Test @Statistics TABLE
        validStatisticsForTableLimitAndPercentage(client, "CAPPED2", 2, 100);

        // Test @Statistics TABLE for normal table
        vt = client.callProcedure("NOCAPPED.insert", 0, 0, 0).getResults()[0];
        // Test @Statistics TABLE
        validStatisticsForTableLimitAndPercentage(client, "NOCAPPED", VoltType.NULL_INTEGER, 0);


        // Test percentage with round up
        vt = client.callProcedure("CAPPED3.insert", 0, 0, 0).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        validStatisticsForTableLimitAndPercentage(client, "CAPPED3", 3, 34);
        vt = client.callProcedure("CAPPED3.insert", 1, 1, 1).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        validStatisticsForTableLimitAndPercentage(client, "CAPPED3", 3, 67);
        vt = client.callProcedure("CAPPED3.insert", 2, 2, 2).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        validStatisticsForTableLimitAndPercentage(client, "CAPPED3", 3, 100);

        verifyProcFails(client, "CONSTRAINT VIOLATION\\s*Table CAPPED3 exceeds table maximum row count 3",
                "CAPPED3.insert", 3, 3, 3);

        // This should also fail if attempting to insert a row via INSERT INTO ... SELECT.
        verifyStmtFails(client, "insert into capped3 select * from capped2",
                "CONSTRAINT VIOLATION\\s*Table CAPPED3 exceeds table maximum row count 3");

        vt = client.callProcedure("@AdHoc", "select count(*) from CAPPED3").getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {3});

    }

    public void testTableLimitPartitionRowsExec() throws IOException, ProcCallException {

        if (isHSQL())
                return;

        Client client = getClient();

        // CAPPED3_LIMIT_ROWS_EXEC is a special table whose name is recognized by the EE.
        // The EE will execute a purge fragment when executing inserts on this table when
        // it's at its 3-row limit:
        //
        //  DELETE FROM capped3_limit_rows_exec WHERE purge_me <> 0

        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 0, 10, 20);
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 0, 20, 40);
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 0, 30, 60);

        // purge fragment executed but deletes no rows... insert still fails.
        verifyProcFails(client,
                        "CONSTRAINT VIOLATION\\s*Table CAPPED3_LIMIT_ROWS_EXEC exceeds table maximum row count 3",
                        "CAPPED3_LIMIT_ROWS_EXEC.insert", 0, 40, 80);

        // If we update the PURGE_ME field, the purge fragment will delete a row on the next insert,
        // allowing it to succeed.
        client.callProcedure("@AdHoc", "UPDATE CAPPED3_LIMIT_ROWS_EXEC SET PURGE_ME = 1 WHERE WAGE = 10");
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 0, 40, 80);

        // Verify the row where WAGE == 10 was deleted.
        String selectAll = "SELECT * FROM CAPPED3_LIMIT_ROWS_EXEC ORDER BY WAGE";
        VoltTable vt = client.callProcedure("@AdHoc", selectAll).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{0, 20, 40}, {0, 30, 60}, {0, 40, 80}});

        // This mark two rows to be purged.
        client.callProcedure("@AdHoc",
                        "UPDATE CAPPED3_LIMIT_ROWS_EXEC SET PURGE_ME = 1 WHERE WAGE IN (20, 40)");
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 0, 50, 100);
        vt = client.callProcedure("@AdHoc", selectAll).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{0, 30, 60}, {0, 50, 100}});

        // Let's top off the table again
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 0, 60, 120);

        // Now mark them all to be purged
        client.callProcedure("@AdHoc", "UPDATE CAPPED3_LIMIT_ROWS_EXEC SET PURGE_ME = 1");

        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 0, 70, 140);
        vt = client.callProcedure("@AdHoc", selectAll).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{0, 70, 140}});

        // Delete remaining row
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.delete", 70);
    }

    public void testTableLimitPartitionRowsExecUnique() throws IOException, ProcCallException {

        if (isHSQL())
                return;

        Client client = getClient();

        // insert into table when it's full, but the
        // - row to be inserted would violate a uniqueness constraint on the table.
        //   The insert should fail, and the delete should be rolled back.
        // - row to be inserted would violate a uniqueness constraint, but the
        //   duplicate row will be purged.  In our current implementation,
        //   this will succeed.

        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 0, 10, 20);
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 1, 20, 40);
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 1, 30, 60);

        verifyProcFails(client,
                        "Constraint Type UNIQUE, Table CatalogId CAPPED3_LIMIT_ROWS_EXEC",
                        "CAPPED3_LIMIT_ROWS_EXEC.insert", 0, 10, 20);

        // Should still be three rows
        String selectAll = "SELECT * FROM CAPPED3_LIMIT_ROWS_EXEC ORDER BY WAGE";
        VoltTable vt = client.callProcedure("@AdHoc", selectAll).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{0, 10, 20}, {1, 20, 40}, {1, 30, 60}});

        // Now try to insert a row with PK value same as an existing row that will be purged.
        // Insert will succeed in this case.
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 0, 20, 99);
        vt = client.callProcedure("@AdHoc", selectAll).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{0, 10, 20}, {0, 20, 99}});

        client.callProcedure("@AdHoc", "DELETE FROM CAPPED3_LIMIT_ROWS_EXEC");
    }

    public void testTableLimitPartitionRowsExecMultiRow() throws IOException, ProcCallException {

        if (isHSQL())
                return;

        Client client = getClient();

        // For multi-row insert, the insert trigger should not fire.
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 1, 10, 20);
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 1, 20, 40);
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 1, 30, 60);

        verifyStmtFails(client,
                        "INSERT INTO CAPPED3_LIMIT_ROWS_EXEC "
                        + "SELECT purge_me, wage + 1, dept from CAPPED3_LIMIT_ROWS_EXEC WHERE WAGE = 20",
                        "exceeds table maximum row count 3");

        String selectAll = "SELECT * FROM CAPPED3_LIMIT_ROWS_EXEC ORDER BY WAGE";
        VoltTable vt = client.callProcedure("@AdHoc", selectAll).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{1, 10, 20}, {1, 20, 40}, {1, 30, 60}});

        // Upsert fails too.
        verifyStmtFails(client,
                        "UPSERT INTO CAPPED3_LIMIT_ROWS_EXEC "
                        + "SELECT purge_me, wage + 1, dept from CAPPED3_LIMIT_ROWS_EXEC WHERE WAGE = 20 "
                        + "ORDER BY 1, 2, 3",
                        "exceeds table maximum row count 3");
        vt = client.callProcedure("@AdHoc", selectAll).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{1, 10, 20}, {1, 20, 40}, {1, 30, 60}});
    }

    public void testTableLimitPartitionRowsExecUpsert() throws Exception {

        if (isHSQL())
            return;

        Client client = getClient();

        // For multi-row insert, the insert trigger should not fire.
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 1, 10, 20);
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 1, 20, 40);
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 1, 30, 60);

        // Upsert (update) should succeed, no delete action.
        client.callProcedure("@AdHoc", "UPSERT INTO CAPPED3_LIMIT_ROWS_EXEC VALUES(1, 30, 61)");
        String selectAll = "SELECT * FROM CAPPED3_LIMIT_ROWS_EXEC ORDER BY WAGE";
        VoltTable vt = client.callProcedure("@AdHoc", selectAll).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{1, 10, 20}, {1, 20, 40}, {1, 30, 61}});

        // Upsert (insert) should succeed, and delete action executions.
        client.callProcedure("@AdHoc", "UPSERT INTO CAPPED3_LIMIT_ROWS_EXEC VALUES(1, 40, 80)");
        vt = client.callProcedure("@AdHoc", selectAll).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{1, 40, 80}});
    }

    public void testTableLimitPartitionRowsComplex() throws Exception {

        if (isHSQL())
            return;

        // CREATE TABLE capped3_limit_exec_complex (
        //   wage INTEGER NOT NULL,
        //   dept INTEGER NOT NULL PRIMARY KEY,
        //   may_be_purged TINYINT DEFAULT 0 NOT NULL,
        //   relevance VARCHAR(255),
        //   priority SMALLINT,
        //   CONSTRAINT tblimit3_exec_complex LIMIT PARTITION ROWS 3
        //     EXECUTE (DELETE FROM capped3_limit_exec_complex
        //              WHERE may_be_purged = 0
        //              AND relevance IN ('irrelevant', 'worthless', 'moot')
        //              AND priority < 16384)
        //   );

        Client client = getClient();

        VoltTable vt;

        // Load the table
        vt = client.callProcedure("CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 1, 0, "important", 17000).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        vt = client.callProcedure("CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 2, 0, "important", 17000).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        vt = client.callProcedure("CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 3, 0, "important", 17000).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        // no rows match purge criteria
        verifyProcFails(client,
                "exceeds table maximum row count 3",
                "CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 4, 0, "important", 17000);

        // Make sure that all three predicates in the DELETE's WHERE clause must be true
        // for rows to be purged.

        vt = client.callProcedure("@AdHoc", "UPDATE CAPPED3_LIMIT_EXEC_COMPLEX SET relevance='moot' WHERE dept = 2").getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        // Insert still fails!
        verifyProcFails(client,
                "exceeds table maximum row count 3",
                "CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 4, 0, "important", 17000);

        vt = client.callProcedure("@AdHoc", "UPDATE CAPPED3_LIMIT_EXEC_COMPLEX SET priority=100 WHERE dept = 2").getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        // Insert still fails!
        verifyProcFails(client,
                "exceeds table maximum row count 3",
                "CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 4, 0, "important", 17000);

        vt = client.callProcedure("@AdHoc", "UPDATE CAPPED3_LIMIT_EXEC_COMPLEX SET may_be_purged=1 WHERE dept = 2").getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        // now the insert succeeds!
        vt = client.callProcedure("CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 4, 1, "moot", 500).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        vt = client.callProcedure("@AdHoc", "SELECT dept FROM CAPPED3_LIMIT_EXEC_COMPLEX ORDER BY dept").getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1, 3, 4});

        // Insert a bunch of purge-able rows in loop
        for (int i = 5; i < 100; ++i) {
            vt = client.callProcedure("CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, i, 1, "irrelevant", i + 10).getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {1});
        }
    }

    public void testAlterTableLimitPartitionRows() throws IOException, ProcCallException {
        if (isHSQL())
            return;

        Client client = getClient();

        VoltTable vt;

        // Load the table
        vt = client.callProcedure("CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 1, 0, "important", 17000).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        vt = client.callProcedure("CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 2, 0, "important", 17000).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        vt = client.callProcedure("CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 3, 0, "important", 17000).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        // no rows match purge criteria
        verifyProcFails(client,
                "exceeds table maximum row count 3",
                "CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 4, 0, "important", 17000);

        ClientResponse cr = client.callProcedure("@AdHoc",
                "ALTER TABLE CAPPED3_LIMIT_EXEC_COMPLEX "
                + "ADD LIMIT PARTITION ROWS 3 "
                + "EXECUTE (DELETE FROM CAPPED3_LIMIT_EXEC_COMPLEX WHERE WAGE = 37)");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // Now the insert should succeed because rows are purge-able.
        vt = client.callProcedure("CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 4, 0, "important", 17000).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        vt = client.callProcedure("CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 5, 0, "important", 17000).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        vt = client.callProcedure("CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 6, 0, "important", 17000).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        // alter the constraint by removing the delete statement
        cr = client.callProcedure("@AdHoc",
                "ALTER TABLE CAPPED3_LIMIT_EXEC_COMPLEX "
                + "ADD LIMIT PARTITION ROWS 3");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("@AdHoc", "select count(*) from capped3_limit_exec_complex").getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {3});

        verifyProcFails(client,
                "exceeds table maximum row count 3",
                "CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 7, 0, "important", 17000);

        // Now remove the constraint altogether
        cr = client.callProcedure("@AdHoc",
                "ALTER TABLE CAPPED3_LIMIT_EXEC_COMPLEX "
                + "DROP LIMIT PARTITION ROWS");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // no more constraint means insert can now succeed.
        vt = client.callProcedure("CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 7, 0, "important", 17000).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        // Verify that we can add the constraint back again
        cr = client.callProcedure("@AdHoc",
                "ALTER TABLE CAPPED3_LIMIT_EXEC_COMPLEX "
                + "ADD LIMIT PARTITION ROWS 3 "
                + "EXECUTE (DELETE FROM CAPPED3_LIMIT_EXEC_COMPLEX WHERE DEPT IN (4, 7))");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("CAPPED3_LIMIT_EXEC_COMPLEX.insert", 37, 8, 0, "important", 17000).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        vt = client.callProcedure("@AdHoc", "select dept from capped3_limit_exec_complex order by dept asc").getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {5, 6, 8});
    }

    // DELETE .. LIMIT <n> is intended to support the row limit trigger
    // so let's test it here.
    public void testLimitPartitionRowsDeleteWithLimit() throws Exception {
        if (isHSQL())
            return;

        Client client = getClient();

        // The table EVENTS is capped at 5 rows.  Inserts that
        // would cause the constraint to fail trigger a delete of
        // the oldest row.

        VoltTable vt;
        for (int i = 0; i < 50; ++i) {
            vt = client.callProcedure("@AdHoc",
                    "INSERT INTO events_capped VALUES (NOW, " + i + ")")
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {1});
        }

        // Should have the most recent 5 rows.
        vt = client.callProcedure("@AdHoc",
                "select info from events_capped order by when_occurred asc")
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {45, 46, 47, 48, 49});
    }

    /**
     * Build a list of the tests that will be run when TestTPCCSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        LocalCluster config = null;

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSQLFeaturesNewSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(BatchedMultiPartitionTest.class.getResource("sqlfeatures-new-ddl.sql"));
        project.addProcedures(PROCEDURES);
        project.setUseDDLSchema(true);

        boolean success;

        //* <-- Change this comment to 'block style' to toggle over to just the one single-server IPC DEBUG config.
        // IF (! DEBUG config) ...

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalCluster("sqlfeatures-new-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        config.setMaxHeap(3300);

        // build the jarfile
        success = config.compile(project);
        assert(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("sqlfeatures-new-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        config.setMaxHeap(3300);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: Local Cluster (of processes)
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("sqlfeatures-new-cluster-rejoin.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        config.setMaxHeap(3800);
        // Commented out until ENG-3076, ENG-3434 are resolved.
        //config = new LocalCluster("sqlfeatures-cluster-rejoin.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI,
        //                          LocalCluster.FailureState.ONE_FAILURE, false);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        /*/ // ... ELSE (DEBUG config) ... [ FRAGILE! This is a structured comment. Do not break it. ]

        /////////////////////////////////////////////////////////////
        // CONFIG #0: DEBUG Local Site/Partition running on IPC backend
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("sqlfeatures-new-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_IPC);
        // build the jarfile
        success = config.compile(project);
        assert(success);
        // add this config to the set of tests to run
        builder.addServerConfig(config);

        // ... ENDIF (DEBUG config) [ FRAGILE! This is a structured comment. Do not break it. ] */

        return builder;
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestSQLFeaturesNewSuite.class);
    }
}
