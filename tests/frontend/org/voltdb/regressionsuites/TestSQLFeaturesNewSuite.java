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
import java.util.UUID;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
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


    private void checkMultiPartitionCappedTableContents(Client client, String tableName, long partitionRowLimit)
            throws NoConnectionsException, IOException, ProcCallException {
        long numPartitions = getLogicalPartitionCount();
        if (numPartitions > 1) {
            VoltTable vt = null;
            // For multi-partition tables, it's possible that just one partition
            // got all the rows, or that rows were evenly distributed (all partitions full).
            final long minRows = partitionRowLimit;
            final long maxRows = partitionRowLimit * numPartitions;
            vt = client.callProcedure("@AdHoc",
                    "select count(*) from " + tableName)
                    .getResults()[0];
            long numRows = vt.asScalarLong();
            assertTrue ("Too many rows in target table: ", numRows <= maxRows);
            assertTrue ("Too few rows in target table: ", numRows >= minRows);

            // Get all rows in descending order
            vt = client.callProcedure("@AdHoc",
                    "SELECT info FROM " + tableName + " "
                            + "ORDER BY when_occurred desc, info desc "
                            + "LIMIT 50 OFFSET 5")
                            .getResults()[0];
            long prevValue = 50;
            while (vt.advanceRow()) {
                long curValue = vt.getLong(0);

                // row numbers may not be adjacent, depending on how UUID hashed,
                // but there should be no duplicates
                assertTrue(curValue < prevValue);
                prevValue = curValue;

                // not sure what else we could assert here?
            }
        }
    }

    // DELETE .. LIMIT <n> is intended to support the row limit trigger
    // so let's test it here.
    public void testLimitPartitionRowsDeleteWithLimit() throws Exception {
        if (isHSQL())
            return;

        final long partitionRowLimit = 5; // from DDL

        Client client = getClient();

        // The following test runs twice and does a truncate table
        // in between, to ensure that the trigger will still work.
        for (int j = 0; j < 2; ++j) {

            // The table EVENTS is capped at 5 rows/partition.  Inserts that
            // would cause the constraint to fail trigger a delete of
            // the oldest row.
            VoltTable vt;
            for (int i = 0; i < 50; ++i) {
                String uuid = UUID.randomUUID().toString();
                vt = client.callProcedure("@AdHoc",
                        "INSERT INTO events_capped VALUES ('" + uuid + "', NOW, " + i + ")")
                        .getResults()[0];

                // Note: this should be *one*, even if insert triggered a delete
                validateTableOfScalarLongs(vt, new long[] {1});

                // ensure that the events are inserted have a unique timestamp so we
                // can sort by it.
                client.drain();
                Thread.sleep(1);
            }

            // Check the contents
            checkMultiPartitionCappedTableContents(client, "events_capped", partitionRowLimit);

            // Should have all of the most recent 5 rows, regardless of how the table is partitioned.
            vt = client.callProcedure("@AdHoc",
                    "select info from events_capped order by when_occurred desc, info desc limit 5")
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {49, 48, 47, 46, 45});

            if (j == 0) {
                // Do a truncate table and run the test again,
                // to ensure that the delete trigger still works.
                client.callProcedure("@AdHoc", "delete from events_capped");
            }
        }
    }

    // Make sure that DELETE ... OFFSET <n> works in partition limit rows context
    public void testLimitPartitionRowsDeleteWithOffset() throws Exception {
        if (isHSQL())
            return;

        final long partitionRowLimit = 5; // from DDL
        final long numPartitions = getLogicalPartitionCount();

        Client client = getClient();

        // The table EVENTS_CAPPED_OFFSET is capped at 5 rows/partition.  Inserts that
        // would cause the constraint to fail trigger a delete of
        // all rows except the newest.
        //
        // The DELETE statement looks like this:
        //   DELETE FROM events_capped_offset
        //   ORDER BY when_occurred DESC, event_id ASC offset 1
        VoltTable vt;
        for (int i = 0; i < 50; ++i) {

            boolean deleteMustBeTriggered = false;
            if (numPartitions == 1) {
                long currNumRows = client.callProcedure("@AdHoc",
                        "select count(*) from events_capped_offset")
                        .getResults()[0].asScalarLong();
                deleteMustBeTriggered = (currNumRows == partitionRowLimit);
            }

            String uuid = UUID.randomUUID().toString();
            vt = client.callProcedure("@AdHoc",
                    "INSERT INTO events_capped_offset VALUES ('" + uuid + "', NOW, " + i + ")")
                    .getResults()[0];

            // Note: this should be *one*, even if insert triggered a delete
            validateTableOfScalarLongs(vt, new long[] {1});

            // ensure that the events are inserted have a unique timestamp so we
            // can sort by it.
            client.drain();
            Thread.sleep(1);

            if (deleteMustBeTriggered) {
                // The last insert just triggered a delete.
                // We should have only the last 2 rows
                validateTableOfScalarLongs(client,
                        "select info from events_capped_offset order by info",
                        new long[] {i - 1, i});
            }
        }

        // Check the contents
        checkMultiPartitionCappedTableContents(client, "events_capped_offset", partitionRowLimit);
    }

    public void testLimitRowsWithTruncatingTrigger() throws IOException, ProcCallException {
        if (isHSQL())
            return;

        Client client = getClient();

        // The table capped_truncate is capped at 5 rows.
        // The LIMIT ROWS trigger for this table just does a
        //   DELETE FROM capped_truncate
        // This is a tricky case since this truncates the table.

        // Insert enough rows to cause the trigger to fire a few times.
        for (int i = 0; i < 13; ++i) {
            VoltTable vt = client.callProcedure("CAPPED_TRUNCATE.insert", i)
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {1});
        }

        // Verify that we only have the last 13 % 5 rows.
        VoltTable vt = client.callProcedure("@AdHoc",
                "SELECT i FROM capped_truncate ORDER BY i")
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {10, 11, 12});
    }

    /* Test to make sure that row limit triggers still execute even when data
     * is being inserted via INSERT INTO ... SELECT */
    public void testLimitRowsWithInsertIntoSelect() throws Exception {
        if (isHSQL())
            return;

        // CREATE TABLE capped3_limit_rows_exec (
        //   purge_me TINYINT DEFAULT 0 NOT NULL,
        //   wage INTEGER NOT NULL PRIMARY KEY,
        //   dept INTEGER NOT NULL,
        //   CONSTRAINT tblimit3_exec LIMIT PARTITION ROWS 3
        //     EXECUTE (DELETE FROM capped3_limit_rows_exec WHERE purge_me <> 0)
        //   );

        Client client = getClient();

        // Populate a source table
        for (int i = 0; i < 11; ++i) {
            VoltTable vt = client.callProcedure("NOCAPPED.insert", i, i*10, i*10)
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {1});
        }

        // Insert into select into a capped table with a trigger
        // NOTE: if target table has a cap, then the SELECT output must be
        // strictly ordered!  Otherwise the effect is not deterministic.
        String stmt = "INSERT INTO capped3_limit_rows_exec "
                + "SELECT 1, wage, dept FROM nocapped";
        verifyStmtFails(client, stmt,
                "Since the table being inserted into has a row limit "
                + "trigger, the SELECT output must be ordered");

        // Add an order by clause to order the select
        stmt += " ORDER BY id, wage, dept";
        VoltTable vt = client.callProcedure("@AdHoc", stmt)
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {11});

        // We should only have the last 11 % 3 rows inserted.
        validateTableOfLongs(client,
                "SELECT purge_me, wage, dept from CAPPED3_LIMIT_ROWS_EXEC ORDER BY wage",
                new long[][] {
                {1, 90, 90},
                {1, 100, 100}
        });

        validateTableOfScalarLongs(client, "delete from capped3_limit_rows_exec", new long[] {2});

        // Try again but this time no rows will be purge-able.
        stmt = "INSERT INTO capped3_limit_rows_exec "
                + "SELECT 0, wage, dept FROM nocapped "
                + "order by id, wage, dept";
        verifyStmtFails(client, stmt,
                "Table CAPPED3_LIMIT_ROWS_EXEC exceeds table maximum row count 3");

        // The failure should have happened at the 4th row, but since we
        // have atomicity, the table should be empty.
        validateTableOfLongs(client, "select purge_me, wage, dept from capped3_limit_rows_exec",
                new long[][] {});
    }

    /* Test to make sure that row limit triggers still execute even when data
     * is being inserted via UPSERT INTO ... SELECT */
    public void testLimitRowsWithUpsertIntoSelect() throws Exception {
        if (isHSQL())
            return;

        Client client = getClient();

        // Populate a source table
        for (int i = 0; i < 11; ++i) {
            VoltTable vt = client.callProcedure("NOCAPPED.insert", i, i*10, i*10)
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {1});
        }

        // the SELECT statement in UPSERT INTO ... SELECT
        // must always be ordered.
        String stmt = "UPSERT INTO capped3_limit_rows_exec "
                + "SELECT 1, wage, dept FROM nocapped";
        verifyStmtFails(client, stmt,
                "UPSERT statement manipulates data in a non-deterministic way");

        // Add an order by clause to order the select, statement can
        // now execute.
        //
        // There are no rows in the table so this just inserts the rows.
        stmt += " ORDER BY id, wage, dept";
        VoltTable vt = client.callProcedure("@AdHoc", stmt)
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {11});

        validateTableOfLongs(client,
                "SELECT purge_me, wage, dept from CAPPED3_LIMIT_ROWS_EXEC ORDER BY wage",
                new long[][] {
                {1, 90, 90},
                {1, 100, 100}
        });

        // Create some rows in the source table that where the wage field
        // overlaps with the target capped table.
        //
        validateTableOfScalarLongs(client,
                "update nocapped set wage = wage + 50, dept = (wage + 50) * 10 + 1 ",
                new long[] {11});

        // Source table now has rows 50..150
        // This overlaps where wage is 90 and 100.
        validateTableOfScalarLongs(client, "select wage from nocapped order by wage",
                new long[] {50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150});

        // Make it so existing rows in capped table will not be purged.
        validateTableOfScalarLongs(client,
                "update capped3_limit_rows_exec set purge_me = 0",
                new long[] {2});

        // Upsert into the capped table where the last
        // two rows from the select are updates.
        validateTableOfScalarLongs(client,
                "upsert into capped3_limit_rows_exec "
                + "select 1, wage, dept from nocapped "
                + "where wage < 110 "
                + "order by id, wage, dept",
                new long[] {6});

        // Note the contents of the table:
        // if rows 90 and 100 had been purged at the second insert,
        // then the contents would be just 90 and 100.
        validateTableOfLongs(client,
                "select purge_me, wage, dept "
                + "from capped3_limit_rows_exec "
                + "order by wage",
                new long[][] {
                {1, 80, 801},
                {1, 90, 901},
                {1, 100, 1001}
                });

        // Leave rows 80 and 90
        validateTableOfLongs(client,
                "delete from capped3_limit_rows_exec where wage = 100",
                new long[][] {{1}});

        // Make the remaining rows un-purge-able
        validateTableOfLongs(client,
                "update capped3_limit_rows_exec set purge_me = 0",
                new long[][] {{2}});

        // upsert into the capped table
        // two rows will be updated, and not purged,
        // The rest of the rows will not be purged
        //
        // The case when is there to make sure the updated
        // rows do no become purge-able after being updated.
        //
        // Set the dept field to -32 as proof that we updated the row.
        validateTableOfScalarLongs(client,
                "upsert into capped3_limit_rows_exec "
                + "select case when wage in (80, 90) then 0 else 1 end, wage, -32 from nocapped "
                + "where wage >= 80 "
                + "order by id, wage, dept",
                new long[] {8});

        validateTableOfLongs(client,
                "select purge_me, wage, dept "
                + "from capped3_limit_rows_exec "
                + "order by wage",
                new long[][] {
                {0, 80, -32},
                {0, 90, -32},
                {1, 150, -32}
                });
    }

    /*
     * Some tricky self-insert cases.  Doing this on a capped table
     * is weird, especially the UPSERT case.
     */
    public void testTableLimitPartitionRowsExecMultiRowSelfInsert() throws Exception {

        if (isHSQL())
                return;

        Client client = getClient();

        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 1, 10, 20);
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 1, 20, 40);
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 1, 30, 60);

        // Fails determinism check
        String stmt = "INSERT INTO CAPPED3_LIMIT_ROWS_EXEC "
                + "SELECT purge_me, wage + 1, dept from CAPPED3_LIMIT_ROWS_EXEC WHERE WAGE = 20";
        verifyStmtFails(client, stmt,
                        "Since the table being inserted into has a row limit "
                        + "trigger, the SELECT output must be ordered.");

        // It passes when we add an ORDER BY clause
        stmt += " ORDER BY purge_me, wage, dept";
        validateTableOfScalarLongs(client, stmt,
                new long[] {1});

        // Table was at its limit, and we inserted one row, which flushed out
        // the existing rows.
        String selectAll = "SELECT * FROM CAPPED3_LIMIT_ROWS_EXEC ORDER BY WAGE";
        VoltTable vt = client.callProcedure("@AdHoc", selectAll).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{1, 21, 40}});

        // Now let's try to do an upsert where the outcome relies both
        // on doing an update, doing an insert and also triggering a delete.

        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 1, 41, 81);
        client.callProcedure("CAPPED3_LIMIT_ROWS_EXEC.insert", 1, 61, 121);
        // Table now contains rows 21, 41, 61.

        // Upsert with select producing
        //   0, 21, 42  -- update, and make the row un-purge-able
        //   1, 42, 82  -- insert, will delete all rows except the first
        //   1, 62, 122 -- insert
        validateTableOfScalarLongs(client,
                        "UPSERT INTO CAPPED3_LIMIT_ROWS_EXEC "
                        + "SELECT "
                        + "  case when wage = 21 then 0 else 1 end, "
                        + "  case wage when 21 then wage else wage + 1 end, "
                        + "  wage * 2 "
                        + "from CAPPED3_LIMIT_ROWS_EXEC "
                        + "ORDER BY 1, 2, 3",
                        new long[] {3});

        vt = client.callProcedure("@AdHoc", selectAll).getResults()[0];
        validateTableOfLongs(vt, new long[][] {
                {0, 21, 42},
                {1, 42, 82},
                {1, 62, 122}
        });
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
