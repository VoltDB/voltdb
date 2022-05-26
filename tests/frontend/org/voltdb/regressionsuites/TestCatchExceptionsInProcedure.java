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
import org.voltdb.ProcedurePartitionData;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

/*
 * Functional tests of the statements compiled in the test suite
 * org.voltdb.planner.TestComplexGroupBySuite.
 */

public class TestCatchExceptionsInProcedure extends RegressionSuite {
    private boolean isTrue(int value) {
        return value == 0 ? false: true;
    }

    private void mpChecker(Client client,
            int hasPreviousBatch, int tryCatchContains1Batch,
            int hasFollowingBatch)
        throws NoConnectionsException, IOException, ProcCallException {
        VoltTable vt;
        String sql;
        final String MPErrorMessage = "attempted to execute new batch "
                + "after hitting EE exception in a previous batch";

        String[] procs = {"MPInsertOnReplicatedTable", "MPInsertOnPartitionTable"};
        String[] tables = {"R1", "P1"};
        int[] followingBatchHasExceptions = {0, 1};
        for (int i = 0; i < procs.length; i++) {
            String proc = procs[i];
            String tb = tables[i];
            for (int followingBatchHasException: followingBatchHasExceptions) {
                try {
                    vt = client.callProcedure(proc,
                            hasPreviousBatch, tryCatchContains1Batch,
                            hasFollowingBatch, followingBatchHasException).getResults()[0];
                    // validate returned value from the procedure calls
                    validateRowOfLongs(vt, new long[]{-1});
                } catch(Exception e) {
                    assertTrue(e.getMessage().contains(MPErrorMessage));
                }

                sql = "select ratio from " + tb + " order by 1;";
                // empty table
                validateTableColumnOfScalarFloat(client, sql, new double[]{});

                // empty table
                sql = "select count(*) from " + tb;
                validateTableOfScalarLongs(client, sql, new long[]{0});

                client.callProcedure("@AdHoc", "truncate table " + tb);
            }
        }
    }

    public void testMPCatchException() throws IOException, ProcCallException {
        System.out.println("test testMPCatchException...");
        Client client = getClient();

        mpChecker(client, 0, 0, 0);
        mpChecker(client, 1, 0, 0);
        mpChecker(client, 0, 1, 0);
        mpChecker(client, 0, 0, 1);
        mpChecker(client, 1, 1, 0);
        mpChecker(client, 1, 0, 1);
        mpChecker(client, 0, 1, 1);
        mpChecker(client, 1, 1, 1);
    }

    private void spChecker(Client client, String proc,
            int hasPreviousBatch,
            int tryCatchContains1BatchFirst,
            int tryCatchContains1BatchSecond,
            int hasFollowingBatch, int followingBatchHasException,
            double[] expected)
        throws NoConnectionsException, IOException, ProcCallException {
        VoltTable vt;
        String sql;
        try {
            // use the default value for partition column to route this procedure
            if (tryCatchContains1BatchSecond == -1) {
                vt = client.callProcedure(proc, 0,
                        hasPreviousBatch, tryCatchContains1BatchFirst,
                        hasFollowingBatch, followingBatchHasException).getResults()[0];
            } else {
                vt = client.callProcedure(proc, 0,
                        hasPreviousBatch, tryCatchContains1BatchFirst, tryCatchContains1BatchSecond,
                        hasFollowingBatch, followingBatchHasException).getResults()[0];
            }
            if (isTrue(followingBatchHasException)) {
                assertTrue(isTrue(hasFollowingBatch));
                fail("Expected failure but succeeded.");
            }
            // validate returned value from the procedure calls
            validateRowOfLongs(vt, new long[]{-1});
        } catch(Exception e) {
            assertTrue(e.getMessage().contains("CONSTRAINT VIOLATION"));
            assertTrue(e.getMessage().contains("3.2")); // violated at row (3, 3.2)
            assertTrue(isTrue(hasFollowingBatch) && isTrue(followingBatchHasException));
        }

        sql = "select ratio from P1 order by 1; ";
        validateTableColumnOfScalarFloat(client, sql, expected);

        client.callProcedure("@AdHoc", "truncate table P1");
    }

    private void spSingleTryCatchChecker(Client client,
            int hasPreviousBatch, int tryCatchContains1Batch,
            int hasFollowingBatch, int followingBatchHasException,
            double[] expected)
        throws NoConnectionsException, IOException, ProcCallException {

        // call the main checker
        spChecker(client, "SPInsertOnPartitionTable",
                hasPreviousBatch, tryCatchContains1Batch, -1,
                hasFollowingBatch, followingBatchHasException, expected);
    }

    public static double prev = 0.1;
    public void testSPCatchException() throws IOException, ProcCallException {
        System.out.println("test testSPCatchException...");
        Client client = getClient();

        // no previous batch
        spSingleTryCatchChecker(client, 0, 0, 0, 0, new double[]{1.1});
        spSingleTryCatchChecker(client, 0, 0, 1, 0, new double[]{1.1, 3.1});
        spSingleTryCatchChecker(client, 0, 0, 1, 1, new double[]{});
        spSingleTryCatchChecker(client, 0, 1, 0, 0, new double[]{});
        spSingleTryCatchChecker(client, 0, 1, 1, 0, new double[]{3.1});
        spSingleTryCatchChecker(client, 0, 1, 1, 1, new double[]{});

        // has previous batch
        spSingleTryCatchChecker(client, 1, 0, 0, 0, new double[]{prev, 1.1});
        spSingleTryCatchChecker(client, 1, 0, 1, 0, new double[]{prev, 1.1, 3.1});
        spSingleTryCatchChecker(client, 1, 0, 1, 1, new double[]{});
        spSingleTryCatchChecker(client, 1, 1, 0, 0, new double[]{prev});
        spSingleTryCatchChecker(client, 1, 1, 1, 0, new double[]{prev, 3.1});
        spSingleTryCatchChecker(client, 1, 1, 1, 1, new double[]{});
    }

    private void spMultiTryCatchChecker(Client client,
            int hasPreviousBatch,
            int tryCatchContains1BatchFirst,int tryCatchContains1BatchSecond,
            int hasFollowingBatch, int followingBatchHasException,
            double[] expected)
        throws NoConnectionsException, IOException, ProcCallException {
        // call the main checker

        spChecker(client, "SPMultipleTryCatchOnPartitionTable",
                hasPreviousBatch, tryCatchContains1BatchFirst, tryCatchContains1BatchSecond,
                hasFollowingBatch, followingBatchHasException, expected);
    }

    public void testSPMultiCatchException() throws IOException, ProcCallException {
        System.out.println("test testSPMultiCatchException...");
        Client client = getClient();

        //
        // no previous batch
        //
        spMultiTryCatchChecker(client, 0, 0, 0, 0, 0, new double[]{1.1, 2.1});
        spMultiTryCatchChecker(client, 0, 1, 1, 1, 1, new double[]{});

        spMultiTryCatchChecker(client, 0, 1, 0, 0, 0, new double[]{2.1});
        spMultiTryCatchChecker(client, 0, 0, 1, 0, 0, new double[]{1.1});
        spMultiTryCatchChecker(client, 0, 0, 0, 1, 0, new double[]{1.1, 2.1, 3.1});

        spMultiTryCatchChecker(client, 0, 1, 1, 0, 0, new double[]{});
        spMultiTryCatchChecker(client, 0, 1, 0, 1, 0, new double[]{2.1, 3.1});
        spMultiTryCatchChecker(client, 0, 0, 1, 1, 0, new double[]{1.1, 3.1});
        spMultiTryCatchChecker(client, 0, 0, 0, 1, 1, new double[]{});

        spMultiTryCatchChecker(client, 0, 1, 1, 1, 0, new double[]{3.1});
        spMultiTryCatchChecker(client, 0, 1, 0, 1, 1, new double[]{});
        spMultiTryCatchChecker(client, 0, 0, 1, 1, 1, new double[]{});


        // has previous batch
        spMultiTryCatchChecker(client, 1, 0, 0, 0, 0, new double[]{prev, 1.1, 2.1});
        spMultiTryCatchChecker(client, 1, 1, 1, 1, 1, new double[]{});

        spMultiTryCatchChecker(client, 1, 1, 0, 0, 0, new double[]{prev, 2.1});
        spMultiTryCatchChecker(client, 1, 0, 1, 0, 0, new double[]{prev, 1.1});
        spMultiTryCatchChecker(client, 1, 0, 0, 1, 0, new double[]{prev, 1.1, 2.1, 3.1});

        spMultiTryCatchChecker(client, 1, 1, 1, 0, 0, new double[]{prev});
        spMultiTryCatchChecker(client, 1, 1, 0, 1, 0, new double[]{prev, 2.1, 3.1});
        spMultiTryCatchChecker(client, 1, 0, 1, 1, 0, new double[]{prev, 1.1, 3.1});
        spMultiTryCatchChecker(client, 1, 0, 0, 1, 1, new double[]{});

        spMultiTryCatchChecker(client, 1, 1, 1, 1, 0, new double[]{prev, 3.1});
        spMultiTryCatchChecker(client, 1, 1, 0, 1, 1, new double[]{});
        spMultiTryCatchChecker(client, 1, 0, 1, 1, 1, new double[]{});
    }

    private void bigBatchChecker(Client client, int hasPreviousBatch, int duplicatedID,
            int hasFollowingBatch, int followingBatchHasException,
            double[] expected, int tableCount)
        throws NoConnectionsException, IOException, ProcCallException {

        VoltTable vt;
        String sql;
        try {
            // use the default value for partition column to route this procedure
            vt = client.callProcedure("SPBigBatchOnPartitionTable", 0,
                    hasPreviousBatch, duplicatedID,
                    hasFollowingBatch, followingBatchHasException).getResults()[0];
            if (isTrue(followingBatchHasException)) {
                assertTrue(isTrue(hasFollowingBatch));
                fail("Expected failure but succeeded.");
            }
            // validate returned value from the procedure calls
            validateRowOfLongs(vt, new long[]{duplicatedID > BIGBATCHTESTSIZE ? 0: -1});
        } catch(Exception e) {
            assertTrue(e.getMessage().contains("CONSTRAINT VIOLATION"));
            assertTrue(e.getMessage().contains("500.2")); // violated at row (3, 3.2)
            assertTrue(isTrue(hasFollowingBatch) && isTrue(followingBatchHasException));
        }

        sql = "select distinct ratio from P1 order by 1; ";
        validateTableColumnOfScalarFloat(client, sql, expected);

        sql = "select count(*) from P1; ";
        validateTableOfScalarLongs(client, sql, new long[]{tableCount});

        client.callProcedure("@AdHoc", "truncate table P1");
    }
    private static final int BIGBATCHTESTSIZE = 300;

    public void testBigBatchException() throws IOException, ProcCallException {
        System.out.println("test testBigBatchException...");
        Client client = getClient();

        int[] duplicates = {123, 200, 201, 256, 300};
        for (int dup: duplicates) {
            // exception in the first 200 batch
            bigBatchChecker(client, 0, dup, 0, 0, new double[]{}, 0);
            bigBatchChecker(client, 0, dup, 1, 0, new double[]{500.1}, 1);
            bigBatchChecker(client, 0, dup, 1, 1, new double[]{}, 0);
            bigBatchChecker(client, 1, dup, 0, 0, new double[]{prev}, 1);
            bigBatchChecker(client, 1, dup, 1, 0, new double[]{prev, 500.1}, 2);
            bigBatchChecker(client, 1, dup, 1, 1, new double[]{}, 0);
        }

        // exception in the second 200 batch
        int[] noDuplicates = {350, 400, 450};
        for (int noDup: noDuplicates) {
            // exception in the first 200 batch
            bigBatchChecker(client, 0, noDup, 0, 0, new double[]{10.1}, BIGBATCHTESTSIZE);
            bigBatchChecker(client, 0, noDup, 1, 0, new double[]{10.1, 500.1}, BIGBATCHTESTSIZE+1);
            bigBatchChecker(client, 0, noDup, 1, 1, new double[]{}, 0);
            bigBatchChecker(client, 1, noDup, 0, 0, new double[]{prev, 10.1}, BIGBATCHTESTSIZE+1);
            bigBatchChecker(client, 1, noDup, 1, 0, new double[]{prev, 10.1, 500.1}, BIGBATCHTESTSIZE+2);
            bigBatchChecker(client, 1, noDup, 1, 1, new double[]{}, 0);
        }
    }


    private void bigBatchAdvancedChecker(Client client, int partitionKey, int hasPreviousBatch,
            int hasBigBatch, int bigBatchDuplicatedID, int hasFollowingBatch,
            int hasFollowingTryCatchBatch, int hasDupsInTryCatch, int exitAbort,
            double[] expected, int tableCount)
        throws NoConnectionsException, IOException, ProcCallException {

        VoltTable vt;
        String sql;

        // use the default value for partition column to route this procedure
        vt = client.callProcedure("SPBigBatchAdvancedOnPartitionTable", 0,
                hasPreviousBatch, hasBigBatch, bigBatchDuplicatedID,
                hasFollowingBatch, hasFollowingTryCatchBatch,
                hasDupsInTryCatch, exitAbort).getResults()[0];
        // validate returned value from the procedure calls
        int result = bigBatchDuplicatedID > BIGBATCHTESTSIZE ? 0: -1;
        if (isTrue(hasDupsInTryCatch)) result = -2;
        validateRowOfLongs(vt, new long[]{result});

        sql = "select distinct ratio from P1 order by 1;";
        validateTableColumnOfScalarFloat(client, sql, expected);

        sql = "select count(*) from P1; ";
        validateTableOfScalarLongs(client, sql, new long[]{tableCount});

        client.callProcedure("@AdHoc", "truncate table P1");
    }

    public void testBigBatchAdvancedException() throws IOException, ProcCallException {
        System.out.println("test testBigBatchAdvancedException...");
        Client client = getClient();
        String sql;

        // so many more permutations
        bigBatchAdvancedChecker(client, 0, 1, 1, 150, 1, 1, 1, 0, new double[]{0.1, 500.1}, 2);
        bigBatchAdvancedChecker(client, 0, 1, 1, 250, 1, 1, 1, 0, new double[]{0.1, 500.1}, 2);
        bigBatchAdvancedChecker(client, 0, 0, 1, 150, 0, 0, 0, 0, new double[]{}, 0);

        //
        // Test multiple procedure call suggested from code review
        //

        // big batch roll back
        client.callProcedure("SPBigBatchAdvancedOnPartitionTable", 0, 1, 1, 150, 0, 0, 0, 0);
        // how transaction roll back
        try {
            client.callProcedure("SPBigBatchAdvancedOnPartitionTable", 0, 0, 0, 0, 0, 0, 0, 1);
            fail();
        } catch(Exception e) {
            assertTrue(e.getMessage().contains("CONSTRAINT VIOLATION"));
            assertTrue(e.getMessage().contains("700.2")); // violated at row (700, 700.2)
        }
        sql = "select distinct ratio from P1 order by 1;";
        validateTableColumnOfScalarFloat(client, sql, new double[]{0.1});


        // clear the data
        client.callProcedure("@AdHoc", "truncate table P1");

        //
        // Test the try catch and re-throw case
        //

        // SP
        try {
            client.callProcedure("SPCatchRethrowOnPartitionTable", 0, 1);
            fail();
        } catch(Exception e) {
            assertTrue(e.getMessage().contains("User's SP constraint error message"));
        }
        sql = "select distinct ratio from P1 order by 1;";
        validateTableColumnOfScalarFloat(client, sql, new double[]{});

        // two batches in the try catch block
        try {
            client.callProcedure("SPCatchRethrowOnPartitionTable", 0, 0);
        } catch(Exception e) {
            assertTrue(e.getMessage().contains("User's SP constraint error message"));
        }
        sql = "select distinct ratio from P1 order by 1;";
        validateTableColumnOfScalarFloat(client, sql, new double[]{});

        // MP
        try {
            client.callProcedure("MPCatchRethrowOnPartitionTable", 1);
            fail();
        } catch(Exception e) {
            System.err.println(e.getMessage());
            assertTrue(e.getMessage().contains("User's MP constraint error message"));
        }
        sql = "select distinct ratio from P1 order by 1;";
        validateTableColumnOfScalarFloat(client, sql, new double[]{});

        // two batches in the try catch block
        try {
            client.callProcedure("MPCatchRethrowOnPartitionTable", 0);
        } catch(Exception e) {
            assertTrue(e.getMessage().contains("User's MP constraint error message"));
        }
        sql = "select distinct ratio from P1 order by 1;";
        validateTableColumnOfScalarFloat(client, sql, new double[]{});
    }


    public TestCatchExceptionsInProcedure(String name) {
        super(name);
    }

    static final Class<?>[] PROCEDURES = {
        org.voltdb_testprocs.regressionsuites.catchexceptions.MPInsertOnReplicatedTable.class,
        org.voltdb_testprocs.regressionsuites.catchexceptions.SPInsertOnPartitionTable.class,
        org.voltdb_testprocs.regressionsuites.catchexceptions.MPInsertOnPartitionTable.class,
        org.voltdb_testprocs.regressionsuites.catchexceptions.SPMultipleTryCatchOnPartitionTable.class,
        org.voltdb_testprocs.regressionsuites.catchexceptions.SPBigBatchOnPartitionTable.class,
        org.voltdb_testprocs.regressionsuites.catchexceptions.SPBigBatchAdvancedOnPartitionTable.class,
        org.voltdb_testprocs.regressionsuites.catchexceptions.SPCatchRethrowOnPartitionTable.class,
        org.voltdb_testprocs.regressionsuites.catchexceptions.MPCatchRethrowOnPartitionTable.class
        };

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestCatchExceptionsInProcedure.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addProcedure(org.voltdb_testprocs.regressionsuites.catchexceptions.MPInsertOnReplicatedTable.class);
        project.addProcedure(org.voltdb_testprocs.regressionsuites.catchexceptions.MPInsertOnPartitionTable.class);
        project.addProcedure(org.voltdb_testprocs.regressionsuites.catchexceptions.MPCatchRethrowOnPartitionTable.class);

        ProcedurePartitionData data = new ProcedurePartitionData("P1", "NUM", "0");
        project.addProcedure(org.voltdb_testprocs.regressionsuites.catchexceptions.SPInsertOnPartitionTable.class,
                data);
        project.addProcedure(org.voltdb_testprocs.regressionsuites.catchexceptions.SPMultipleTryCatchOnPartitionTable.class,
                data);
        project.addProcedure(org.voltdb_testprocs.regressionsuites.catchexceptions.SPBigBatchOnPartitionTable.class,
                data);
        project.addProcedure(org.voltdb_testprocs.regressionsuites.catchexceptions.SPBigBatchAdvancedOnPartitionTable.class,
                data);
        project.addProcedure(org.voltdb_testprocs.regressionsuites.catchexceptions.SPCatchRethrowOnPartitionTable.class,
                data);

        final String literalSchema =
                "CREATE TABLE r1 ( "
                + "id INTEGER DEFAULT 0 NOT NULL, "
                + "num INTEGER DEFAULT 0 NOT NULL, "
                + "ratio FLOAT, "
                + "PRIMARY KEY (id) ); " +

                "CREATE TABLE p1 ( "
                + "id INTEGER DEFAULT 0 NOT NULL assumeunique, "
                + "num INTEGER DEFAULT 0 NOT NULL, "
                + "ratio FLOAT, "
                + "PRIMARY KEY (id, num) ); " +

                "PARTITION TABLE p1 ON COLUMN num; " +
                ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        boolean success;

        config = new LocalCluster("catchexceptions-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("catchexceptions-onesite.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
