/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *//* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
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
        spSingleTryCatchChecker(client, 1, 0, 0, 0, new double[]{0.1, 1.1});
        spSingleTryCatchChecker(client, 1, 0, 1, 0, new double[]{0.1, 1.1, 3.1});
        spSingleTryCatchChecker(client, 1, 0, 1, 1, new double[]{});
        spSingleTryCatchChecker(client, 1, 1, 0, 0, new double[]{0.1});
        spSingleTryCatchChecker(client, 1, 1, 1, 0, new double[]{0.1, 3.1});
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
        double prv = 0.1;
        spMultiTryCatchChecker(client, 1, 0, 0, 0, 0, new double[]{prv, 1.1, 2.1});
        spMultiTryCatchChecker(client, 1, 1, 1, 1, 1, new double[]{});

        spMultiTryCatchChecker(client, 1, 1, 0, 0, 0, new double[]{prv, 2.1});
        spMultiTryCatchChecker(client, 1, 0, 1, 0, 0, new double[]{prv, 1.1});
        spMultiTryCatchChecker(client, 1, 0, 0, 1, 0, new double[]{prv, 1.1, 2.1, 3.1});

        spMultiTryCatchChecker(client, 1, 1, 1, 0, 0, new double[]{prv});
        spMultiTryCatchChecker(client, 1, 1, 0, 1, 0, new double[]{prv, 2.1, 3.1});
        spMultiTryCatchChecker(client, 1, 0, 1, 1, 0, new double[]{prv, 1.1, 3.1});
        spMultiTryCatchChecker(client, 1, 0, 0, 1, 1, new double[]{});

        spMultiTryCatchChecker(client, 1, 1, 1, 1, 0, new double[]{prv, 3.1});
        spMultiTryCatchChecker(client, 1, 1, 0, 1, 1, new double[]{});
        spMultiTryCatchChecker(client, 1, 0, 1, 1, 1, new double[]{});
    }

    public TestCatchExceptionsInProcedure(String name) {
        super(name);
    }

    static final Class<?>[] PROCEDURES = {
        org.voltdb_testprocs.regressionsuites.catchexceptions.MPInsertOnReplicatedTable.class,
        org.voltdb_testprocs.regressionsuites.catchexceptions.SPInsertOnPartitionTable.class,
        org.voltdb_testprocs.regressionsuites.catchexceptions.MPInsertOnPartitionTable.class,
        org.voltdb_testprocs.regressionsuites.catchexceptions.SPMultipleTryCatchOnPartitionTable.class
        };

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestCatchExceptionsInProcedure.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addProcedures(PROCEDURES);
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
