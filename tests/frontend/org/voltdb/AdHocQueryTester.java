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

package org.voltdb;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.google_voltpatches.common.base.Strings;
import org.voltcore.utils.PortGenerator;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.utils.MiscUtils;

public abstract class AdHocQueryTester extends JUnit4LocalClusterTest {

    // TODO: make an enum
    protected final int NOT_VALIDATING_SP_RESULT = 0;
    protected final int VALIDATING_SP_RESULT = 1;
    protected final int VALIDATING_TOTAL_SP_RESULT = 2;

    public static void setUpSchema(VoltProjectBuilder builder, String pathToCatalog, String pathToDeployment)
            throws Exception {
        String schema =
                "create table PARTED1 (" +
                "PARTVAL bigint not null, " +
                "NONPART bigint not null," +
                "PRIMARY KEY(PARTVAL));" +

                "create table PARTED2 (" +
                "PARTVAL bigint not null, " +
                "NONPART bigint not null," +
                "PRIMARY KEY(PARTVAL));" +

                "create table PARTED3 (" +
                "PARTVAL bigint not null, " +
                "NONPART bigint not null ASSUMEUNIQUE," +
                "PRIMARY KEY(NONPART, PARTVAL));" +

                "create table PARTED4 (" +
                "PARTVAL integer not null, " +
                "NONPART bigint not null ASSUMEUNIQUE," +
                "PRIMARY KEY(NONPART, PARTVAL));" +

                "create table REPPED1 (" +
                "REPPEDVAL bigint not null, " +
                "NONPART bigint not null," +
                "PRIMARY KEY(REPPEDVAL));" +

                "create table REPPED2 (" +
                "REPPEDVAL bigint not null, " +
                "NONPART bigint not null," +
                "PRIMARY KEY(REPPEDVAL));" +

                "create view V_PARTED1 (PARTVAL, num_rows, sum_bigint) as " +
                "select PARTVAL, count(*), sum(NONPART) from PARTED1 group by PARTVAL;" +

                "create view V_SCATTERED1 (NONPART, PARTVAL, num_rows, sum_bigint) as " +
                "select NONPART, PARTVAL, count(*), sum(PARTVAL) from PARTED1 group by NONPART, PARTVAL;" +

                "create view V_REPPED1 (REPPEDVAL, num_rows, sum_bigint) as " +
                "select REPPEDVAL, count(*), sum(NONPART) from REPPED1 group by REPPEDVAL;" +

                "create table long_query_table (id INTEGER NOT NULL, NAME VARCHAR(16));" +
                "";

        builder.addLiteralSchema(schema);
        builder.addPartitionInfo("PARTED1", "PARTVAL");
        builder.addPartitionInfo("PARTED2", "PARTVAL");
        builder.addPartitionInfo("PARTED3", "PARTVAL");
        builder.addPartitionInfo("PARTED4", "PARTVAL");

        builder.addProcedure(org.voltdb_testprocs.adhoc.executeSQLMP.class);
        builder.addProcedure(org.voltdb_testprocs.adhoc.executeSQLMPWRITE.class);

        ProcedurePartitionData parted1Data = new ProcedurePartitionData("PARTED1", "PARTVAL", "0");
        builder.addProcedure(org.voltdb_testprocs.adhoc.executeSQLSP.class, parted1Data);
        builder.addProcedure(org.voltdb_testprocs.adhoc.executeSQLSPWRITE.class, parted1Data);
    }

    public static VoltDB.Configuration setUpSPDB() throws Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocsp.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocsp.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();

        setUpSchema(builder, pathToCatalog, pathToDeployment);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration(new PortGenerator());
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        return config;
    }

    public abstract int runQueryTest(String query, int hash, int spPartialSoFar, int expected, int validatingSPresult)
            throws IOException, ProcCallException;

    /**
     * @throws ProcCallException
     * @throws IOException
     * @throws NoConnectionsException
     */
    protected void runAllAdHocSPtests(int hashableA, int hashableB, int hashableC, int hashableD)
            throws NoConnectionsException, IOException, ProcCallException {
        int spPartialCount = 0;
        spPartialCount = runQueryTest("SELECT * FROM PARTED1;",
                hashableA, 0, 2, NOT_VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED1 WHERE PARTVAL != %d;", hashableA),
                hashableB, spPartialCount-1, 1, VALIDATING_TOTAL_SP_RESULT);

        spPartialCount = runQueryTest("SELECT * FROM PARTED3;",
                hashableA, 0, 2, NOT_VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED3 WHERE PARTVAL != %d;", hashableA),
                hashableB, spPartialCount-1, 1, VALIDATING_TOTAL_SP_RESULT);

        runQueryTest("SELECT * FROM REPPED1;", hashableA, 0, 2, VALIDATING_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM PARTED1 WHERE PARTVAL = %d;", hashableA),
                hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED3 WHERE PARTVAL = %d;", hashableA),
                hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 WHERE REPPEDVAL = %d;", hashableA),
                hashableA, 0, 1, VALIDATING_SP_RESULT);

        spPartialCount = runQueryTest("SELECT * FROM V_PARTED1;",
                hashableA, 0, 2, NOT_VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM V_PARTED1 WHERE PARTVAL != %d;", hashableA),
                hashableB, spPartialCount-1, 1, VALIDATING_TOTAL_SP_RESULT);

        spPartialCount = runQueryTest("SELECT * FROM V_SCATTERED1;",
                hashableA, 0, 2, NOT_VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM V_SCATTERED1 WHERE NONPART != %d;", hashableA),
                hashableB, spPartialCount-1, 1, VALIDATING_TOTAL_SP_RESULT);

        runQueryTest("SELECT * FROM V_REPPED1;",
                hashableA, 0, 2, VALIDATING_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM V_PARTED1 WHERE PARTVAL = %d;", hashableA),
                hashableA, 0, 1, VALIDATING_SP_RESULT);

        spPartialCount = runQueryTest(String.format("SELECT * FROM V_SCATTERED1 WHERE NONPART = %d;", hashableA),
                hashableA, 0, 1, NOT_VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM V_SCATTERED1 WHERE NONPART != %d;", hashableA),
                hashableB, spPartialCount-1, 1, VALIDATING_TOTAL_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM V_REPPED1 WHERE REPPEDVAL = %d;", hashableA),
                hashableA, 0, 1, VALIDATING_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = %d and B.PARTVAL = %d;",
                hashableA, hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = %d and B.PARTVAL = %d;",
                hashableA, hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = %d and B.PARTVAL = %d;",
                hashableA, hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = %d and B.PARTVAL = %d;",
                hashableA, hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = %d and B.REPPEDVAL = %d;",
                hashableA, hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = %d and B.REPPEDVAL = %d;",
                hashableA, hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = %d and A.PARTVAL = B.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = %d and A.PARTVAL = B.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = %d and A.PARTVAL = B.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = %d and A.REPPEDVAL = B.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = %d and A.PARTVAL = B.REPPEDVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = %d and A.REPPEDVAL = B.REPPEDVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = %d and B.PARTVAL = A.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = %d and B.PARTVAL = A.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = %d and B.PARTVAL = A.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = %d and B.PARTVAL = A.REPPEDVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = %d and B.REPPEDVAL = A.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = %d and B.REPPEDVAL = A.REPPEDVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM PARTED1 A, PARTED2 B WHERE B.PARTVAL = %d and A.PARTVAL = B.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED3 A, PARTED2 B WHERE B.PARTVAL = %d and A.PARTVAL = B.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, PARTED3 B WHERE B.PARTVAL = %d and A.PARTVAL = B.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = %d and A.REPPEDVAL = B.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = %d and A.PARTVAL = B.REPPEDVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE B.REPPEDVAL = %d and A.REPPEDVAL = B.REPPEDVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM PARTED1 A, PARTED2 B WHERE B.PARTVAL = %d and B.PARTVAL = A.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED3 A, PARTED2 B WHERE B.PARTVAL = %d and B.PARTVAL = A.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, PARTED3 B WHERE B.PARTVAL = %d and B.PARTVAL = A.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = %d and B.PARTVAL = A.REPPEDVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = %d and B.REPPEDVAL = A.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE B.REPPEDVAL = %d and B.REPPEDVAL = A.REPPEDVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = B.PARTVAL and A.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = B.PARTVAL and A.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = B.PARTVAL and A.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = B.PARTVAL and A.REPPEDVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = B.REPPEDVAL and A.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = B.REPPEDVAL and A.REPPEDVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = B.PARTVAL and B.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = B.PARTVAL and B.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = B.PARTVAL and B.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = B.PARTVAL and B.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = B.REPPEDVAL and B.REPPEDVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = B.REPPEDVAL and B.REPPEDVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM PARTED1 A, PARTED2 B WHERE B.PARTVAL = A.PARTVAL and A.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED3 A, PARTED2 B WHERE B.PARTVAL = A.PARTVAL and A.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, PARTED3 B WHERE B.PARTVAL = A.PARTVAL and A.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = A.REPPEDVAL and A.REPPEDVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = A.PARTVAL and A.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE B.REPPEDVAL = A.REPPEDVAL and A.REPPEDVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM PARTED1 A, PARTED2 B WHERE B.PARTVAL = A.PARTVAL and B.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED3 A, PARTED2 B WHERE B.PARTVAL = A.PARTVAL and B.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, PARTED3 B WHERE B.PARTVAL = A.PARTVAL and B.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = A.REPPEDVAL and B.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = A.PARTVAL and B.REPPEDVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE B.REPPEDVAL = A.REPPEDVAL and B.REPPEDVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);


        // Selectively try a sampling of these same cases with materialized view tables.
        runQueryTest(String.format("SELECT * FROM V_PARTED1 A, PARTED2 B WHERE A.PARTVAL = %d and B.PARTVAL = %d;",
                hashableA, hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM V_REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = %d and B.PARTVAL = %d;",
                hashableA, hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, V_REPPED1 B WHERE A.PARTVAL = %d and B.REPPEDVAL = %d;",
                hashableA, hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM V_REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = %d and B.REPPEDVAL = %d;",
                hashableA, hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        spPartialCount = runQueryTest(String.format("SELECT * FROM V_SCATTERED1 A, REPPED2 B WHERE A.NONPART = %d and B.REPPEDVAL = %d;",
                hashableA, hashableA), hashableA, 0, 1, NOT_VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM V_SCATTERED1 A, REPPED2 B WHERE A.NONPART = %d and B.REPPEDVAL = %d;",
                hashableA, hashableA), hashableB, spPartialCount, 1, VALIDATING_TOTAL_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM V_PARTED1 A, PARTED2 B WHERE A.PARTVAL = %d and A.PARTVAL = B.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM V_REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = %d and A.REPPEDVAL = B.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, V_REPPED1 B WHERE A.PARTVAL = %d and A.PARTVAL = B.REPPEDVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM V_REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = %d and A.REPPEDVAL = B.REPPEDVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        spPartialCount = runQueryTest(String.format("SELECT * FROM V_SCATTERED1 A, REPPED2 B WHERE A.NONPART = %d and A.NONPART = B.REPPEDVAL;",
                hashableA), hashableA, 0, 1, NOT_VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM V_SCATTERED1 A, REPPED2 B WHERE A.NONPART = %d and A.NONPART = B.REPPEDVAL;",
                hashableA), hashableB, spPartialCount, 1, VALIDATING_TOTAL_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM V_PARTED1 A, PARTED2 B WHERE B.PARTVAL = A.PARTVAL and B.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM V_REPPED1 A, PARTED2 B WHERE B.PARTVAL = A.REPPEDVAL and B.PARTVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, V_REPPED1 B WHERE B.REPPEDVAL = A.PARTVAL and B.REPPEDVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM V_REPPED1 A, REPPED2 B WHERE B.REPPEDVAL = A.REPPEDVAL and B.REPPEDVAL = %d;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        spPartialCount = runQueryTest(String.format("SELECT * FROM V_SCATTERED1 A, REPPED2 B WHERE B.REPPEDVAL = A.NONPART and B.REPPEDVAL = %d;",
                hashableA), hashableA, 0, 1, NOT_VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM V_SCATTERED1 A, REPPED2 B WHERE B.REPPEDVAL = A.NONPART and B.REPPEDVAL = %d;",
                hashableA), hashableB, spPartialCount, 1, VALIDATING_TOTAL_SP_RESULT);

/* These queries are not yet supported SP because of B's varying partition key.
        runQueryTest(String.format("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = %d and B.PARTVAL != A.PARTVAL;", hashableA), hashableA, 1);
        runQueryTest(String.format("SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = %d and B.PARTVAL != A.PARTVAL;", hashableA), hashableA, 1);
        runQueryTest(String.format("SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = %d and B.PARTVAL != A.PARTVAL;", hashableA), hashableA, 1);
*/
        spPartialCount = runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = %d and B.PARTVAL != A.REPPEDVAL;",
                hashableA), hashableA, 0, 1, NOT_VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = %d and B.PARTVAL != A.REPPEDVAL;",
                hashableA), hashableB, spPartialCount, 1, VALIDATING_TOTAL_SP_RESULT);

        runQueryTest(String.format("SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = %d and B.REPPEDVAL != A.PARTVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = %d and B.REPPEDVAL != A.REPPEDVAL;",
                hashableA), hashableA, 0, 1, VALIDATING_SP_RESULT);

        spPartialCount = runQueryTest(String.format("SELECT * FROM PARTED1 A LEFT JOIN PARTED2 B ON A.PARTVAL = %d and B.PARTVAL = A.PARTVAL;",
                hashableA), hashableA, 0, 2, NOT_VALIDATING_SP_RESULT);
        runQueryTest(String.format("SELECT * FROM PARTED1 A LEFT JOIN PARTED2 B ON A.PARTVAL = %d and B.PARTVAL = A.PARTVAL;",
                hashableA), hashableB, spPartialCount, 2, NOT_VALIDATING_SP_RESULT);
        try {
            runQueryTest(String.format("SELECT * FROM PARTED1 A LEFT JOIN PARTED2 B ON A.PARTVAL = %d and B.PARTVAL = %d;",
                    hashableA, hashableA), hashableA, 0, 1, NOT_VALIDATING_SP_RESULT);
        } catch (Exception pce) {
            String msg = pce.toString();
            assertTrue(msg.contains("This query is not plannable.  The planner cannot guarantee that all rows would be in a single partition."));
        }

        // spPartialCount = runQueryTest(String.format("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = B.PARTVAL;"), hashableA, 0, 2, NOT_VALIDATING_SP_RESULT);
        // runQueryTest(String.format("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = B.PARTVAL;"), hashableB, spPartialCount, 2, VALIDATING_TOTAL_SP_RESULT);

        // TODO: Three-way join test cases are probably required to cover all code paths through AccessPaths.
    }

    protected static String getQueryForLongQueryTable(int numberOfPredicates) {
        StringBuilder sb = new StringBuilder("SELECT count(*) FROM long_query_table ");
        if (numberOfPredicates > 0) {
            sb.append("WHERE ID = 123 ").append(Strings.repeat("AND ID > 100 ", numberOfPredicates - 1));
        }
        sb.append(";");
        return sb.toString();
    }

}
