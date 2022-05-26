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
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

/*
 * This set of tests is verifies the results of Queries performed in different batches
 * to ensure no data loss irrespective of whether the first, final or fallback buffers are
 * used between the EE and the procedure runner.
 */

public class TestBigBatchAndFallbackBufferResults extends RegressionSuite {

    private void populateTable(Client client, int rowCount) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr = client.callProcedure("SPPopulatePartitionTable", 0, rowCount);
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        assertTrue(cr.getResults()[0].asScalarLong() == 0);
   }

    private void processBigMultiQueryProc(Client client, int returnBuffer, int checkMiddle, int useFinal,
            int firstMod, int secondMod, int thirdMod) throws NoConnectionsException, IOException, ProcCallException {
        String reqStr = returnBuffer + ", " + checkMiddle + ", " + useFinal + " using ( " +
                firstMod + ":" + secondMod + ":" + thirdMod +")";
        System.out.println("requesting: " + reqStr);
        ClientResponse cr = client.callProcedure("SPMultiStatementQuery", 0, returnBuffer, checkMiddle, useFinal, firstMod, secondMod, thirdMod);
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        VoltTable vt = cr.getResults()[0];
        vt.advanceRow();
        Long val = vt.getLong("id");
        switch (returnBuffer) {
        case RETURN_FIRST:
            assertTrue(val == firstMod);
            break;
        case RETURN_MIDDLE:
            assertTrue(val == secondMod);
            break;
        case RETURN_LAST:
            assertTrue(val == thirdMod);
            break;
        default:
            assertTrue(false);
        }
    }

    private static final int RETURN_FIRST = 1;
    private static final int RETURN_MIDDLE = 2;
    private static final int RETURN_LAST = 3;
    private static final int PERFORM_MIDDLE_QUERY = 1;
    private static final int SKIP_MIDDLE_QUERY = 0;
    private static final int USE_FINAL_OPTION = 1;
    private static final int NO_FINAL_OPTION = 0;
    //10485743
    public void testQueryIsolation() throws IOException, ProcCallException {
        System.out.println("test testQueryIsolation...");
        Client client = getClient();
        // Note that because there are 300 rows in the table and each row is slightly over 1MB a MOD
        // value of 6 or lower will return 50+ rows which will exceed the max result buffer limit
        // Therefore a MOD value of < 7 for the middle query will fail if that query is executed.
        // In addition, a MOD value of 31 or more will ensure that the buffer is less than the 10MB,
        // ensuring that the fallback buffer is not used at all.
        populateTable(client, 300);

        // Test first buffers
        processBigMultiQueryProc(client, RETURN_FIRST, SKIP_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 1, 9);
        processBigMultiQueryProc(client, RETURN_FIRST, SKIP_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 1, 33);
        processBigMultiQueryProc(client, RETURN_FIRST, SKIP_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 1, 9);
        processBigMultiQueryProc(client, RETURN_FIRST, SKIP_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 1, 33);
        processBigMultiQueryProc(client, RETURN_FIRST, SKIP_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 1, 9);
        processBigMultiQueryProc(client, RETURN_FIRST, SKIP_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 1, 33);
        processBigMultiQueryProc(client, RETURN_FIRST, SKIP_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 1, 9);
        processBigMultiQueryProc(client, RETURN_FIRST, SKIP_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 1, 33);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 8, 9);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 8, 33);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 8, 9);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 8, 33);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 8, 9);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 8, 33);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 8, 9);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 8, 33);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 32, 9);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 32, 33);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 32, 9);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 32, 33);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 32, 9);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 32, 33);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 32, 9);
        processBigMultiQueryProc(client, RETURN_FIRST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 32, 33);

        // Test second buffer values
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 8, 9);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 8, 33);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 8, 9);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 8, 33);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 8, 9);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 8, 33);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 8, 9);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 8, 33);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 32, 9);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 32, 33);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 32, 9);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 32, 33);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 32, 9);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 32, 33);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 32, 9);
        processBigMultiQueryProc(client, RETURN_MIDDLE, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 32, 33);

        // Test third buffer values
        processBigMultiQueryProc(client, RETURN_LAST, SKIP_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 1, 9);
        processBigMultiQueryProc(client, RETURN_LAST, SKIP_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 1, 33);
        processBigMultiQueryProc(client, RETURN_LAST, SKIP_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 1, 9);
        processBigMultiQueryProc(client, RETURN_LAST, SKIP_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 1, 33);
        processBigMultiQueryProc(client, RETURN_LAST, SKIP_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 1, 9);
        processBigMultiQueryProc(client, RETURN_LAST, SKIP_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 1, 33);
        processBigMultiQueryProc(client, RETURN_LAST, SKIP_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 1, 9);
        processBigMultiQueryProc(client, RETURN_LAST, SKIP_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 1, 33);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 8, 9);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 8, 33);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 8, 9);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 8, 33);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 8, 9);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 8, 33);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 8, 9);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 8, 33);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 32, 9);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 7, 32, 33);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 32, 9);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, USE_FINAL_OPTION, 31, 32, 33);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 32, 9);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 7, 32, 33);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 32, 9);
        processBigMultiQueryProc(client, RETURN_LAST, PERFORM_MIDDLE_QUERY, NO_FINAL_OPTION, 31, 32, 33);
    }

    public TestBigBatchAndFallbackBufferResults(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestBigBatchAndFallbackBufferResults.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addProcedure(org.voltdb_testprocs.regressionsuites.fallbackbuffers.SPPopulatePartitionTable.class,
                new ProcedurePartitionData("P1", "NUM"));
        project.addProcedure(org.voltdb_testprocs.regressionsuites.fallbackbuffers.SPMultiStatementQuery.class,
                new ProcedurePartitionData("P1", "NUM"));

        final String literalSchema =
                "CREATE TABLE p1 ( "
                + "id INTEGER DEFAULT 0 NOT NULL assumeunique, "
                + "num INTEGER DEFAULT 0 NOT NULL, "
                + "str VARCHAR(1048576 BYTES), "
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
//        config = new LocalCluster("catchexceptions-onesite.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
//        success = config.compile(project);
//        assertTrue(success);
//        builder.addServerConfig(config);

        return builder;
    }
}
