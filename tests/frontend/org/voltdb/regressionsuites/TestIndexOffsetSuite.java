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

import junit.framework.Test;
import org.voltdb.BackendTarget;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.BatchedMultiPartitionTest;

import java.io.IOException;

public class TestIndexOffsetSuite extends RegressionSuite {

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestIndexOffsetSuite(String name) {
        super(name);
    }

    private void callWithExpectedTupleId(Client client, int tupleId, String procName, Object... params)
            throws IOException, ProcCallException {
        ClientResponse cr = client.callProcedure(procName, params);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertEquals(1, cr.getResults().length);
        VoltTable result = cr.getResults()[0];

        if (tupleId == Integer.MIN_VALUE) {
            assertEquals(0, result.getRowCount());
        } else {
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals(tupleId, result.getLong(0));
        }
    }

    private void callWithExpectedKeyValue(Client client, String columnName, VoltType type, Object value,
            String procName, Object... params) throws IOException, ProcCallException {
        ClientResponse cr = client.callProcedure(procName, params);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertEquals(1, cr.getResults().length);
        VoltTable result = cr.getResults()[0];

        if (value == null) {
            assertEquals(0, result.getRowCount());
        } else {
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals(value, result.get(columnName, type));
        }
    }

    private void checkExplainPlan(Client client, String[] procedures) throws IOException, ProcCallException {
        for (String proc: procedures) {
            VoltTable vt = client.callProcedure("@ExplainProc", proc).getResults()[0];
            assertTrue(vt.toString(), vt.toString().contains("for offset rank lookup"));
        }
    }

    public void testSingleColumnIndex() throws Exception {
        System.out.println("Running testSingleColumnIndex");
        Client client = getClient();

        // check offset rank lookup plan
        checkExplainPlan(client, new String[]{
                "TU1_ID", "TU1_ABS_POINTS", "TU1_ID_DESC", "TU1_ABS_POINTS_DESC", "TM1_POINTS"});

        // Unique Map, Single column index
        client.callProcedure("TU1.insert", 1, 1);
        client.callProcedure("TU1.insert", 2, 2);
        client.callProcedure("TU1.insert", 3, 3);
        client.callProcedure("TU1.insert", 6, 6);
        client.callProcedure("TU1.insert", 8, 8);
        client.callProcedure("TU1.insert", 10, null);

        // unique key single column index
        callWithExpectedTupleId(client, 1, "TU1_ID", 0, 0);
        callWithExpectedTupleId(client, 2, "TU1_ID", 0, 1);
        callWithExpectedTupleId(client, 3, "TU1_ID", 0, 2);
        callWithExpectedTupleId(client, 6, "TU1_ID", 0, 3);
        callWithExpectedTupleId(client, 8, "TU1_ID", 0, 4);
        callWithExpectedTupleId(client, 10, "TU1_ID", 0, 5);
        callWithExpectedTupleId(client, Integer.MIN_VALUE, "TU1_ID", 0, 6);

        // descending order
        callWithExpectedTupleId(client, 10, "TU1_ID_DESC", 0, 0);
        callWithExpectedTupleId(client, 8, "TU1_ID_DESC", 0, 1);
        callWithExpectedTupleId(client, 6, "TU1_ID_DESC", 0, 2);
        callWithExpectedTupleId(client, 3, "TU1_ID_DESC", 0, 3);
        callWithExpectedTupleId(client, 2, "TU1_ID_DESC", 0, 4);
        callWithExpectedTupleId(client, 1, "TU1_ID_DESC", 0, 5);
        callWithExpectedTupleId(client, Integer.MIN_VALUE, "TU1_ID_DESC", 0, 6);

        // NULL value is the MIN VALUE stored in VoltDB
        // check expression index

        // non-deterministic query, but our index is compacting index without holes
        if (!isHSQL() && !isValgrind()) {
            callWithExpectedTupleId(client, 10, "TU1_ABS_POINTS", 0, 0);
            callWithExpectedTupleId(client, 1, "TU1_ABS_POINTS", 0, 1);
            callWithExpectedTupleId(client, 8, "TU1_ABS_POINTS", 0, 5);
            callWithExpectedTupleId(client, Integer.MIN_VALUE, "TU1_ABS_POINTS", 0, 6);

            // hsql handle NULL differently
            callWithExpectedTupleId(client, 8, "TU1_ABS_POINTS_DESC", 0, 0);
            callWithExpectedTupleId(client, 6, "TU1_ABS_POINTS_DESC", 0, 1);
            callWithExpectedTupleId(client, 10, "TU1_ABS_POINTS_DESC", 0, 5);
            callWithExpectedTupleId(client, Integer.MIN_VALUE, "TU1_ABS_POINTS_DESC", 0, 6);
        }

        if (!isValgrind()) {
            // Non-deterministic storage makes this fail in MEMCHECK builds

            // Multi-map
            client.callProcedure("TM1.insert", 1, 1);
            client.callProcedure("TM1.insert", 2, 2);
            client.callProcedure("TM1.insert", 3, 2);
            client.callProcedure("TM1.insert", 4, 2);
            client.callProcedure("TM1.insert", 5, 3);
            client.callProcedure("TM1.insert", 6, 6);
            client.callProcedure("TM1.insert", 7, 6);
            client.callProcedure("TM1.insert", 8, 8);
            client.callProcedure("TM1.insert", 9, null);
            client.callProcedure("TM1.insert", 10, null);


            // non-deterministic query, but our index is compacting index without holes
            callWithExpectedTupleId(client, 9, "TM1_POINTS", 0, 0);
            callWithExpectedTupleId(client, 10, "TM1_POINTS", 0, 1);
            callWithExpectedTupleId(client, 1, "TM1_POINTS", 0, 2);
            callWithExpectedTupleId(client, 2, "TM1_POINTS", 0, 3);
            callWithExpectedTupleId(client, 3, "TM1_POINTS", 0, 4);
            callWithExpectedTupleId(client, 4, "TM1_POINTS", 0, 5);
            callWithExpectedTupleId(client, 5, "TM1_POINTS", 0, 6);
            callWithExpectedTupleId(client, 6, "TM1_POINTS", 0, 7);
            callWithExpectedTupleId(client, 7, "TM1_POINTS", 0, 8);
            callWithExpectedTupleId(client, 8, "TM1_POINTS", 0, 9);
            callWithExpectedTupleId(client, Integer.MIN_VALUE, "TM1_POINTS", 0, 10);
        }
    }

    public void testTwoOrMoreColumnsUniqueIndex() throws Exception {
        Client client = getClient();

        checkExplainPlan(client, new String[]{
                "TU2_BY_UNAME_POINTS", "TU2_BY_UNAME", "TU2_BY_UNAME_POINTS_DESC", "TU2_BY_UNAME_DESC"});

        client.callProcedure("TU2.insert", 1, 1, "xin");
        client.callProcedure("TU2.insert", 2, 2, "xin");
        client.callProcedure("TU2.insert", 3, 3, "xin");
        client.callProcedure("TU2.insert", 4, 6, "xin");
        client.callProcedure("TU2.insert", 5, 8, "xin");
        client.callProcedure("TU2.insert", 6, 1, "jiao");
        client.callProcedure("TU2.insert", 7, 2, "jiao");
        client.callProcedure("TU2.insert", 8, 3, "jiao");
        client.callProcedure("TU2.insert", 9, 6, "jiao");
        client.callProcedure("TU2.insert", 10, 8, "jiao");

        // test order by two column with index
        callWithExpectedTupleId(client, 6, "TU2_BY_UNAME_POINTS", 0, 0);
        callWithExpectedTupleId(client, 7, "TU2_BY_UNAME_POINTS", 0, 1);
        callWithExpectedTupleId(client, 8, "TU2_BY_UNAME_POINTS", 0, 2);
        callWithExpectedTupleId(client, 9, "TU2_BY_UNAME_POINTS", 0, 3);
        callWithExpectedTupleId(client, 10, "TU2_BY_UNAME_POINTS", 0, 4);

        callWithExpectedTupleId(client, 1, "TU2_BY_UNAME_POINTS", 0, 5);
        callWithExpectedTupleId(client, 2, "TU2_BY_UNAME_POINTS", 0, 6);
        callWithExpectedTupleId(client, 3, "TU2_BY_UNAME_POINTS", 0, 7);
        callWithExpectedTupleId(client, 4, "TU2_BY_UNAME_POINTS", 0, 8);
        callWithExpectedTupleId(client, 5, "TU2_BY_UNAME_POINTS", 0, 9);
        callWithExpectedTupleId(client, Integer.MIN_VALUE, "TU2_BY_UNAME_POINTS", 0, 10);

        // test order by one column with index, this is not a deterministic query
        callWithExpectedKeyValue(client, "UNAME", VoltType.STRING, "jiao", "TU2_BY_UNAME", 0, 0);
        callWithExpectedKeyValue(client, "UNAME", VoltType.STRING, "jiao", "TU2_BY_UNAME", 0, 1);
        callWithExpectedKeyValue(client, "UNAME", VoltType.STRING, "jiao", "TU2_BY_UNAME", 0, 2);
        callWithExpectedKeyValue(client, "UNAME", VoltType.STRING, "xin", "TU2_BY_UNAME", 0, 6);

        // test descending
        callWithExpectedTupleId(client, 5, "TU2_BY_UNAME_POINTS_DESC", 0, 0);
        callWithExpectedTupleId(client, 4, "TU2_BY_UNAME_POINTS_DESC", 0, 1);
        callWithExpectedTupleId(client, 10, "TU2_BY_UNAME_POINTS_DESC", 0, 5);
        callWithExpectedTupleId(client, Integer.MIN_VALUE, "TU2_BY_UNAME_POINTS_DESC", 0, 10);

        callWithExpectedKeyValue(client, "UNAME", VoltType.STRING, "xin", "TU2_BY_UNAME_DESC", 0, 0);
        callWithExpectedKeyValue(client, "UNAME", VoltType.STRING, "xin", "TU2_BY_UNAME_DESC", 0, 1);
        callWithExpectedKeyValue(client, "UNAME", VoltType.STRING, "xin", "TU2_BY_UNAME_DESC", 0, 3);
        callWithExpectedKeyValue(client, "UNAME", VoltType.STRING, "jiao", "TU2_BY_UNAME_DESC", 0, 6);
    }

    static public Test suite() {
        VoltServerConfig config = null;

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestIndexOffsetSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(BatchedMultiPartitionTest.class.getResource("sqlindex-ddl.sql"));

        // this is not a deterministic query
        project.addStmtProcedure("TU1_ID",
                "SELECT ID, POINTS, CAST(? AS INTEGER) AS PARTITION FROM TU1 ORDER BY ID OFFSET ? LIMIT 1",
                new ProcedurePartitionData("TU1", "ID", "0"));

        project.addStmtProcedure("TU1_ABS_POINTS",
                "SELECT ID, POINTS, CAST(? AS INTEGER) AS PARTITION FROM TU1 ORDER BY ABS(POINTS) OFFSET ? LIMIT 1",
                new ProcedurePartitionData("TU1", "ID", "0"));

        project.addStmtProcedure("TU1_ID_DESC",
                "SELECT ID, POINTS, CAST(? AS INTEGER) AS PARTITION FROM TU1 ORDER BY ID DESC OFFSET ? LIMIT 1",
                new ProcedurePartitionData("TU1", "ID", "0"));

        project.addStmtProcedure("TU1_ABS_POINTS_DESC",
                "SELECT ID, POINTS, CAST(? AS INTEGER) AS PARTITION FROM TU1 ORDER BY ABS(POINTS) DESC OFFSET ? LIMIT 1",
                new ProcedurePartitionData("TU1", "ID", "0"));

        project.addStmtProcedure("TM1_POINTS",
                "SELECT ID, POINTS, CAST(? AS INTEGER) AS PARTITION FROM TM1 ORDER BY POINTS OFFSET ? LIMIT 1",
                new ProcedurePartitionData("TM1", "ID", "0"));

        // multi-column index
        project.addStmtProcedure("TU2_BY_UNAME_POINTS",
                "SELECT ID, UNAME, CAST(? AS INTEGER) AS PARTITION FROM TU2 ORDER BY UNAME, POINTS OFFSET ? LIMIT 1",
                new ProcedurePartitionData("TU2", "UNAME", "0"));

        // this is not a deterministic query
        project.addStmtProcedure("TU2_BY_UNAME",
                "SELECT ID, UNAME, CAST(? AS INTEGER) AS PARTITION FROM TU2 ORDER BY UNAME OFFSET ? LIMIT 1",
                new ProcedurePartitionData("TU2", "UNAME", "0"));

        project.addStmtProcedure("TU2_BY_UNAME_POINTS_DESC",
                "SELECT ID, UNAME, CAST(? AS INTEGER) AS PARTITION FROM TU2 ORDER BY UNAME DESC, POINTS DESC OFFSET ? LIMIT 1",
                new ProcedurePartitionData("TU2", "UNAME", "0"));

        // this is not a deterministic query
        project.addStmtProcedure("TU2_BY_UNAME_DESC",
                "SELECT ID, UNAME, CAST(? AS INTEGER) AS PARTITION FROM TU2 ORDER BY UNAME DESC OFFSET ? LIMIT 1",
                new ProcedurePartitionData("TU2", "UNAME", "0"));

        boolean success;

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalCluster("sqlOffsetIndex-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        // build the jarfile
        success = config.compile(project);
        assert(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("sqlOffsetIndex-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }
}
