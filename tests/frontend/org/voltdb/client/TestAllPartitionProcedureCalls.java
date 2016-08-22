/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
package org.voltdb.client;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.utils.VoltFile;

/**
 *  Test client all partition calls
 *
 */
public class TestAllPartitionProcedureCalls extends RegressionSuite {

    static final Class<?>[] PROCEDURES = {IntPartitionCallTestProc.class,
                                          StringPartitionCallTestProc.class};

    static Map<String, Integer> EXPECT_PARTIITON_COUNTS = new HashMap<String, Integer>();
    static {
        EXPECT_PARTIITON_COUNTS.put("0", new Integer(129));
        EXPECT_PARTIITON_COUNTS.put("1", new Integer(119));
        EXPECT_PARTIITON_COUNTS.put("2", new Integer(127));
        EXPECT_PARTIITON_COUNTS.put("7", new Integer(141));
        EXPECT_PARTIITON_COUNTS.put("11", new Integer(118));
        EXPECT_PARTIITON_COUNTS.put("15", new Integer(137));
        EXPECT_PARTIITON_COUNTS.put("19", new Integer(110));
        EXPECT_PARTIITON_COUNTS.put("23", new Integer(119));
    }

    @Override
    public void setUp() throws Exception
    {
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();
    }

    public TestAllPartitionProcedureCalls(String name) {
        super(name);
    }

    public void testCallAllPartitionProcedures() throws Exception{

        Client client = getClient();

        try {
            load(client, "TABLE_INT_PARTITION");
            load(client, "TABLE_STRING_PARTITION");

            ClientResponseWithPartitionKey[]  responses = client.callAllPartitionProcedure("IntPartitionCallTestProc");
            validateResults(responses);

            client.callAllPartitionProcedure(new CallBack(), "IntPartitionCallTestProc");

            responses = client.callAllPartitionProcedure("StringPartitionCallTestProc");
            validateResults(responses);

            client.callAllPartitionProcedure(new CallBack(), "StringPartitionCallTestProc");

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void validateResults(ClientResponseWithPartitionKey[]  responses)
    {
        for (ClientResponseWithPartitionKey resp: responses) {
            VoltTable results = resp.m_response.getResults()[0];
            int expected = EXPECT_PARTIITON_COUNTS.get(resp.m_partitionKey.toString());
            assert(expected == results.fetchRow(0).getLong(0));
        }
    }

    private void load(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        for(int i = 0; i < 1000; i++) {
            StringBuilder builder = new StringBuilder();
            builder.append("insert into " + tableName + " values (" + i);
            builder.append(", 'foo" + i + "', " + i + ", " + i + ")");
            client.callProcedure("@AdHoc", builder.toString());
        }

        String sql = "SELECT count(*) from " + tableName;;
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assert(1000 == vt.fetchRow(0).getLong(0));
    }

    static public junit.framework.Test suite() throws Exception {
        return buildEnv();
    }

    static public MultiConfigSuiteBuilder buildEnv() throws Exception {

        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAllPartitionProcedureCalls.class);
        Map<String, String> additionalEnv = new HashMap<String, String>();
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(TestAllPartitionProcedureCalls.class.getResource("allpartitioncall.sql"));
        project.addProcedures(PROCEDURES);
        LocalCluster config = new LocalCluster("client-all-partitions.jar", 4, 2, 0, BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }

    public static class CallBack implements AllPartitionProcedureCallback {

        @Override
        public void clientCallback(ClientResponseWithPartitionKey[] clientResponse) throws Exception {

            if (clientResponse != null) {
                 for (ClientResponseWithPartitionKey resp: clientResponse) {
                    VoltTable results = resp.m_response.getResults()[0];
                    int expected = EXPECT_PARTIITON_COUNTS.get(resp.m_partitionKey);
                    assert(expected == results.fetchRow(0).getLong(0));
                }
            }
        }
    }
}
