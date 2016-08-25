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

    static Map<Integer, Long> INT_PARTITION_EXPECTED_COUNTS = new HashMap<Integer, Long>();
    static {
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(0), new Long(112));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(1), new Long(138));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(2), new Long(122));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(7), new Long(131));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(11), new Long(112));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(15), new Long(133));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(19), new Long(142));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(23), new Long(110));
    }

    static Map<Integer, Long> STRING_PARTITION_EXPECTED_COUNTS = new HashMap<Integer, Long>();
    static {
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(0), new Long(130));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(1), new Long(129));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(2), new Long(139));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(7), new Long(122));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(11), new Long(123));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(15), new Long(114));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(19), new Long(125));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(23), new Long(118));
    }

    static final String[] TENS = {"", " ten"," twenty", " thirty", " forty", " fifty", " sixty", " seventy", " eighty", " ninety"};
    static final String[] NUMS = {"", " one", " two", " three", " four", " five", " six", " seven", " eight", " nine", " ten", " eleven",
            " twelve", " thirteen", " fourteen", " fifteen", " sixteen", " seventeen", " eighteen", " nineteen" };

    /**
     * convert the number into its word form so that these numbers will be distributed among the partitions with string partition
     * differently from the distribution with integer partition.
     * @param number  a number less than 1000
     * @return  the word form of the number
     */
    private  static String convert(int number) {

        if (number == 0) {
            return "zero";
        }

        String soFar;
        if (number % 100 < 20) {
            soFar = NUMS[number % 100];
            number /= 100;
        } else {
            soFar = NUMS[number % 10];
            number /= 10;

            soFar = TENS[number % 10] + soFar;
            number /= 10;
        }

        if (number == 0) {
            return soFar.trim();
        }
        return (NUMS[number] + " hundred" + soFar).trim();
    }

    @Override
    public void setUp() throws Exception
    {
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();
        Client client = getClient();
        load(client, "TABLE_INT_PARTITION");
        load(client, "TABLE_STRING_PARTITION");
    }

    public TestAllPartitionProcedureCalls(String name) {
        super(name);
    }

    public void testSyncCallAllPartitionProcedureWithIntPartition() throws Exception {

        Client client = getClient();
        ClientResponseWithPartitionKey[]  responses = client.callAllPartitionProcedure("PartitionIntegerTestProc");
        validateResults(responses, INT_PARTITION_EXPECTED_COUNTS);
     }

    public void testAsyncCallAllPartitionProcedureWithIntPartition() throws Exception {

        Client client = getClient();
        CallBack cb = new CallBack(INT_PARTITION_EXPECTED_COUNTS);
        client.callAllPartitionProcedure(cb, "PartitionIntegerTestProc");
        Thread.sleep(2000);
        assertTrue(cb.m_callbackInvoked);
     }

    public void testSyncCallAllPartitionProcedureWithStringPartition() throws Exception {

        Client client = getClient();
        ClientResponseWithPartitionKey[]  responses = client.callAllPartitionProcedure("PartitionStringTestProc");
        validateResults(responses, STRING_PARTITION_EXPECTED_COUNTS);
    }

    public void testAsyncCallAllPartitionProcedureWithStringPartition() throws Exception{

        Client client = getClient();
        CallBack cb = new CallBack(STRING_PARTITION_EXPECTED_COUNTS);
        client.callAllPartitionProcedure(cb, "PartitionStringTestProc");
        Thread.sleep(2000);
        assertTrue(cb.m_callbackInvoked);
    }

    public void testCallAllPartitionProcedureFailuerProc() throws Exception {

        Client client = getClient();
        ClientResponseWithPartitionKey[]  responses = client.callAllPartitionProcedure("PartitionFailureTestProc");
        for (ClientResponseWithPartitionKey resp: responses) {
            int key = (int)(resp.getPartitionKey());
            if (key == 7) {
                 assertTrue(resp.getResponse().getStatus() == 1);
            } else {
                 assertFalse(resp.getResponse().getStatus() == 1);
            }
        }
    }


    private void validateResults(ClientResponseWithPartitionKey[]  responses, Map<Integer, Long> expectedCounts) {
        for (ClientResponseWithPartitionKey resp: responses) {
            VoltTable results = resp.getResponse().getResults()[0];
            Long expected = expectedCounts.get(resp.getPartitionKey());
            assert(expected != null);
            assertTrue(expected.longValue() == results.fetchRow(0).getLong(0));
        }
    }

    private void load(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {

        for (int i = 0; i < 1000; i++) {
            StringBuilder builder = new StringBuilder();
            builder.append("insert into " + tableName + " values (" + i);
            builder.append(", '" + convert(i) + "', " + i + ", " + i + ")");
            client.callProcedure("@AdHoc", builder.toString());
        }

        String sql = "SELECT count(*) from " + tableName;
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertTrue(1000 == vt.fetchRow(0).getLong(0));
    }

    static public junit.framework.Test suite() throws Exception {

        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAllPartitionProcedureCalls.class);
        Map<String, String> additionalEnv = new HashMap<String, String>();
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(TestAllPartitionProcedureCalls.class.getResource("allpartitioncall.sql"));
        project.addPartitionInfo("TABLE_INT_PARTITION", "value_number1");
        project.addPartitionInfo("TABLE_STRING_PARTITION", "value_string");

        project.addProcedures(new VoltProjectBuilder.ProcedureInfo(PartitionIntegerTestProc.class,
                "TABLE_INT_PARTITION.value_number1"),
                new VoltProjectBuilder.ProcedureInfo(PartitionStringTestProc.class,
                        "TABLE_STRING_PARTITION.value_string"),
                new VoltProjectBuilder.ProcedureInfo(PartitionFailureTestProc.class,
                        "TABLE_INT_PARTITION.value_number1")
                );

        LocalCluster config = new LocalCluster("client-all-partitions.jar", 4, 2, 0, BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }

    public static class CallBack implements AllPartitionProcedureCallback {

        Map<Integer, Long> m_expectedCounts;
        boolean m_callbackInvoked = false;
        CallBack(Map<Integer, Long> expectedCounts) {
            m_expectedCounts = expectedCounts;
        }

        @Override
        public void clientCallback(ClientResponseWithPartitionKey[] clientResponse) throws Exception {
            m_callbackInvoked = true;
            if (clientResponse != null) {
                 for (ClientResponseWithPartitionKey resp: clientResponse) {
                    VoltTable results = resp.getResponse().getResults()[0];
                    Long expected = m_expectedCounts.get(resp.getPartitionKey());
                    assertTrue(expected != null);
                    assertTrue(expected.longValue() == results.fetchRow(0).getLong(0));
                }
            }
        }
    }
}
