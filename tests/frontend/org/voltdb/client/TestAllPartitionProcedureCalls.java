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
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.utils.VoltFile;

import junit.framework.TestCase;

/**
 *  Test client all partition calls
 *
 */
public class TestAllPartitionProcedureCalls extends TestCase {

    static Map<Integer, Long> INT_PARTITION_EXPECTED_COUNTS = new HashMap<Integer, Long>();
    static {
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(0), new Long(114));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(1), new Long(128));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(2), new Long(130));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(7), new Long(131));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(11), new Long(102));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(15), new Long(133));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(19), new Long(142));
        INT_PARTITION_EXPECTED_COUNTS.put(new Integer(23), new Long(120));
    }

    static Map<Integer, Long> STRING_PARTITION_EXPECTED_COUNTS = new HashMap<Integer, Long>();
    static {
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(0), new Long(132));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(1), new Long(126));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(2), new Long(137));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(7), new Long(119));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(11), new Long(125));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(15), new Long(114));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(19), new Long(125));
        STRING_PARTITION_EXPECTED_COUNTS.put(new Integer(23), new Long(122));
    }

    static Map<Integer, Long> ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS = new HashMap<Integer, Long>();
    static {
        ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS.put(new Integer(0), new Long(78));
        ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS.put(new Integer(1), new Long(84));
        ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS.put(new Integer(2), new Long(101));
        ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS.put(new Integer(5), new Long(77));
        ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS.put(new Integer(7), new Long(98));
        ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS.put(new Integer(10), new Long(70));
        ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS.put(new Integer(11), new Long(70));
        ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS.put(new Integer(15), new Long(87));
        ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS.put(new Integer(19), new Long(100));
        ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS.put(new Integer(20), new Long(82));
        ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS.put(new Integer(24), new Long(72));
        ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS.put(new Integer(33), new Long(81));
    }

    static final String[] TENS = {"", " ten"," twenty", " thirty", " forty", " fifty", " sixty", " seventy", " eighty", " ninety"};
    static final String[] NUMS = {"", " one", " two", " three", " four", " five", " six", " seven", " eight", " nine", " ten", " eleven",
            " twelve", " thirteen", " fourteen", " fifteen", " sixteen", " seventeen", " eighteen", " nineteen" };

    private LocalCluster cluster;
    private Client client;
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

        if (TheHashinator.getConfiguredHashinatorType() != TheHashinator.HashinatorType.ELASTIC) return;
        cluster = new LocalCluster("client-all-partitions.jar", 4, 2, 0, BackendTarget.NATIVE_EE_JNI);
        cluster.overrideAnyRequestForValgrind();

        cluster.setHasLocalServer(false);
        cluster.setJavaProperty("ELASTIC_TOTAL_TOKENS", "128");
        cluster.setJavaProperty("ELASTIC_TARGET_THROUGHPUT", "10485760");
        cluster.setJavaProperty("ELASTIC_TARGET_TRANSFER_TIME_MS", "1000");

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

        boolean success = cluster.compile(project);
        assertTrue(success);
        cluster.startUp();

        ClientConfig config = new ClientConfig();
        config.setClientAffinity(false);
        client = ClientFactory.createClient(config);
        client.createConnection("", cluster.port(0));
        load(client, "TABLE_INT_PARTITION");
        load(client, "TABLE_STRING_PARTITION");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (cluster != null) {
            cluster.shutDown();
        }
        if(client != null){
            client.close();
        }
    }

    @Test
    public void testSyncCallAllPartitionProcedureWithIntPartition() throws Exception {

        ClientResponseWithPartitionKey[]  responses = client.callAllPartitionProcedure("PartitionIntegerTestProc");
        validateResults(responses, INT_PARTITION_EXPECTED_COUNTS);
     }

    @Test
    public void testAsyncCallAllPartitionProcedureWithIntPartition() throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        CallBack cb = new CallBack(INT_PARTITION_EXPECTED_COUNTS, latch);
        client.callAllPartitionProcedure(cb, "PartitionIntegerTestProc");
        try{
            latch.await();
        } catch (InterruptedException e) {

        }
     }

    @Test
    public void testSyncCallAllPartitionProcedureWithStringPartition() throws Exception {

        ClientResponseWithPartitionKey[]  responses = client.callAllPartitionProcedure("PartitionStringTestProc");
        validateResults(responses, STRING_PARTITION_EXPECTED_COUNTS);
    }

    @Test
    public void testAsyncCallAllPartitionProcedureWithStringPartition() throws Exception{
        CountDownLatch latch = new CountDownLatch(1);
        CallBack cb = new CallBack(STRING_PARTITION_EXPECTED_COUNTS, latch);
        client.callAllPartitionProcedure(cb, "PartitionStringTestProc");
       try{
           latch.await();
       } catch (InterruptedException e) {

       }
    }

    @Test
    public void testCallAllPartitionProcedureFailuerProc() throws Exception {
        ClientResponseWithPartitionKey[]  responses = client.callAllPartitionProcedure("PartitionFailureTestProc");
        for (ClientResponseWithPartitionKey resp: responses) {
            int key = (int)(resp.partitionKey);
            if (key == 7) {
                 assertTrue(resp.response.getStatus() == 1);
            } else {
                 assertFalse(resp.response.getStatus() == 1);
            }
        }
    }

    @Test
    public void testSyncCallAllPartitionProcedureWithElasticJoin() throws Exception {
        //add a new node, should get 12 partitions
        cluster.joinOne(2);
        Thread.sleep(5000);
        ClientResponseWithPartitionKey[] responses = client.callAllPartitionProcedure("PartitionIntegerTestProc");
        validateResults(responses, ELASTIC_JOIN_INT_PARTITION_EXPECTED_COUNTS);
    }

     private void validateResults(ClientResponseWithPartitionKey[]  responses, Map<Integer, Long> expectedCounts) {
        long total = 0;
        for (ClientResponseWithPartitionKey resp: responses) {
            VoltTable results = resp.response.getResults()[0];
            Long expected = expectedCounts.get(resp.partitionKey);
            assert(expected != null);
            long count = results.fetchRow(0).getLong(0);
            total += count;
            assertTrue(expected.longValue() == count);
        }
        assertTrue(total == 1000);
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
    public static class CallBack implements AllPartitionProcedureCallback {

        Map<Integer, Long> m_expectedCounts;
        final CountDownLatch m_latch;
        CallBack(Map<Integer, Long> expectedCounts, CountDownLatch latch) {
            m_expectedCounts = expectedCounts;
            m_latch = latch;
        }

        @Override
        public void clientCallback(ClientResponseWithPartitionKey[] clientResponse) throws Exception {
            long total = 0;
            try {
                for (ClientResponseWithPartitionKey resp: clientResponse) {
                    VoltTable results = resp.response.getResults()[0];
                    Long expected = m_expectedCounts.get(resp.partitionKey);
                    assertTrue(expected != null);
                    long count = results.fetchRow(0).getLong(0);
                    total += count;
                    assertTrue(expected.longValue() == count);
                }
                assertTrue(total == 1000);
            } finally {
                m_latch.countDown();
            }
        }
    }
}
