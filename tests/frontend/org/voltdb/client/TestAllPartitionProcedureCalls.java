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
package org.voltdb.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;

/**
 *  Test client all partition calls
 *
 */
public class TestAllPartitionProcedureCalls extends JUnit4LocalClusterTest {

    static final int  ROWS = 1000;
    private LocalCluster cluster;
    private Client client;
    private Client clientWithAffinity;

    @Before
    public void setUp() throws Exception
    {
        FileUtils.deleteDirectory(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();

        cluster = new LocalCluster("client-all-partitions.jar", 4, 2, 0, BackendTarget.NATIVE_EE_JNI);
        cluster.overrideAnyRequestForValgrind();

        cluster.setHasLocalServer(false);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(TestAllPartitionProcedureCalls.class.getResource("allpartitioncall.sql"));

        boolean success = cluster.compile(project);
        assertTrue(success);
        cluster.startUp();

        ClientConfig config = new ClientConfig();
        client = ClientFactory.createClient(config);
        client.createConnection("", cluster.port(0));
        load(client, "TABLE_INT_PARTITION");
        load(client, "TABLE_STRING_PARTITION");

        clientWithAffinity = ClientFactory.createClient();
        clientWithAffinity.createConnection("", cluster.port(0));
    }

    @After
    public void tearDown() throws Exception {

        if(client != null){
            client.close();
        }

        if(clientWithAffinity != null){
            clientWithAffinity.close();
        }

        if (cluster != null) {
            cluster.shutDown();
        }
    }

    @Test
    public void testSyncCallAllPartitionProcedureWithIntPartition() throws Exception {

        ClientResponseWithPartitionKey[]  responses = client.callAllPartitionProcedure("PartitionIntegerTestProc");
        validateResults(responses, 8);

        responses = clientWithAffinity.callAllPartitionProcedure("PartitionIntegerTestProc");
        validateResults(responses, 8);
     }


    @Test
    public void testAsyncCallAllPartitionProcedureWithIntPartition() throws Exception {
        asyncTest(client, "PartitionIntegerTestProc");
        asyncTest(clientWithAffinity, "PartitionIntegerTestProc");
     }

    @Test
    public void testCallInvalidPartitionProcedure() throws Exception {
        ClientResponseWithPartitionKey[] responses;

        // check sysproc
        responses = client.callAllPartitionProcedure("@Statistics", "MEMORY");
        for (ClientResponseWithPartitionKey response : responses) {
            checkFail(response);
        }

        // check multipart
        responses = client.callAllPartitionProcedure("MultiPartitionProcedureSample", 0);
        for (ClientResponseWithPartitionKey response : responses) {
            checkFail(response);
        }

        // check wrong-partitioning
        responses = client.callAllPartitionProcedure("PartitionedTestProcNonZeroPartitioningParam", 0, 1);
        for (ClientResponseWithPartitionKey response : responses) {
            checkFail(response);
        }
    }

    private void checkFail(ClientResponseWithPartitionKey response) {
        assertEquals(ClientResponse.GRACEFUL_FAILURE, response.response.getStatus());
        String msg = response.response.getStatusString();
        assertTrue("wrong failure msg \""+msg+"\"", msg.contains("Invalid procedure for all-partition execution"));
    }

    @Test
    public void testSyncCallAllPartitionProcedureWithStringPartition() throws Exception {

        ClientResponseWithPartitionKey[]  responses = client.callAllPartitionProcedure("PartitionStringTestProc");
        validateResults(responses, 8);

        responses = clientWithAffinity.callAllPartitionProcedure("PartitionStringTestProc");
        validateResults(responses, 8);
    }

    private void asyncTest(Client cleint, String procName) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        CallBack cb = new CallBack(8, latch);
        client.callAllPartitionProcedure(cb, procName);
        try{
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAsyncCallAllPartitionProcedureWithStringPartition() throws Exception{
        asyncTest(client, "PartitionStringTestProc");
        asyncTest(clientWithAffinity, "PartitionStringTestProc");
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
    public void testCallAllPartitionProcedureWithNoPartitionKey() throws Exception {
        ClientResponseWithPartitionKey[] responses = client.callAllPartitionProcedure("PartitionedTestProc");
        validateResults(responses, 8);

        responses = clientWithAffinity.callAllPartitionProcedure("PartitionedTestProc");
        validateResults(responses, 8);
    }

    @Test
    public void testCallAllPartitionSQLProcedureWithNoPartitionKey() throws Exception {
        ClientResponseWithPartitionKey[] responses = client.callAllPartitionProcedure("PartitionedSQLTestProc");
        validateResults(responses, 8);

        responses = clientWithAffinity.callAllPartitionProcedure("PartitionedSQLTestProc");
        validateResults(responses, 8);
    }

    private void validateResults(ClientResponseWithPartitionKey[]  responses, int partitionCount) {
        assertNotNull("responses are null", responses);
        assertEquals("response array size is not equal to the number of partitions", partitionCount, responses.length);
        long total = 0;
        for (ClientResponseWithPartitionKey resp: responses) {
            byte status = resp.response.getStatus();
            String msg = resp.response.getStatusString();
            assertEquals("response failed, message \""+msg+"\"", ClientResponse.SUCCESS, status);
            VoltTable[] vta = resp.response.getResults();
            assertTrue("response has no table", vta.length > 0);
            VoltTable results = vta[0];
            long count = results.fetchRow(0).getLong(0);
            assertTrue(count > 0);
            total += count;
        }
        assertTrue(total == ROWS);
    }

    private void load(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {

        for (int i = 0; i < 1000; i++) {
            StringBuilder builder = new StringBuilder();
            builder.append("insert into " + tableName + " values (" + i);
            builder.append(", '" + i + "', " + i + ", " + i + ")");
            client.callProcedure("@AdHoc", builder.toString());
        }

        String sql = "SELECT count(*) from " + tableName;
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertTrue(1000 == vt.fetchRow(0).getLong(0));
    }

    public static class CallBack implements AllPartitionProcedureCallback {

        final int m_partitionCount;
        final CountDownLatch m_latch;
        CallBack(int partitionCount, CountDownLatch latch) {
            m_partitionCount = partitionCount;
            m_latch = latch;
        }

        @Override
        public void clientCallback(ClientResponseWithPartitionKey[] responses) throws Exception {
            assertEquals("response array size is not equal to the number of partitions", m_partitionCount, responses.length);
            long total = 0;
            try {
                for (ClientResponseWithPartitionKey resp: responses) {
                    VoltTable results = resp.response.getResults()[0];
                    long count = results.fetchRow(0).getLong(0);
                    total += count;
                    assertTrue(count > 0);
                }
                assertTrue(total == ROWS);
            } finally {
                m_latch.countDown();
            }
        }
    }
}
