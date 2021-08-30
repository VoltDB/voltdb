/* This file is part of VoltDB.
 * Copyright (C) 2021 VoltDB Inc.
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.VoltFile;

import junit.framework.TestCase;

/**
 *  Test client all-partition calls
 *  for Client2 implementation
 */
public class TestClient2AllPartitionCall extends TestCase {

    static final int ROWS = 1000;
    private LocalCluster cluster;
    private Client2 client;

    @Override
    public void setUp() throws Exception
    {
        System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", getName());

        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();

        cluster = new LocalCluster("client-all-partitions.jar", 4, 2, 0, BackendTarget.NATIVE_EE_JNI);
        cluster.overrideAnyRequestForValgrind();
        cluster.setHasLocalServer(false);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(getClass().getResource("allpartitioncall.sql"));

        boolean success = cluster.compile(project);
        assertTrue(success);
        cluster.startUp();

        client = ClientFactory.createClient(new Client2Config());
        client.connectSync("localhost", cluster.port(0));
        load("TABLE_INT_PARTITION");
        load("TABLE_STRING_PARTITION");
    }

    private void load(String tableName) throws NoConnectionsException, IOException, ProcCallException {
        for (int i=0; i<ROWS; i++) {
            StringBuilder builder = new StringBuilder();
            builder.append("insert into " + tableName + " values (" + i);
            builder.append(", '" + i + "', " + i + ", " + i + ")");
            client.callProcedureSync("@AdHoc", builder.toString());
        }
        String sql = "SELECT count(*) from " + tableName;
        VoltTable vt = client.callProcedureSync("@AdHoc", sql).getResults()[0];
        assertEquals(ROWS, vt.fetchRow(0).getLong(0));
    }

    @Override
    public void tearDown() throws Exception {
        if (client != null){
            client.close();
        }
        if (cluster != null) {
            cluster.shutDown();
        }
        System.out.printf("=-=-=-=-=-=-= End of test %s =-=-=-=-=-=-=\n", getName());
    }

    /*
     * Test simple success cases
     */
    public void testSyncCallInt() throws Exception {
        ClientResponseWithPartitionKey[] responses = client.callAllPartitionProcedureSync(null, "PartitionIntegerTestProc");
        validateResults(responses, 8);
     }

    public void testAsyncCallInt() throws Exception {
        CompletableFuture<ClientResponseWithPartitionKey[]> future = client.callAllPartitionProcedureAsync(null, "PartitionIntegerTestProc");
        ClientResponseWithPartitionKey[] responses = future.get();
        validateResults(responses, 8);
     }

    public void testSyncCallString() throws Exception {
        ClientResponseWithPartitionKey[] responses = client.callAllPartitionProcedureSync(null, "PartitionStringTestProc");
        validateResults(responses, 8);
    }

    public void testAsyncCallString() throws Exception{
        CompletableFuture<ClientResponseWithPartitionKey[]> future = client.callAllPartitionProcedureAsync(null, "PartitionStringTestProc");
        ClientResponseWithPartitionKey[] responses = future.get();
        validateResults(responses, 8);
    }

    public void testNoPartitionKey() throws Exception {
        ClientResponseWithPartitionKey[] responses = client.callAllPartitionProcedureSync(null, "PartitionedTestProc");
        validateResults(responses, 8);
    }

    public void testSQLProcNoPartitionKey() throws Exception {
        ClientResponseWithPartitionKey[] responses = client.callAllPartitionProcedureSync(null, "PartitionedSQLTestProc");
        validateResults(responses, 8);
    }

    /*
     * Test some procs that are not all-partition
     */
    public void testInvalidPartition() throws Exception {
        ClientResponseWithPartitionKey[] responses;

        // check sysproc
        responses = client.callAllPartitionProcedureSync(null, "@Statistics", "MEMORY");
        for (ClientResponseWithPartitionKey response : responses) {
            checkFail(response);
        }

        // check multipart
        responses = client.callAllPartitionProcedureSync(null, "MultiPartitionProcedureSample", 0);
        for (ClientResponseWithPartitionKey response : responses) {
            checkFail(response);
        }

        // check wrong-partitioning
        responses = client.callAllPartitionProcedureSync(null, "PartitionedTestProcNonZeroPartitioningParam", 0, 1);
        for (ClientResponseWithPartitionKey response : responses) {
            checkFail(response);
        }
    }

    private void checkFail(ClientResponseWithPartitionKey response) {
        assertEquals(ClientResponse.GRACEFUL_FAILURE, response.response.getStatus());
        String msg = response.response.getStatusString();
        assertTrue("wrong failure msg \""+msg+"\"", msg.contains("Invalid procedure for all-partition execution"));
    }

    /*
     * Some but not all instances of procedure fail
     */
    public void testFailureProc() throws Exception {
        ClientResponseWithPartitionKey[] responses = client.callAllPartitionProcedureSync(null, "PartitionFailureTestProc");
        for (ClientResponseWithPartitionKey resp: responses) {
            int key = (int)(resp.partitionKey);
            if (key == 7) {
                assertEquals(1, resp.response.getStatus());
            } else {
                assertNotEquals(1, resp.response.getStatus());
            }
        }
    }

    private void validateResults(ClientResponseWithPartitionKey[] responses, int partitionCount) {
        assertNotNull("response array is null", responses);
        assertEquals("response array size is not equal to the number of partitions", partitionCount, responses.length);
        long total = 0;
        for (ClientResponseWithPartitionKey resp : responses) {
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
        assertEquals("total row count is not as expected", ROWS, total);
    }
}
