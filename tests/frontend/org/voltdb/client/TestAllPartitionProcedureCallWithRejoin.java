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

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;

public class TestAllPartitionProcedureCallWithRejoin {

    static final int  ROWS = 1000;
    @Test
    public void testSyncCallAllPartitionProcedureWithElasticJoin() throws Exception {
        if (TheHashinator.getConfiguredHashinatorType() != TheHashinator.HashinatorType.ELASTIC) return;
        LocalCluster cluster = new LocalCluster("client-all-partitions-rejoin.jar", 4, 2, 0, BackendTarget.NATIVE_EE_JNI);
        cluster.overrideAnyRequestForValgrind();

        cluster.setHasLocalServer(false);
        cluster.setJavaProperty("ELASTIC_TOTAL_TOKENS", "128");
        cluster.setJavaProperty("ELASTIC_TARGET_THROUGHPUT", "10485760");
        cluster.setJavaProperty("ELASTIC_TARGET_TRANSFER_TIME_MS", "1000");

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(TestAllPartitionProcedureCalls.class.getResource("allpartitioncall.sql"));
        project.addPartitionInfo("TABLE_INT_PARTITION", "value_number1");

        project.addProcedures(new VoltProjectBuilder.ProcedureInfo(PartitionIntegerTestProc.class,
                "TABLE_INT_PARTITION.value_number1")
                );

        boolean success = cluster.compile(project);
        assertTrue(success);
        cluster.startUp();

        ClientConfig config = new ClientConfig();
        config.setClientAffinity(false);
        Client client = ClientFactory.createClient(config);
        client.createConnection("", cluster.port(0));
        load(client, "TABLE_INT_PARTITION");

        Client clientWithAffinity = ClientFactory.createClient();
        clientWithAffinity.createConnection("", cluster.port(0));

        Thread.sleep(1000);
        //add a new node, should get 12 partitions
        cluster.joinOne(2);
        Thread.sleep(30000);
        ClientResponseWithPartitionKey[] responses = client.callAllPartitionProcedure("PartitionIntegerTestProc");
        validateResults(responses, 12);

        responses = clientWithAffinity.callAllPartitionProcedure("PartitionIntegerTestProc");
        validateResults(responses, 12);

        client.close();
        clientWithAffinity.close();
        cluster.shutDown();
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

    private void validateResults(ClientResponseWithPartitionKey[]  responses, int partitionCount) {
        assertTrue (responses.length == partitionCount);
        long total = 0;
        for (ClientResponseWithPartitionKey resp: responses) {
            VoltTable results = resp.response.getResults()[0];
            long count = results.fetchRow(0).getLong(0);
            assertTrue(count > 0);
            total += count;
        }
        assertTrue(total == ROWS);
    }
}
