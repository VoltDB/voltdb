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

package org.voltdb.rejoin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.ClientResponseImpl;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;

public class LiveRejoinFailureTests extends JUnit4LocalClusterTest {

    public class SnapshotCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                System.out.println(((ClientResponseImpl) clientResponse).toJSONString());
                System.exit(-1);
            }
        }

    }

    public class SnapshotRequesterThread extends Thread {

        final Client m_client;

        public SnapshotRequesterThread(Client client) {
            m_client = client;
        }

        @Override
        public void run() {
            int i = 0;

            while (true) {
                try {
                    m_client.callProcedure(new SnapshotCallback(), "@SnapshotSave", "{uripath:\"file:///tmp\",nonce:\"manual" + i++ + "\",block:false}");
                } catch (NoConnectionsException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void testHaltLiveRejoinOnOverflow() throws Exception {

        LocalCluster cluster = null;
        Client client = null;
        ClientResponse cr;

        String simpleSchema =
                "CREATE TABLE P1 (\n" +
                "  ID BIGINT DEFAULT '0' NOT NULL,\n" +
                "  DATA VARCHAR(1048576) NOT NULL,\n" +
                "  PRIMARY KEY (ID)\n" +
                ");\n" +
                "CREATE INDEX I1 ON P1 (DATA);\n" +
                "CREATE INDEX I2 ON P1 (ID, DATA);\n" +
                "CREATE INDEX I3 ON P1 (DATA, ID);\n" +
                "PARTITION TABLE P1 ON COLUMN ID;\n";

        // build and compile a catalog
        System.out.println("Compiling catalog.");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.setSnapshotSettings("1s", 5, "/tmp/", "auto");
        cluster = new LocalCluster("liverejoinoverflow.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        cluster.setMaxHeap(4096);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        //File df = new File(cluster.getPathToDeployment());
        //df.

        System.out.println("Starting cluster.");
        cluster.setHasLocalServer(false);
        cluster.overrideAnyRequestForValgrind();
        cluster.startUp(true);

        System.out.println("Getting client connected.");
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProcedureCallTimeout(Long.MAX_VALUE);
        clientConfig.setMaxOutstandingTxns(100);
        client = ClientFactory.createClient(clientConfig);
        for (String address : cluster.getListenerAddresses()) {
            client.createConnection(address);
        }

        // insert token data to snapshot
        String data = "ABCDEFGHIJ";
        for (int i = 0; i < 16; i++) {
            data = data + data;
        }

        System.out.println("Loading");

        // load up > 1gb data
        for (int i = 0; i < 4000; i++) {
            if ((i % 400) == 0) {
                System.out.printf("%d%%\n", (i / 400) * 10);
            }
            cr = client.callProcedure("P1.insert", i, data);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        System.out.println("100% loaded. Killing node 0.");

        // kill a node
        cluster.killSingleHost(0);
        Thread.sleep(1000);

        //cr = client.callProcedure("@SnapshotSave", "{uripath:\"file:///tmp\",nonce:\"mydb\",block:false}");
        //assert(cr.getStatus() == ClientResponse.SUCCESS);
        //System.out.println(((ClientResponseImpl) cr).toJSONString());

        SnapshotRequesterThread srt = new SnapshotRequesterThread(client);
        srt.start();
        Thread.sleep(1500);

        System.out.println("Recovering node 0.");
        cluster.recoverOne(0, 1, true);

        System.out.println("Recovered.");

        //System.out.println("Recovering node 0.");
        //cluster.recoverOne(0, 1, "", true);

        Thread.sleep(1000000);

        client.drain();
        client.close();

        cluster.shutDown();
    }

}
