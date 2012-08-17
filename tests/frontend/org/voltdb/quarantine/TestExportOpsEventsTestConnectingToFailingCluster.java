/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.quarantine;

import java.net.InetSocketAddress;

import org.voltdb.VoltDB;
import org.voltdb.exportclient.ExportClientException;
import org.voltdb.exportclient.TestExportOpsEvents;
import org.voltdb.exportclient.VoltDBFickleCluster;

public class TestExportOpsEventsTestConnectingToFailingCluster extends TestExportOpsEvents {

    public void testConnectingToFailingCluster() throws Exception {
        System.out.println("testConnectingToFailingCluster");
        NullExportClient client = new NullExportClient();
        client.addServerInfo(new InetSocketAddress("localhost", VoltDB.DEFAULT_PORT));

        VoltDBFickleCluster.start();

        client.addServerInfo(new InetSocketAddress(VoltDBFickleCluster.getPort(0)));
        assertTrue(client.connect());

        VoltDBFickleCluster.killNode();

        // work for 10 seconds, or until the connection is dropped
        long now = System.currentTimeMillis();
        while ((System.currentTimeMillis() - now) < 10000) {
            try {
                client.work();
                fail(); // this is supposed to throw
            }
            catch (ExportClientException e) {
                // this is supposed to happen
                break;
            }
        }

        client.disconnect();

        boolean connected = client.connect();
        if (!connected) {
            System.out.println("Couldn't reconnect");

            // Do the debug output a 2nd time to see if the status changes after the failure/time passes
            Process p = Runtime.getRuntime().exec("lsof -i");
            java.io.InputStreamReader reader = new java.io.InputStreamReader(p.getInputStream());
            java.io.BufferedReader br = new java.io.BufferedReader(reader);
            String str = null;
            while ((str = br.readLine()) != null) {
                if (str.contains("LISTEN")) {
                    System.out.println(str);
                }
            }
        }
        assertTrue(connected);
        client.disconnect();

        VoltDBFickleCluster.stop();
    }
}