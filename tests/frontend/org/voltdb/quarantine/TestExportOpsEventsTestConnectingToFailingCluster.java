/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import junit.framework.TestCase;

import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportClientException;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.VoltDBFickleCluster;

public class TestExportOpsEventsTestConnectingToFailingCluster extends TestCase {

    @Override
    protected void setUp() throws Exception {
        VoltDBFickleCluster.compile();
    }

    class NullExportClient extends ExportClientBase {
        public class TrivialDecoder extends ExportDecoderBase {
            public TrivialDecoder(AdvertisedDataSource source) {
                super(source);
            }
            @Override
            public boolean processRow(int rowSize, byte[] rowData) {
                return true;
            }
            @Override
            public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
                // TODO Auto-generated method stub

            }
        }
        @Override
        public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
            return new TrivialDecoder(source);
        }
    }

    public void testConnectingToNothing() throws ExportClientException {
        System.out.println("testConnectToNothing");
        NullExportClient client = new NullExportClient();
        client.addServerInfo(new InetSocketAddress("localhost", 21212));

        // the first connect should return false, but shouldn't
        // throw and exception
        assertFalse(client.connect());

        // now run for a while...
        // it should run for 1.5 seconds without failing, but also without connecting
        client.run(1500);
    }

    public void testConnectingToFailingCluster() throws Exception {
        System.out.println("testConnectingToFailingCluster");
        NullExportClient client = new NullExportClient();
        client.addServerInfo(new InetSocketAddress("localhost", 21212));

        VoltDBFickleCluster.start();

        assertTrue(client.connect());

        VoltDBFickleCluster.killNode();

        // work for 10 seconds, or until the connection is dropped
        long now = System.currentTimeMillis();
        boolean success = false;
        while ((System.currentTimeMillis() - now) < 10000) {
            try {
                client.work();
            }
            catch (ExportClientException e) {
                // this is supposed to happen
                success = true;
                break;
            }
        }
        assertTrue(success);

        client.disconnect();

        // Debug code added to make sure the server is listening on the client port.
        // On the mini we were seeing that the export client couldn't reconnect
        // and got connection refused which doesn't make any sense. Adding this
        // debug output changed the timing so it wouldn't show up. Leave it in to make
        // the test pass more consistently, and give us more info if it does end up failing again.
        {
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