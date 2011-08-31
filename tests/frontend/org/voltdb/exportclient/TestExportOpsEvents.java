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

package org.voltdb.exportclient;

import java.net.InetSocketAddress;

import junit.framework.TestCase;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.utils.MiscUtils;

public class TestExportOpsEvents extends TestCase {

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

    public void testConnectingToExportDisabledServer() throws Exception {
        System.out.println("testConnectingToExportDisabledServer");

        // compile a trivial voltdb catalog/deployment (no export)
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("create table blah (ival bigint default 0 not null, PRIMARY KEY(ival));");
        builder.addStmtProcedure("Insert", "insert into blah values (?);", null);
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("disabled-export.jar"), 1, 1, 0);
        assert(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("disabled-export.xml"));

        // start voltdb server (no export)
        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("disabled-export.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("disabled-export.xml");
        ServerThread localServer = new ServerThread(config);
        localServer.start();
        localServer.waitForInitialization();

        NullExportClient client = new NullExportClient();
        client.addServerInfo(new InetSocketAddress("localhost", 21212));

        // the first connect should return false, but shouldn't
        // throw and exception
        try {
            client.connect();
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Export is not enabled"));
        }

        localServer.shutdown();
        localServer.join();
    }

    public void testConnectingToLateServer() throws Exception {
        System.out.println("testConnectingToLateServer");
        NullExportClient client = new NullExportClient();
        client.addServerInfo(new InetSocketAddress("localhost", 21212));

        // the first connect should return false, but shouldn't
        // throw and exception
        assertFalse(client.connect());

        VoltDBFickleCluster.start();

        assertTrue(client.connect());
        client.disconnect();
        VoltDBFickleCluster.stop();
    }
}