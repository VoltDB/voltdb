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

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;

import junit.framework.TestCase;

import org.voltdb.BackendTarget;
import org.voltdb.VoltDB;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.exportclient.ExportToFileClient.ExportToFileDecoder;
import org.voltdb.regressionsuites.LocalCluster;

public class TestExportToFileClient extends TestCase {

    public void testEng1088() throws Exception {
        ExportToFileClient exportClient =
            new ExportToFileClient(
                ',',
                "testnonce",
                new File("/tmp/" + System.getProperty("user.name")),
                60,
                "yyyyMMddHHmmss",
                null,
                0,
                false,
                false,
                false,
                0);
        AdvertisedDataSource source0 = TestExportDecoderBase.constructTestSource(0);
        AdvertisedDataSource source1 = TestExportDecoderBase.constructTestSource(1);
        ExportToFileDecoder decoder0 = exportClient.constructExportDecoder(source0);
        ExportToFileDecoder decoder1 = exportClient.constructExportDecoder(source1);
        assertEquals(decoder0, decoder1);
        decoder0.sourceNoLongerAdvertised(source1);
        decoder0.sourceNoLongerAdvertised(source0);
    }

    public void testNoAutoDiscovery() throws Exception {
        // clean up any files that exist
        File tmpdir = new File("/tmp");
        assertTrue(tmpdir.exists());
        assertTrue(tmpdir.isDirectory());
        FileFilter filter = new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getPath().contains("nadclient") && pathname.getPath().contains(".csv");
            }
        };
        File[] filesToDelete = tmpdir.listFiles(filter);
        for (File f : filesToDelete) {
            f.delete();
        }

        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "PRIMARY KEY(ival));";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addStmtProcedure("Insert", "insert into blah values (?);", null);
        builder.addPartitionInfo("blah", "ival");
        builder.addExport("org.voltdb.export.processors.RawProcessor", true, null);
        builder.setTableAsExportOnly("blah");

        LocalCluster cluster = new LocalCluster("exportAuto.jar",
                2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        cluster.setHasLocalServer(true);
        boolean success = cluster.compile(builder);
        assertTrue(success);
        cluster.startUp(true);

        final String listener = cluster.getListenerAddresses().get(0);
        final Client client = ClientFactory.createClient();
        client.createConnection(listener);
        client.callProcedure("Insert", 5);
        client.close();

        final ExportToFileClient exportClient1 =
                new ExportToFileClient(
                    ',',
                    "nadclient1",
                    new File("/tmp/"),
                    1,
                    "yyyyMMddHHmmss",
                    null,
                    6,
                    false,
                    false,
                    false,
                    0,
                    false);
        final ExportToFileClient exportClient2 =
                new ExportToFileClient(
                    ',',
                    "nadclient2",
                    new File("/tmp/"),
                    1,
                    "yyyyMMddHHmmss",
                    null,
                    6,
                    false,
                    false,
                    false,
                    0,
                    false);

        InetSocketAddress inetaddr1 = new InetSocketAddress("localhost", VoltDB.DEFAULT_PORT);
        InetSocketAddress inetaddr2 = new InetSocketAddress("localhost", VoltDB.DEFAULT_PORT + 1);

        exportClient1.addServerInfo(inetaddr1);
        exportClient2.addServerInfo(inetaddr2);

        // run both export clients for 5s
        Thread other = new Thread() {
            @Override
            public void run() {
                try {
                    exportClient1.run(5000);
                } catch (ExportClientException e) {
                    e.printStackTrace();
                }
            }
        };
        other.start();
        exportClient2.run(5000);
        other.join();

        cluster.shutDown();

        // compare the output files
        File[] filesToCompare = tmpdir.listFiles(filter);
        assertEquals(2, filesToCompare.length);
        assertEquals(filesToCompare[0].length(), filesToCompare[1].length());
        InputStream in1 = new FileInputStream(filesToCompare[0]);
        InputStream in2 = new FileInputStream(filesToCompare[1]);
        int b1 = 0, b2 = 0;
        do {
            b1 = in1.read();
            b2 = in2.read();
        } while ((b1 == b2) && (b1 != -1));
        assertEquals(-1, b1);
        in1.close();
        in2.close();
    }
}
