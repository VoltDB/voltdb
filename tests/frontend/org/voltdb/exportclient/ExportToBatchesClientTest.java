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
import java.util.Timer;
import java.util.TimerTask;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class ExportToBatchesClientTest {

    public static void main(String[] args) throws Exception {

        // compile a voltdb app
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("create table blah (ival bigint not null, sval varchar(255) not null);");
        builder.addPartitionInfo("blah", "ival");
        builder.addStmtProcedure("Insert", "insert into blah values (?, ?);", "blah.ival: 0");
        builder.setTableAsExportOnly("blah");
        builder.addExport("org.voltdb.export.processors.RawProcessor", true, null);
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("sqexport.jar"), 1, 1, 0);
        if (!success) {
            System.err.println("Failed to compile");
            System.exit(-1);
        }
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("sqexport.xml"));

        // start a voltdb server
        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("sqexport.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("sqexport.xml");
        ServerThread localServer = new ServerThread(config);
        localServer.start();
        localServer.waitForInitialization();

        // create a client
        final Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        // do inserts every second
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            int counter = 0;
            @Override
            public void run() {
                try {
                    System.out.printf("Inserting a tuple from the client with value %d\n", counter + 1);
                    String second = "你好";
                    if ((counter % 2) == 0) second = "newline\nnewline";
                    ClientResponse response = client.callProcedure("Insert", counter++, second);
                    assert(response.getStatus() == ClientResponse.SUCCESS);
                }
                catch (Exception e) { e.printStackTrace(); System.exit(-1); }
            }
        };
        timer.scheduleAtFixedRate(task, 1000, 1000);

        // start the voltdb export client for sq
        File dir = new File(System.getProperty("user.dir") + File.separator + "00_exportout");
        System.out.printf("Working dir is %s\n", dir.getPath());
        ExportToFileClient exportClient = new ExportToFileClient(
                ',', "testy", dir, 1, "yyyyMMddHHmmss", null, 0, false, true, true, 0);
        //ExportToFileClient exportClient = new ExportToFileClient(',', "testy", dir, 1, "yyyyMMddHHmmss", 0, false);
        //DiscardingExportClient exportClient = new DiscardingExportClient(false);
        exportClient.addServerInfo("localhost", false);
        exportClient.addCredentials("", "");

        exportClient.run();
    }

}
