/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb;

import java.io.File;
import java.net.URLEncoder;

import junit.framework.TestCase;

import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;

public class TestRejoinEndToEnd extends TestCase {

    public void testNothing() {

    }

    public void testRejoin() throws Exception {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "PRIMARY KEY(ival));";

        File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        String schemaPath = schemaFile.getPath();
        schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addSchema(schemaPath);
        builder.addPartitionInfo("blah", "ival");
        builder.addStmtProcedure("Insert", "insert into blah values (?);");
        boolean success = builder.compile("rejoin.jar", 1, 2, 1, "localhost");
        assertTrue(success);

        LocalCluster cluster = new LocalCluster("rejoin.jar", 1, 2, 1, BackendTarget.NATIVE_EE_JNI);
        cluster.compile(builder);
        cluster.setHasLocalServer(false);

        cluster.startUp();
        cluster.shutDownSingleHost(0);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = "rejoin.jar";
        config.m_rejoinToHostAndPort = "localhost";
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();

        Thread.sleep(100);

        localServer.shutdown();
        cluster.shutDown();

        /*VoltDB.Configuration config1 = new VoltDB.Configuration();
        config1.m_pathToCatalog = "rejoin.jar";
        ServerThread server1 = new ServerThread(config1);


        VoltDB.Configuration config2 = new VoltDB.Configuration();
        config2.m_pathToCatalog = "rejoin.jar";
        config2.m_port = config1.m_port + 1;
        ServerThread server2 = new ServerThread(config2);

        server1.start();
        // give server 1 a headstart so it will be the leader
        Thread.sleep(100);
        server2.start();

        server1.waitForInitialization();
        server2.waitForInitialization();

        server1.shutdown();
        server2.shutdown();

        server1.join();
        server2.join();*/
    }
}
