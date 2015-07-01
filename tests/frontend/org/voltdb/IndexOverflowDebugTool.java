/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class IndexOverflowDebugTool extends TestCase {

    public void testSimple() throws Exception {
        String simpleSchema =
            "CREATE TABLE P1 (\n" +
            "  ID INTEGER DEFAULT '0' NOT NULL,\n" +
            "  TINY TINYINT NOT NULL,\n" +
            "  SMALL SMALLINT NOT NULL,\n" +
            "  BIG BIGINT NOT NULL,\n" +
            "  PRIMARY KEY (ID)\n" +
            ");\n" +
            "CREATE UNIQUE INDEX I1 ON P1 (ID, TINY);\n" +
            "\n" +
            "CREATE TABLE R1 (\n" +
            "  ID INTEGER DEFAULT '0' NOT NULL,\n" +
            "  TINY TINYINT NOT NULL,\n" +
            "  SMALL SMALLINT NOT NULL,\n" +
            "  BIG BIGINT NOT NULL,\n" +
            "  PRIMARY KEY (ID)\n" +
            ");";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addPartitionInfo("P1", "ID");
        builder.addStmtProcedure("nocrash", "select * from P1 where ID = 6000000000;");
        builder.addStmtProcedure("crash", "UPDATE P1 SET BIG = BIG + 4 WHERE P1.ID>= 5200704751286217677");
        builder.addStmtProcedure("crash2", "SELECT * FROM P1 WHERE P1.ID>-6611959682909750107");
        builder.addStmtProcedure("crash3", "SELECT * FROM P1 INNER JOIN R1 ON P1.ID = R1.BIG");
        builder.addStmtProcedure("crash4", "SELECT * FROM P1 WHERE P1.ID = 5 AND P1.TINY > -2000;");
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("indexoverflow.jar"), 1, 1, 0);
        assert(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("indexoverflow.xml"));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("indexoverflow.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("indexoverflow.xml");
        config.m_backend = BackendTarget.NATIVE_EE_IPC;
        config.m_ipcPort = 10001;
        ServerThread localServer = new ServerThread(config);
        localServer.start();
        localServer.waitForInitialization();

        ClientConfig clientConfig = new ClientConfig();
        Client client = ClientFactory.createClient(clientConfig);
        client.createConnection("127.0.0.1");

        client.callProcedure("P1.insert", 5, 5, 5, 5);
        ClientResponse cr = client.callProcedure("crash4");
        //client.callProcedure("@AdHoc", "UPDATE P1 SET BIG = BIG + 4 WHERE P1.ID>= 5200704751286217677");

        assert(cr.getStatus() == ClientResponse.SUCCESS);
        assert(cr.getResults().length == 1);
        assert(cr.getResults()[0].getRowCount() == 1);

        client.close();
        client = null;

        localServer.shutdown();
        localServer = null;
        System.gc();
    }

}
