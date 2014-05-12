/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdhocDDL extends TestCase {

    public void testDropTable() throws Exception {

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhoc_like.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhoc_like.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
            "create table BLAH (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PRIMARY KEY(ID));\n" +
            "create table DROPME (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PRIMARY KEY(ID));");
        builder.addPartitionInfo("BLAH", "ID");
        builder.addPartitionInfo("DROPME", "ID");
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        ServerThread localServer = new ServerThread(config);

        Client client = null;

        try {
            localServer.start();
            localServer.waitForInitialization();

            client = ClientFactory.createClient();
            client.createConnection("localhost");

            //client.callProcedure("@AdHoc", "select * from BLAH; drop table DROPME;");
            client.callProcedure("@AdHoc", "drop table DROPME;");
        }
        finally {
            if (client != null) client.close();
            client = null;

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;

            // no clue how helpful this is
            System.gc();
        }
    }
}
