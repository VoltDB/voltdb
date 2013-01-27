/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.catalog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import junit.framework.TestCase;

import org.voltdb.ServerThread;
import org.voltdb.TableHelper;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.CatalogUtil;

public class TestLiveTableSchemaMigration extends TestCase {

    String catalogPathForTable(VoltTable t, String jarname) throws IOException {
        CatalogBuilder builder = new CatalogBuilder();
        builder.addLiteralSchema(TableHelper.ddlForTable(t));
        System.out.println(TableHelper.ddlForTable(t));
        String retval = Configuration.getPathToCatalogForTest(jarname);
        boolean success = builder.compile(retval);
        assertTrue(success);
        return retval;
    }

    void migrateSchema(String schema1, String schema2) throws Exception {
        ServerThread server = null;
        Client client = null;

        try {
            VoltTable t1 = TableHelper.quickTable(schema1);
            VoltTable t2 = TableHelper.quickTable(schema2);

            TableHelper.randomFill(t1, 1000, 1024, new Random(0));
            TableHelper.migrateTable(t1, t2);

            String catPath1 = catalogPathForTable(t1, "t1.jar");
            String catPath2 = catalogPathForTable(t2, "t2.jar");
            byte[] catBytes2 = CatalogUtil.toBytes(new File(catPath2));

            DeploymentBuilder depBuilder = new DeploymentBuilder();
            String deployment = depBuilder.getXML("/tmp/jhugg/foobar");
            File deploymentFile = VoltProjectBuilder.writeStringToTempFile(deployment);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToDeployment = deploymentFile.getAbsolutePath();
            config.m_pathToCatalog = catPath1;
            config.m_ipcPorts = new ArrayList<Integer>();
            config.m_ipcPorts.add(10000);
            //config.m_backend = BackendTarget.NATIVE_EE_IPC;
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            ClientConfig clientConfig = new ClientConfig();
            client = ClientFactory.createClient(clientConfig);
            client.createConnection("localhost");

            client.callProcedure("@LoadMultipartitionTable", "FOO", t1);

            client.callProcedure("@UpdateApplicationCatalog", catBytes2, deployment);

            VoltTable t3 = client.callProcedure("@AdHoc", "select * from FOO").getResults()[0];

            assert(t3.hasSameContents(t2));
        }
        finally {
            if (client != null) {
                client.close();
            }
            if (server != null) {
                server.shutdown();
            }
        }
    }

    public void testBasicnessitude() throws Exception {
        // base case of widening column
        /*migrateSchema("FOO (A:BIGINT, B:TINYINT, C:INTEGER)", "FOO (A:BIGINT, B:SMALLINT, C:INTEGER)");

        // string widening
        migrateSchema("FOO (A:BIGINT, B:VARCHAR12, C:INTEGER)", "FOO (A:BIGINT, B:VARCHAR24, C:INTEGER)");
        migrateSchema("FOO (A:BIGINT, B:VARCHAR100, C:INTEGER)", "FOO (A:BIGINT, B:VARCHAR120, C:INTEGER)");
        migrateSchema("FOO (A:BIGINT, B:VARCHAR12, C:INTEGER)", "FOO (A:BIGINT, B:VARCHAR120, C:INTEGER)");*/

        migrateSchema("FOO (VARCHAR12)", "FOO (VARCHAR120)");

        // same schema with a new name for middle col
        migrateSchema("FOO (A:BIGINT, B:TINYINT, C:INTEGER)", "FOO (A:BIGINT, D:TINYINT, C:INTEGER)");

        // adding a column with a default value
        migrateSchema("FOO (A:BIGINT)", "FOO (A:BIGINT, B:VARCHAR60/'This is spinal tap')");
        migrateSchema("FOO (A:BIGINT)", "FOO (A:BIGINT, B:VARCHAR120/'This is spinal tap')");

        // drop a column
        migrateSchema("FOO (A:BIGINT, B:TINYINT, C:INTEGER)", "FOO (B:SMALLINT, C:INTEGER)");

        // reordering columns
        migrateSchema("FOO (A:BIGINT, B:TINYINT, C:INTEGER)", "FOO (C:INTEGER, A:BIGINT, B:TINYINT)");
    }
}
