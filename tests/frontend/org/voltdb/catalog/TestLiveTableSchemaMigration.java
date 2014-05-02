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

package org.voltdb.catalog;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.voltdb.ClientResponseImpl;
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
import org.voltdb.utils.MiscUtils;

public class TestLiveTableSchemaMigration extends TestCase {

    /**
     * Assuming given table has schema metadata, make a catalog containing
     * that table on disk.
     */
    String catalogPathForTable(VoltTable t, String jarname) throws IOException {
        CatalogBuilder builder = new CatalogBuilder();
        builder.addLiteralSchema(TableHelper.ddlForTable(t));
        String retval = Configuration.getPathToCatalogForTest(jarname);
        boolean success = builder.compile(retval);
        assertTrue(success);
        return retval;
    }

    /**
     * Assuming given tables have schema metadata, fill them with random data
     * and compare a pure-java schema migration with an EE schema migration.
     */
    void migrateSchema(VoltTable t1, VoltTable t2) throws Exception {
        ServerThread server = null;
        Client client = null;

        try {
            TableHelper.randomFill(t1, 1000, 1024, new Random(0));

            String catPath1 = catalogPathForTable(t1, "t1.jar");
            String catPath2 = catalogPathForTable(t2, "t2.jar");
            byte[] catBytes2 = MiscUtils.fileToBytes(new File(catPath2));

            DeploymentBuilder depBuilder = new DeploymentBuilder(1, 1, 0);
            depBuilder.setVoltRoot("/tmp/jhugg/foobar");
            String deployment = depBuilder.getXML();
            File deploymentFile = VoltProjectBuilder.writeStringToTempFile(deployment);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToDeployment = deploymentFile.getAbsolutePath();
            config.m_pathToCatalog = catPath1;
            config.m_ipcPort = 10000;
            //config.m_backend = BackendTarget.NATIVE_EE_IPC;
            server = new ServerThread(config);
            server.start();
            server.waitForInitialization();

            System.out.printf("PRE:  %s\n", TableHelper.ddlForTable(t1));
            System.out.printf("POST: %s\n", TableHelper.ddlForTable(t2));

            TableHelper.migrateTable(t1, t2);
            t2 = TableHelper.sortTable(t2);

            ClientConfig clientConfig = new ClientConfig();
            client = ClientFactory.createClient(clientConfig);
            client.createConnection("localhost");

            client.callProcedure("@LoadMultipartitionTable", "FOO", t1);

            ClientResponseImpl response = (ClientResponseImpl) client.callProcedure(
                    "@UpdateApplicationCatalog", catBytes2, deployment);
            System.out.println(response.toJSONString());

            VoltTable t3 = client.callProcedure("@AdHoc", "select * from FOO").getResults()[0];
            t3 = TableHelper.sortTable(t3);

            // compare the tables
            StringBuilder sb = new StringBuilder();
            if (!TableHelper.deepEqualsWithErrorMsg(t2, t3, sb)) {
                System.out.println("Table Mismatch");
                //System.out.printf("PRE:  %s\n", t2.toFormattedString());
                //System.out.printf("POST: %s\n", t3.toFormattedString());
                System.out.println(sb.toString());
                fail();
            }
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

    /**
     * Helper if you have quick schema, rather than tables.
     */
    void migrateSchema(String schema1, String schema2) throws Exception {
        VoltTable t1 = TableHelper.quickTable(schema1);
        VoltTable t2 = TableHelper.quickTable(schema2);

        migrateSchema(t1, t2);
    }

    /**
     * Test migrating between given schemas. Create random data and
     * migrate it in java, then compare the results with a real schema
     * change operation in the EE.
     */
    public void testFixedSchemas() throws Exception {
        // do nada
        migrateSchema("FOO (A:INTEGER, B:TINYINT)", "FOO (A:INTEGER, B:TINYINT)");

        // do nada with more schema
        migrateSchema("FOO (A:INTEGER-N/'28154', B:TINYINT/NULL, C:VARCHAR1690/NULL, " +
                      "CX:VARCHAR563-N/'mbZyuwvBzhMDvajcrmOFKeGOxgFm', D:FLOAT, E:TIMESTAMP, " +
                      "PKEY:BIGINT-N, F:VARCHAR24, G:DECIMAL, C4:TIMESTAMP-N/'1970-01-15 22:52:29.508000') P(PKEY)",
                      "FOO (A:INTEGER-N/'28154', B:TINYINT/NULL, C:VARCHAR1690/NULL, " +
                      "CX:VARCHAR563-N/'mbZyuwvBzhMDvajcrmOFKeGOxgFm', D:FLOAT, E:TIMESTAMP, " +
                      "PKEY:BIGINT-N, F:VARCHAR24, G:DECIMAL, C4:TIMESTAMP-N/'1970-01-15 22:52:29.508000') P(PKEY)");

        // try to add a column in front of a pkey
        migrateSchema("FOO (A:INTEGER) P(0)", "FOO (X:INTEGER, A:INTEGER) P(1)");

        // widen a pkey (a unique index)
        migrateSchema("FOO (X:INTEGER, A:INTEGER) P(0)", "FOO (X:INTEGER, A:INTEGER) P(0,1)");

        // base case of widening column
        migrateSchema("FOO (A:INTEGER, B:TINYINT) P(0)", "FOO (A:BIGINT, B:TINYINT) P(0)");
        migrateSchema("FOO (A:BIGINT, B:TINYINT, C:INTEGER) P(2,1)", "FOO (A:BIGINT, B:SMALLINT, C:INTEGER), P(2,1)");

        // string widening
        migrateSchema("FOO (A:BIGINT, B:VARCHAR12, C:INTEGER)", "FOO (A:BIGINT, B:VARCHAR24, C:INTEGER)");
        migrateSchema("FOO (A:BIGINT, B:VARCHAR100, C:INTEGER)", "FOO (A:BIGINT, B:VARCHAR120, C:INTEGER)");

        // even widen across inline/out-of-line boundaries
        migrateSchema("FOO (A:BIGINT, B:VARCHAR12, C:INTEGER)", "FOO (A:BIGINT, B:VARCHAR120, C:INTEGER)");
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

    /**
     * Create and mutate a bunch of random schemas and data in java,
     * then compare the mutated results with a schema change in the EE.
     *
     * The number of times the loop is run can be changed to make the test
     * better at the cost of runtime.
     */
    public void testRandomSchemas() throws Exception {
        int count = 15;
        Random rand = new Random(0);
        for (int i = 0; i < count; i++) {
            VoltTable t1 = TableHelper.getTotallyRandomTable("foo", rand);
            VoltTable t2 = TableHelper.mutateTable(t1, true, rand);
            migrateSchema(t1, t2);
            System.out.printf("testRandomSchemas tested %d/%d\n", i+1, count);
        }
    }
}
