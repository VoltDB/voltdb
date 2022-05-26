/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import org.voltdb.ClientResponseImpl;
import org.voltdb.ServerThread;
import org.voltdb.TableHelper;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

import junit.framework.TestCase;

public class TestLiveTableSchemaMigration extends TestCase {

    /**
     * Assuming given table has schema metadata, make a catalog containing
     * that table on disk.
     */
    String catalogPathForTable(VoltTable t, String jarname) throws IOException {
        CatalogBuilder builder = new CatalogBuilder();
        String ddl = TableHelper.ddlForTable(t, false);
        builder.addLiteralSchema(ddl);
        String retval = Configuration.getPathToCatalogForTest(jarname);
        boolean success = builder.compile(retval);
        // good spot below for a breakpoint if compiling fails
        if (!success) {
            fail();
        }
        return retval;
    }

    void migrateSchema(VoltTable t1, VoltTable t2) throws Exception {
        migrateSchema(t1, t2, true);
    }

    /**
     * Assuming given tables have schema metadata, fill them with random data
     * and compare a pure-java schema migration with an EE schema migration.
     */
    void migrateSchema(VoltTable t1, VoltTable t2, boolean withData) throws Exception {
        ServerThread server = null;
        Client client = null;
        TableHelper helper = new TableHelper();

        try {
            if (withData) {
                helper.randomFill(t1, 1000, 1024);
            }

            String catPath1 = catalogPathForTable(t1, "t1.jar");
            String catPath2 = catalogPathForTable(t2, "t2.jar");
            byte[] catBytes2 = MiscUtils.fileToBytes(new File(catPath2));

            DeploymentBuilder depBuilder = new DeploymentBuilder(1, 1, 0);
            depBuilder.setVoltRoot("/tmp/rootbar");
            // disable logging
            depBuilder.configureLogging("/tmp/foobar", "/tmp/goobar", false, false, 1, 1, 3);
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

            System.out.printf("PRE:  %s\n", TableHelper.ddlForTable(t1, false));
            System.out.printf("POST: %s\n", TableHelper.ddlForTable(t2, false));

            ClientConfig clientConfig = new ClientConfig();
            client = ClientFactory.createClient(clientConfig);
            client.createConnection("localhost");

            TableHelper.loadTable(client, t1);

            ClientResponseImpl response = (ClientResponseImpl) client.callProcedure(
                    "@UpdateApplicationCatalog", catBytes2, null);
            System.out.println(response.toJSONString());

            VoltTable t3 = client.callProcedure("@AdHoc", "select * from FOO").getResults()[0];
            t3 = TableHelper.sortTable(t3);

            // compute the migrated table entirely in Java for comparison purposes
            TableHelper.migrateTable(t1, t2);
            t2 = TableHelper.sortTable(t2);

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
     * Assuming given tables have schema metadata, fill them with random data
     * and compare a pure-java schema migration with an EE schema migration.
     */
    void migrateSchemaUsingAlter(VoltTable t1, VoltTable t2, boolean withData)
            throws Exception
    {
        ServerThread server = null;
        Client client = null;
        TableHelper helper = new TableHelper();

        try {
            String alterText = TableHelper.getAlterTableDDLToMigrate(t1, t2);

            if (withData) {
                helper.randomFill(t1, 1000, 1024);
            }

            String catPath1 = catalogPathForTable(t1, "t1.jar");

            DeploymentBuilder depBuilder = new DeploymentBuilder(1, 1, 0);
            depBuilder.setVoltRoot("/tmp/rootbar");
            depBuilder.setUseDDLSchema(true);
            // disable logging
            depBuilder.configureLogging("/tmp/foobar", "/tmp/goobar", false, false, 1, 1, 3);
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

            System.out.printf("PRE:  %s\n", TableHelper.ddlForTable(t1, false));
            System.out.printf("POST: %s\n", TableHelper.ddlForTable(t2, false));

            TableHelper.migrateTable(t1, t2);
            t2 = TableHelper.sortTable(t2);

            ClientConfig clientConfig = new ClientConfig();
            client = ClientFactory.createClient(clientConfig);
            client.createConnection("localhost");

            TableHelper.loadTable(client, t1);

            if (alterText.trim().length() > 0) {
                ClientResponseImpl response =
                        (ClientResponseImpl) client.callProcedure("@AdHoc", alterText);
                System.out.println(response.toJSONString());
            }

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
        migrateSchema(schema1, schema2, true);
    }

    void migrateSchemaWithDataExpectFail(String schema1, String schema2, String pattern) throws Exception {
        try {
            migrateSchema(schema1, schema2);
            fail();
        }
        catch (ProcCallException e) {
            ClientResponseImpl cri = (ClientResponseImpl) e.getClientResponse();
            assertEquals(cri.getStatus(), ClientResponse.GRACEFUL_FAILURE);
            assertTrue(cri.getStatusString().contains(pattern));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Expected ProcCallException but got: " + e);
        }
    }

    /**
     * Try the old way (@UpdateApplicationCatalog) and try using ALTER TABLE
     */
    void migrateSchema(String schema1, String schema2, boolean withData) throws Exception {
        VoltTable t1 = TableHelper.quickTable(schema1);
        VoltTable t2 = TableHelper.quickTable(schema2);

        migrateSchema(t1, t2, withData);

        t1 = TableHelper.quickTable(schema1);
        t2 = TableHelper.quickTable(schema2);
        migrateSchemaUsingAlter(t1, t2, withData);
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
                      "PKEY:BIGINT-N, F:VARCHAR24, G:DECIMAL, C4:TIMESTAMP-N/'1970-01-15 22:52:29.508000') PK(PKEY)",
                      "FOO (A:INTEGER-N/'28154', B:TINYINT/NULL, C:VARCHAR1690/NULL, " +
                      "CX:VARCHAR563-N/'mbZyuwvBzhMDvajcrmOFKeGOxgFm', D:FLOAT, E:TIMESTAMP, " +
                      "PKEY:BIGINT-N, F:VARCHAR24, G:DECIMAL, C4:TIMESTAMP-N/'1970-01-15 22:52:29.508000') PK(PKEY)");

        // try to add a column in front of a pkey
        migrateSchema("FOO (A:INTEGER) PK(0)", "FOO (X:INTEGER, A:INTEGER) PK(1)");

        // widen a pkey (a unique index)
        migrateSchema("FOO (X:INTEGER, A:INTEGER) PK(0)", "FOO (X:INTEGER, A:INTEGER) PK(0,1)");

        // widen a pkey (a unique index) in a partitioned table
        migrateSchema("FOO (X:INTEGER-N, A:INTEGER) P(0) PK(0)", "FOO (X:INTEGER-N, A:INTEGER) P(0) PK(0,1)");

        // base case of widening column
        migrateSchema("FOO (A:INTEGER, B:TINYINT) PK(0)", "FOO (A:BIGINT, B:TINYINT) PK(0)");
        migrateSchema("FOO (A:BIGINT, B:TINYINT, C:INTEGER) PK(2,1)", "FOO (A:BIGINT, B:SMALLINT, C:INTEGER), PK(2,1)");

        // string widening
        migrateSchema("FOO (A:BIGINT, B:VARCHAR12, C:INTEGER)", "FOO (A:BIGINT, B:VARCHAR24, C:INTEGER)");
        migrateSchema("FOO (A:BIGINT, B:VARCHAR100, C:INTEGER)", "FOO (A:BIGINT, B:VARCHAR120, C:INTEGER)");

        // even widen across inline/out-of-line boundaries
        migrateSchema("FOO (A:BIGINT, B:VARCHAR12, C:INTEGER)", "FOO (A:BIGINT, B:VARCHAR120, C:INTEGER)");
        migrateSchema("FOO (VARCHAR12)", "FOO (VARCHAR120)");

        // same schema with a new name for middle col
        // we don't support renaming yet - this is testing drop and then add
        migrateSchema("FOO (A:BIGINT, B:TINYINT, C:INTEGER)", "FOO (A:BIGINT, D:TINYINT, C:INTEGER)");

        // adding a column with a default value
        migrateSchema("FOO (A:BIGINT)", "FOO (A:BIGINT, B:VARCHAR60/'This is spinal tap')");
        migrateSchema("FOO (A:BIGINT)", "FOO (A:BIGINT, B:VARCHAR120/'This is spinal tap')");

        // drop a column
        migrateSchema("FOO (A:BIGINT, B:TINYINT, C:INTEGER)", "FOO (B:SMALLINT, C:INTEGER)");

        // EXPECT FAIL change partitioned to replicated and back on an empty table
        migrateSchemaWithDataExpectFail("FOO (A:INTEGER-N, B:TINYINT) P(A)", "FOO (A:INTEGER-N, B:TINYINT)", "Unable to");
        migrateSchemaWithDataExpectFail("FOO (A:INTEGER-N, B:TINYINT)", "FOO (A:INTEGER-N, B:TINYINT) P(A)", "Unable to");

        // EXPECT FAIL change partition key on an empty table
        migrateSchemaWithDataExpectFail("FOO (A:INTEGER-N, B:TINYINT-N) P(A)", "FOO (A:INTEGER-N, B:TINYINT-N) P(B)", "Unable to");

        // EXPECT FAIL shrink a column
        migrateSchemaWithDataExpectFail("FOO (A:INTEGER-N, B:BIGINT)", "FOO (A:INTEGER-N, B:TINYINT)", "Unable to");
        migrateSchemaWithDataExpectFail("FOO (A:INTEGER-N, B:VARCHAR120)", "FOO (A:INTEGER-N, B:VARCHAR60)", "Unable to");

        // EXPECT FAIL change a column type
        migrateSchemaWithDataExpectFail("FOO (A:INTEGER-N, B:TINYINT) P(A)", "FOO (A:INTEGER-N, B:VARCHAR30) P(A)", "Unable to");
        migrateSchemaWithDataExpectFail("FOO (A:INTEGER-N, B:DECIMAL) P(A)", "FOO (A:INTEGER-N, B:FLOAT) P(A)", "Unable to");

        // EXPECT FAIL shrink a unique index
        migrateSchemaWithDataExpectFail("FOO (A:INTEGER-N, B:TINYINT-N) PK(A,B)", "FOO (A:INTEGER-N, B:TINYINT-N) PK(A)", "Unable to");
        migrateSchemaWithDataExpectFail("FOO (A:INTEGER-N, B:TINYINT-N) PK(A,B) P(B)", "FOO (A:INTEGER-N, B:TINYINT-N) PK(B) P(B)", "Unable to");
    }

    public void testFixedSchemasNoData() throws Exception {
        migrateSchema("FOO (A:INTEGER, B:TINYINT)", "FOO (A:INTEGER, B:TINYINT)", false);

        // change partitioned to replicated and back on an empty table
        migrateSchema("FOO (A:INTEGER-N, B:TINYINT) P(A)", "FOO (A:INTEGER-N, B:TINYINT)", false);
        migrateSchema("FOO (A:INTEGER-N, B:TINYINT)", "FOO (A:INTEGER-N, B:TINYINT) P(A)", false);

        // change partition key on an empty table
        migrateSchema("FOO (A:INTEGER-N, B:TINYINT-N) P(A)", "FOO (A:INTEGER-N, B:TINYINT-N) P(B)", false);

        // shrink a column
        migrateSchema("FOO (A:INTEGER-N, B:BIGINT)", "FOO (A:INTEGER-N, B:TINYINT)", false);
        migrateSchema("FOO (A:INTEGER-N, B:VARCHAR120)", "FOO (A:INTEGER-N, B:VARCHAR60)", false);

        // change a column type
        migrateSchema("FOO (A:INTEGER-N, B:TINYINT) P(A)", "FOO (A:INTEGER-N, B:VARCHAR30) P(A)", false);
        migrateSchema("FOO (A:INTEGER-N, B:DECIMAL) P(A)", "FOO (A:INTEGER-N, B:FLOAT) P(A)", false);

        // shrink a unique index
        migrateSchema("FOO (A:INTEGER-N, B:TINYINT-N) PK(A,B)", "FOO (A:INTEGER-N, B:TINYINT-N) PK(A)", false);
        migrateSchema("FOO (A:INTEGER-N, B:TINYINT-N) PK(A,B) P(B)", "FOO (A:INTEGER-N, B:TINYINT-N) PK(B) P(B)", false);

        // modify a unique index
        migrateSchema("FOO (A:INTEGER-N, B:TINYINT) PK(A,B)", "FOO (A:INTEGER-N, B:TINYINT) PK(B,A)", false);

        // add a unique index out of whole cloth
        migrateSchema("FOO (A:INTEGER-N, B:TINYINT)", "FOO (A:INTEGER-N, B:TINYINT) PK(A)", false);
        migrateSchema("FOO (A:INTEGER-N, B:TINYINT) P(A)", "FOO (A:INTEGER-N, B:TINYINT) P(A) PK(A)", false);
    }

    //
    // Create and mutate a bunch of random schemas and data in java,
    // then compare the mutated results with a schema change in the EE.
    //
    // The number of times the loop is run can be changed to make the test
    // better at the cost of runtime.
    //
    public void testRandomSchemas() throws Exception {
        int count = 15;
        TableHelper helper = new TableHelper();
        for (int i = 0; i < count; i++) {
            TableHelper.RandomTable trt = helper.getTotallyRandomTable("foo");
            VoltTable t1 = trt.table;
            VoltTable t2 = helper.mutateTable(t1, true);
            migrateSchema(t1, t2);
            System.out.printf("testRandomSchemas tested %d/%d\n", i+1, count);
        }
    }

    public void testRandomSchemasUsingAlter() throws Exception {
        int count = 15;
        TableHelper helper = new TableHelper();
        for (int i = 0; i < count; i++) {
            TableHelper.RandomTable trt = helper.getTotallyRandomTable("foo");
            VoltTable t1 = trt.table;
            VoltTable t2 = helper.mutateTable(t1, true);
            migrateSchemaUsingAlter(t1, t2, true);
            System.out.printf("testRandomSchemasUsingAlter tested %d/%d\n", i+1, count);
        }
    }
}
