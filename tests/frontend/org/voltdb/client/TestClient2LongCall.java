/* This file is part of VoltDB.
 * Copyright (C) 2021-2022 Volt Active Data Inc.
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

package org.voltdb.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.concurrent.TimeUnit;
import java.util.Random;

import org.voltdb.ServerThread;
import org.voltdb.TableHelper;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;

/**
 * This test exercises the strange case, inherited from
 * the older client implementation, that known long-running
 * procedure calls are exempted from the usual timeout
 * consideration.
 */
public class TestClient2LongCall {

    static VoltDB.Configuration serverConfig;
    static DeploymentBuilder depBuilder;

    @Rule
    public final TestName testname = new TestName();

    @BeforeClass
    public static void prologue() {
        try {
            System.out.println("=-=-=-= Prologue =-=-=-=");

            CatalogBuilder catBuilder = new CatalogBuilder();
            catBuilder.addSchema(TestClient2LongCall.class.getResource("clientfeatures.sql"));
            catBuilder.addProcedures(ArbitraryDurationProc.class);

            boolean success = catBuilder.compile(Configuration.getPathToCatalogForTest("timeouts.jar"));
            assertTrue("bad catalog", success);

            depBuilder = new DeploymentBuilder(1, 1, 0);
            depBuilder.writeXML(Configuration.getPathToCatalogForTest("timeouts.xml"));

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("timeouts.jar");
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("timeouts.xml");
            serverConfig = config;
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        // Note: we have a new server for each test because otherwise
        // timeout testing causes cross-talk between tests, because
        // tests remain queued in the server after test completion.
        // Although currently there's only one test, so this is moot.
    }

    @AfterClass
    public static void epilogue() {
        System.out.println("=-=-=-= Epilogue =-=-=-=");
    }

    ServerThread localServer;

    @Before
    public void setup() {
        System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", testname.getMethodName());
        ServerThread.resetUserTempDir();
        localServer = new ServerThread(serverConfig);
        localServer.start();
        localServer.waitForInitialization();
    }

    @After
    public void teardown() throws Exception {
        localServer.shutdown();
        localServer.join();
        localServer = null;
        System.out.printf("=-=-=-=-=-=-= End of test %s =-=-=-=-=-=-=\n", testname.getMethodName());
    }

    /**
     * Test special exception for slow snapshots or catalogs updates
     * Both features are pro only
     */
    @Test
    public void testLongCallNoTimeout() throws Exception {

        // build a catalog with a ton of indexes so catalog update will be slow
        CatalogBuilder builder = new CatalogBuilder();
        builder.addSchema(getClass().getResource("clientfeatures-wellindexed.sql"));
        byte[] catalogToUpdate = builder.compileToBytes();
        assertNotNull(catalogToUpdate);

        // make a copy of the table from ddl for loading
        // (shouldn't have to do this, but for now, the table loader requires
        //  a VoltTable, and can't read schema. Could fix by using this VoltTable
        //  to generate schema or by teaching to loader how to discover tables)
        TableHelper.Configuration helperConfig = new TableHelper.Configuration();
        helperConfig.rand = new Random();
        TableHelper helper = new TableHelper(helperConfig);
        VoltTable t = TableHelper.quickTable("indexme (pkey:bigint, " +
                                             "c01:varchar63, " +
                                             "c02:varchar63, " +
                                             "c03:varchar63, " +
                                             "c04:varchar63, " +
                                             "c05:varchar63, " +
                                             "c06:varchar63, " +
                                             "c07:varchar63, " +
                                             "c08:varchar63, " +
                                             "c09:varchar63, " +
                                             "c10:varchar63) " +
                                             "PKEY(pkey)");

        // get a client with a normal timeout
        // uses old client for now; this is not the test
        Client clientX = ClientFactory.createClient();
        clientX.createConnection("localhost");
        helper.fillTableWithBigintPkey(t, 400, 0, clientX, 0, 1);

        // Now connect the short-timeout client we're testing
        System.out.println("Connecting...");
        Client2Config config = new Client2Config()
            .procedureCallTimeout(500, TimeUnit.MILLISECONDS);
        Client2 client = ClientFactory.createClient(config);
        client.connectSync("localhost");

        // run a catalog update that *might* normally timeout
        System.out.println("Calling @UpdateApplicationCatalog...");
        long start = System.nanoTime();
        ClientResponse response = client.callProcedureSync("@UpdateApplicationCatalog", catalogToUpdate, depBuilder.getXML());
        double duration = (System.nanoTime() - start) / 1_000_000_000.0;
        System.out.printf("Catalog update duration: %.2f sec\n", duration);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        // run a blocking snapshot that *might* normally timeout
        System.out.println("Calling @SnapshotSave...");
        start = System.nanoTime();
        response = client.callProcedureSync("@SnapshotSave", Configuration.getPathToCatalogForTest(""), "slow", 1);
        duration = (System.nanoTime() - start) / 1_000_000_000.0;
        System.out.printf("Snapshot save duration: %.2f sec\n", duration);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
    }
}
