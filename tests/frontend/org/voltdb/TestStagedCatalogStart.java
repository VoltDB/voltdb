/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Test;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.CatalogUtil;

final public class TestStagedCatalogStart {

    // TODO - restructure this into separate tests on a single init'd cluster

    /** Tests that a cluster can start with each node using an identical staged catalog.
     * Since durability is off, the staged catalog will be kept around but not reflect any DDL changes.
     * Restarting the database will reload the previous staged catalog.
     */
    @Test
    public void testStartWithStagedCatalogNoDurability() throws Exception {

        final String schema =
                "create table HELLOWORLD (greeting varchar(30), greeterid integer, PRIMARY KEY(greeting));\n" +
                "create procedure from class org.voltdb.compiler.procedures.SelectStarHelloWorld;";
        final int siteCount = 1;
        final int hostCount = 3;
        final int kfactor = 0;
        LocalCluster cluster = LocalCluster.createLocalClusterViaStagedCatalog(schema, null, siteCount, hostCount, kfactor, null);

        System.out.println("First start up is expected to succeed");
        boolean clearLocalDataDirectories = false;
        boolean skipInit = false;
        cluster.startUp(clearLocalDataDirectories, skipInit);

        for (int i = 0; i < hostCount; i++) {
            System.out.printf("Local cluster node[%d] ports: internal=%d, admin=%d, client=%d\n",
                              i, cluster.internalPort(i), cluster.adminPort(i), cluster.port(i));
        }

        System.out.println("Verifying schema and classes are present");
        Client client = ClientFactory.createClient();
        client.createConnection("localhost", cluster.port(0));
        ClientResponse response;
        response = client.callProcedure("@AdHoc", "INSERT INTO HELLOWORLD VALUES ('Hello, world!', 1);");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("SelectStarHelloWorld");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(1, response.getResults().length);
        assertEquals(1, response.getResults()[0].getRowCount());

        // Staged catalog will persist because durability is off, and being able to recover the schema is beneficial.
        int nodesWithStagedCatalog = cluster.countNodesWithFile(Constants.CONFIG_DIR + File.separator + CatalogUtil.STAGED_CATALOG_FILE_NAME);
        assertEquals(hostCount, nodesWithStagedCatalog);
        cluster.shutDown();

        System.out.println("Delete staged catalog from node 0; cluster must not let us restart.");
        final File root0StagedCatalog = new File(RealVoltDB.getStagedCatalogPath(cluster.getServerSpecificRoot("0")));
        assertTrue(root0StagedCatalog.delete());
        nodesWithStagedCatalog = cluster.countNodesWithFile(Constants.CONFIG_DIR + File.separator + CatalogUtil.STAGED_CATALOG_FILE_NAME);
        assertEquals(hostCount - 1, nodesWithStagedCatalog);

        // Make sure cluster can't be restarted with a missing staged catalog
        clearLocalDataDirectories = false;
        skipInit = true;
        cluster.setExpectedToCrash(true);
        cluster.startUp(clearLocalDataDirectories, skipInit);
        // FIXME how to verify the error message is as expected? It is, but test doesn't know it is.

        System.out.println("Sneak in a staged catalog that has a different schema; cluster must not let us restart.");
        final String differentSchema = "create table HELLOWORLD (greeting varchar(30), greeterid integer, PRIMARY KEY(greeterid));\n" +
                                       "create procedure from class org.voltdb.compiler.procedures.SelectStarHelloWorld;";
        final boolean standalone = true;
        final boolean isXCDR = false;
        VoltCompiler compiler = new VoltCompiler(standalone, isXCDR);
        if (!compiler.compileFromDDL(root0StagedCatalog.getAbsolutePath(), VoltProjectBuilder.createFileForSchema(differentSchema).getAbsolutePath())){
            fail();
        }
        nodesWithStagedCatalog = cluster.countNodesWithFile(Constants.CONFIG_DIR + File.separator + CatalogUtil.STAGED_CATALOG_FILE_NAME);
        assertEquals(nodesWithStagedCatalog, hostCount);
        clearLocalDataDirectories = false;
        skipInit = true;
        cluster.startUp(clearLocalDataDirectories, skipInit);
        // FIXME how to verify the error message is as expected? It is, but test doesn't know it is.

        // TODO: test that different stored procedure code results in startup failure
    }
}
