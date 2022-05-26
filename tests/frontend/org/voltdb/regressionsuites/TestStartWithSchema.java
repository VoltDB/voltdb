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

package org.voltdb.regressionsuites;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster.FailureState;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.InMemoryJarfile;


/** Tests that 'start' works with initialized schemas
 * 'init' tests can be found in TestInitStartAction
 */
final public class TestStartWithSchema {

    static final int siteCount = 1;
    static final int hostCount = 3;
    static final int kfactor   = 0;
    static final int clusterID = 0;

    static final String schema =
            "create table HELLOWORLD (greeting varchar(30), greeterid integer, PRIMARY KEY(greeting));\n" +
            "create procedure from class org.voltdb.compiler.procedures.SelectStarHelloWorld;";

    static final String mismatchSchema =
            "create table TEST (myval bigint not null, PRIMARY KEY(myval));";

    static void ensureAllCatalogDefaultArtifactsExists(InMemoryJarfile jarFile, String nameForDebugging) {
        Set<String> files = jarFile.keySet();
        assertTrue(nameForDebugging + " is empty!", files.size() > 0);
        for (String artifact : CatalogUtil.CATALOG_DEFAULT_ARTIFACTS) {
            // NOTE: autogen-ddl.sql IS required, as it contains the schema
            if (!artifact.equals(CatalogUtil.CATALOG_EMPTY_DDL_FILENAME)) {
                assertTrue(nameForDebugging + " does not contain " + artifact, files.contains(artifact));
            }
        }
    }

    /** Counts how many nodes contain staged catalogs (user-initialized schemas)
     * @param cluster
     * @return number of nodes in cluster that have staged catalogs
     */
    static int countNodesWithStagedCatalog(LocalCluster cluster) throws IOException {
        final String pathWithinSubroot = File.separator + Constants.DBROOT + File.separator + CatalogUtil.STAGED_CATALOG_PATH;
        int total = 0;
        for (Map.Entry<String, String> entry : cluster.getHostRoots().entrySet()) {
            assert( entry.getValue().contains(Constants.DBROOT) == false ) : entry.getValue();
            File testFile = new File(entry.getValue() + pathWithinSubroot);
            if (testFile.canRead() && (testFile.length() > 0)) {
                InMemoryJarfile jar = new InMemoryJarfile(testFile);
                ensureAllCatalogDefaultArtifactsExists(jar, testFile.getAbsolutePath());
                total++;
            }
        }
        return total;
    }

    LocalCluster cluster = null;

    @Before
    public void setUp() throws Exception
    {
        // Creates a cluster on the local machine using NewCLI, staging the specified schema.
        // Catalog compilation is taken care of by VoltDB itself - no need to do so explicitly.
        cluster = new LocalCluster(
                schema,
                null,
                null,
                siteCount,
                hostCount,
                kfactor,
                clusterID,
                BackendTarget.NATIVE_EE_JNI,
                FailureState.ALL_RUNNING, false, null);
        cluster.setHasLocalServer(false);
        cluster.overrideAnyRequestForValgrind();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.setUseDDLSchema(true);
        cluster.compileDeploymentOnly(builder);
        new File(builder.getPathToDeployment()).deleteOnExit();
        // positive tests should succeed; negative tests look for the exception
        cluster.setExpectedToCrash(false);
    }

    @Test
    public void testMatch() throws Exception
    {
        System.out.println("Start up is expected to succeed");
        boolean clearLocalDataDirectories = true;
        boolean skipInit = false;
        cluster.startUp(clearLocalDataDirectories, skipInit);

        for (int i = 0; i < hostCount; i++) {
            System.out.printf("Local cluster node[%d] ports: internal=%d, admin=%d, client=%d\n",
                              i, cluster.internalPort(i), cluster.adminPort(i), cluster.port(i));
        }

        System.out.println("Verifying schema and classes are present");
        Client client = ClientFactory.createClient();
        client.createConnection("localhost", cluster.port(1));
        ClientResponse response;
        response = client.callProcedure("@AdHoc", "INSERT INTO HELLOWORLD VALUES ('Hello, world!', 1);");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("SelectStarHelloWorld");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(1, response.getResults().length);
        assertEquals(1, response.getResults()[0].getRowCount());

        // Staged catalog will persist because durability is off, and being able to recover the schema is beneficial.
        int nodesWithStagedCatalog = TestStartWithSchema.countNodesWithStagedCatalog(cluster);
        assertEquals(hostCount, nodesWithStagedCatalog);

        client.close();
        cluster.shutDown();
    }

    /** Verify that one node having a staged catalog that is different prevents cluster from starting.
     */
    @Test
    public void testMismatch() throws Exception
    {
        cluster.setMismatchSchemaForInit(mismatchSchema, 1);

        System.out.println("Start up is expected to fail");
        boolean clearLocalDataDirectories = true;
        boolean skipInit = false;
        try {
            cluster.setExpectedToCrash(false); // ensure we get the exception
            cluster.startUp(clearLocalDataDirectories, skipInit);
            fail("Cluster started with mismatched schemas");
        } catch (Exception e){
            if (e.getMessage() == null || !e.getMessage().contains("external processes failed to start")){
                throw e;
            }
        }
        // cluster should already be shut down
    }

    @Test
    public void testMissing() throws Exception
    {
        cluster.setMismatchSchemaForInit(null, 2); // leave node 2 bare

        System.out.println("Start up is expected to fail");
        boolean clearLocalDataDirectories = true;
        boolean skipInit = false;
        try {
            cluster.startUp(clearLocalDataDirectories, skipInit);
            fail("Cluster started with a node missing the staged schema");
        } catch (Exception e){
            if (e.getMessage() == null || !e.getMessage().contains("external processes failed to start")){
                throw e;
            }
        }
        // cluster should already be shut down
    }
}
