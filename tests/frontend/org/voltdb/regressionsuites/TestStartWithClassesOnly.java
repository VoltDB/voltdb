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

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster.FailureState;
import org.voltdb.utils.InMemoryJarfile;


/** Tests that 'start' works with initialized schemas
 * 'init' tests can be found in TestInitStartAction
 */
final public class TestStartWithClassesOnly {

    static final int siteCount = 1;
    static final int hostCount = 1;
    static final int kfactor   = 0;
    static final int clusterID = 0;

    LocalCluster cluster = null;

    @Before
    public void setUp() throws Exception
    {
        InMemoryJarfile classesFile = new InMemoryJarfile();
        String classesJarToStage = File.createTempFile("preloaded-classes", ".jar").getCanonicalPath();
        classesFile.writeToFile(new File(classesJarToStage));

        // Creates a cluster on the local machine using NewCLI, staging the specified schema.
        // Catalog compilation is taken care of by VoltDB itself - no need to do so explicitly.
        cluster = new LocalCluster(
                null,
                classesJarToStage,
                null,
                siteCount,
                hostCount,
                kfactor,
                clusterID,
                BackendTarget.NATIVE_EE_JNI,
                FailureState.ALL_RUNNING, false, null);
        cluster.setHasLocalServer(true);
        cluster.overrideAnyRequestForValgrind();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.setUseDDLSchema(true);
        cluster.compileDeploymentOnly(builder);
        new File(builder.getPathToDeployment()).deleteOnExit();
        // positive tests should succeed; negative tests look for the exception
        cluster.setExpectedToCrash(false);
    }

    @Test
    public void testLoadAndStart() throws Exception
    {
        System.out.println("Start up is expected to succeed");
        boolean clearLocalDataDirectories = true;
        boolean skipInit = false;
        cluster.startUp(clearLocalDataDirectories, skipInit);

        for (int i = 0; i < hostCount; i++) {
            System.out.printf("Local cluster node[%d] ports: internal=%d, admin=%d, client=%d\n",
                              i, cluster.internalPort(i), cluster.adminPort(i), cluster.port(i));
        }

        // Staged catalog will persist because durability is off, and being able to recover the schema is beneficial.
        int nodesWithStagedCatalog = TestStartWithSchema.countNodesWithStagedCatalog(cluster);
        assertEquals(hostCount, nodesWithStagedCatalog);

        System.out.println("Verifying schema and classes are present");
        Client client = ClientFactory.createClient();
        client.createConnection("localhost", cluster.port(0));
        ClientResponse response;

        //Now load the table and procedure should be successful as classes should be present.
        response = client.callProcedure("@AdHoc", "create table HELLOWORLD (greeting varchar(30), greeterid integer, PRIMARY KEY(greeting));");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("@AdHoc", "create procedure from class org.voltdb.compiler.procedures.SelectStarHelloWorld;");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        //We should be able to utilize those artifacts.
        response = client.callProcedure("@AdHoc", "INSERT INTO HELLOWORLD VALUES ('Hello, world!', 1);");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("SelectStarHelloWorld");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(1, response.getResults().length);
        assertEquals(1, response.getResults()[0].getRowCount());

        client.close();
        cluster.shutDown();
    }

}
