/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.sysprocs;

import java.io.File;
import java.io.IOException;

import org.mockito.Matchers;
import org.mockito.Mockito;
import org.voltdb.OperationMode;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.sysprocs.UpdateApplicationCatalog.JavaClassForTest;
import org.voltdb.utils.MiscUtils;

import junit.framework.TestCase;

public class TestMockUpdateApplicationCatalog extends TestCase {
    private ServerThread m_localServer;
    private VoltDB.Configuration m_config;
    private Client m_client;

    static Class<?>[] BASEPROCS = { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                    org.voltdb.benchmark.tpcc.procedures.SelectAll.class,
                                    org.voltdb.benchmark.tpcc.procedures.delivery.class };

    @Override
    public void setUp() throws Exception
    {
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addDefaultSchema();
        builder.addDefaultPartitioning();
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar"), 1, 1, 0);
        assert(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml"));

        m_config = new VoltDB.Configuration();
        m_config.m_pathToCatalog = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.jar");
        m_config.m_pathToDeployment = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
        m_localServer = new ServerThread(m_config);
        m_localServer.start();
        m_localServer.waitForInitialization();

        builder = new TPCCProjectBuilder();
        builder.addDefaultSchema();
        builder.addDefaultPartitioning();
        builder.addProcedures(BASEPROCS);
        success = builder.compile(Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.jar"), 1, 1, 0);
        assert(success);

        JavaClassForTest testClass = Mockito.mock(JavaClassForTest.class);
        Mockito.when(testClass.forName(Matchers.anyString(), Matchers.anyBoolean(), Mockito.any(ClassLoader.class))).
                     thenThrow(new UnsupportedClassVersionError("Unsupported major.minor version 52.0"));
        UpdateApplicationCatalog.setJavaClassForTest(testClass);

        assertEquals(OperationMode.RUNNING, VoltDB.instance().getMode());
        m_client = ClientFactory.createClient();
        m_client.createConnection("localhost:" + m_config.m_adminPort);
    }

    @Override
    public void tearDown() throws Exception {
        if (m_client != null) {
            m_client.close();
        }
        if (m_localServer != null) {
            m_localServer.shutdown();
        }
    }

    public void testVersionCheck() throws IOException, ClassNotFoundException {
        String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");
        try {
            m_client.updateApplicationCatalog(new File(newCatalogURL), new File(deploymentURL));
            fail("Update catalog should fail with version error.");
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Cannot load classes compiled with a higher version"));
            assertTrue(e.getMessage().contains("Java 8"));
        }
    }
}
