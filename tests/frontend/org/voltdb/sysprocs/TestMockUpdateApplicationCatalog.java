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

package org.voltdb.sysprocs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.OperationMode;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.UpdateApplicationCatalog;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestMockUpdateApplicationCatalog {
    private ServerThread m_localServer;
    private VoltDB.Configuration m_config;
    private Client m_client;


    static private void addBaseProcedures(VoltProjectBuilder project) {
        project.addProcedure(org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                "NEW_ORDER.NO_W_ID: 2");
        project.addProcedure(org.voltdb.benchmark.tpcc.procedures.SelectAll.class);
        project.addProcedure(org.voltdb.benchmark.tpcc.procedures.delivery.class,
                "WAREHOUSE.W_ID: 0");
    }

    @Before
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
        addBaseProcedures(builder);
        success = builder.compile(Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.jar"), 1, 1, 0);
        assert(success);

        assertEquals(OperationMode.RUNNING, VoltDB.instance().getMode());
        m_client = ClientFactory.createClient();
        m_client.createConnection("localhost:" + m_config.m_adminPort);
    }

    @After
    public void tearDown() throws Exception {
        if (m_client != null) {
            m_client.close();
        }
        if (m_localServer != null) {
            m_localServer.shutdown();
        }
    }

    @Test
    public void testInvalidCatalogJar() throws IOException, ClassNotFoundException {
        String newCatalogURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-expanded.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("catalogupdate-cluster-base.xml");

        String tempjarURL = newCatalogURL + ".tmp";
        JarOutputStream jos = new JarOutputStream(new FileOutputStream(tempjarURL));

        // Create a corrupted new catalog jar
        JarFile jarFile = new JarFile(new File(newCatalogURL));
        for (Enumeration<JarEntry> e = jarFile.entries(); e.hasMoreElements();) {
            JarEntry je = e.nextElement();
            System.err.println(je.getName());

            if (!je.getName().contains(".class")) {
                // Other files
                jos.putNextEntry(je);
                InputStream is = jarFile.getInputStream(je);
                byte[] bytes = new byte[2048];
                int numOfBytes;
                while ((numOfBytes = is.read(bytes)) != -1) {
                    jos.write(bytes, 0, numOfBytes);
                }
                is.close();
            } else {
                // Corrupt the class files paths
                JarEntry newJe = new JarEntry("fake/" + je.getName());
                jos.putNextEntry(newJe);
                InputStream is = jarFile.getInputStream(je);
                byte[] bytes = new byte[2048];
                int numOfBytes;
                while ((numOfBytes = is.read(bytes)) != -1) {
                    jos.write(bytes, 0, numOfBytes);
                }
                is.close();
            }
        }

        jos.close();

        // Replace the jar
        new File(newCatalogURL).delete();
        new File(tempjarURL).renameTo(new File(newCatalogURL));

        try {
            UpdateApplicationCatalog.update(m_client, new File(newCatalogURL), new File(deploymentURL));
            fail("Update catalog should fail with version error.");
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("NoClassDefFoundError"));
            assertTrue(e.getMessage().contains("Error loading class"));
        }
    }
}
