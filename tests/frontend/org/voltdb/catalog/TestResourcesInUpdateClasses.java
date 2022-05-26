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
import java.net.URL;

import org.junit.Test;
import org.voltcore.utils.Pair;
import org.voltdb.BackendTarget;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb_testprocs.catalog.resourceuse.SwitchResourceProc;
import org.voltdb_testprocs.catalog.resourceuse.UseResourceProc;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.io.Resources;

import static org.junit.Assert.*;

public class TestResourcesInUpdateClasses extends JUnit4LocalClusterTest {

    private Pair<InMemoryJarfile, String> buildInMemoryJar(
            byte[] rawCatalog, String resourceFileVersion, boolean useVersionInName) throws Exception {
        // turn it into an in memory JarFile
        InMemoryJarfile IMJF = new InMemoryJarfile(rawCatalog);

        // get a resource from our filesystem and add that resource to the jar
        URL resourceURL = UseResourceProc.class.getResource("resource" + resourceFileVersion + ".txt");
        byte[] resourceContents = Resources.toByteArray(resourceURL);
        String resourceContentsString = Resources.toString(resourceURL, Charsets.UTF_8);
        if (useVersionInName) {
            IMJF.put("org/voltdb_testprocs/catalog/resourceuse/catalog_resource" + resourceFileVersion + ".txt", resourceContents);
        } else {
            IMJF.put("org/voltdb_testprocs/catalog/resourceuse/resource.txt", resourceContents);
        }
        return Pair.of(IMJF, resourceContentsString);
    }

    private Pair<File, String> buildJarFile(
            byte[] rawCatalog, String resourceFileVersion, boolean useVersionInName) throws Exception {
        Pair<InMemoryJarfile, String> rslt = buildInMemoryJar(rawCatalog, resourceFileVersion, useVersionInName);
        // write the new jar to disk
        File jarPath = new File(Configuration.getPathToCatalogForTest("jarWithResource" + resourceFileVersion + ".jar"));
        rslt.getFirst().writeToFile(jarPath);
        return Pair.of(jarPath, rslt.getSecond());
    }

    private Pair<byte[], String> buildJarBytes(
            byte[] rawCatalog, String resourceFileVersion, boolean useVersionInName) throws Exception {
        Pair<InMemoryJarfile, String> rslt = buildInMemoryJar(rawCatalog, resourceFileVersion, useVersionInName);
        return Pair.of(rslt.getFirst().getFullJarBytes(), rslt.getSecond());
    }

    @Test
    public void testBasic() throws Exception {
        // create a catalog jarfile with one class, the UseResourceProc
        CatalogBuilder builder = new CatalogBuilder();
        builder.addProcedures(UseResourceProc.class);
        byte[] rawCatalog = builder.compileToBytes();

        Pair<File, String> ucChange1 = buildJarFile(rawCatalog, "1", false);
        Pair<File, String> ucChange2 = buildJarFile(rawCatalog, "2", false);
        assertNotEquals(ucChange2.getSecond(), ucChange1.getSecond());

        // start voltdb
        VoltProjectBuilder vpb = new VoltProjectBuilder();
        vpb.setUseDDLSchema(true);
        LocalCluster server = new LocalCluster("ddl.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        boolean success = server.compile(vpb);
        assertTrue(success);

        server.setHasLocalServer(false);
        server.startUp();
        Client client = null;

        try {
            client = ClientFactory.createClient();
            client.createConnection(server.getListenerAddress(0));

            // load the new jar
            client.updateClasses(ucChange1.getFirst(), "");

            // create the procedure from loaded jar
            ClientResponse cr = client.callProcedure("@AdHoc",
                    "create procedure from class org.voltdb_testprocs.catalog.resourceuse.UseResourceProc;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            // check the proc can be called and that it uses the resource correctly
            cr = client.callProcedure(UseResourceProc.class.getSimpleName());
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            assertEquals(1, cr.getResults().length);
            VoltTable t = cr.getResults()[0];
            assertEquals(1, t.getRowCount());
            assertEquals(1, t.getColumnCount());
            assertEquals(VoltType.STRING, t.getColumnType(0));

            VoltTableRow row = t.fetchRow(0);
            assertEquals(ucChange1.getSecond(), row.getString(0));

            // load the second jar to replace the resource
            client.updateClasses(ucChange2.getFirst(), "");

            // check the proc can be called and that it uses the resource correctly
            cr = client.callProcedure(UseResourceProc.class.getSimpleName());
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            assertEquals(1, cr.getResults().length);
            t = cr.getResults()[0];
            assertEquals(1, t.getRowCount());
            assertEquals(1, t.getColumnCount());
            assertEquals(VoltType.STRING, t.getColumnType(0));

            row = t.fetchRow(0);
            assertEquals(ucChange2.getSecond(), row.getString(0));
        } finally {
            if (client != null) {
                client.close();
            }
            server.shutDown();
        }
    }

    @Test
    public void testRemoveResourceWithUAC() throws Exception {
        // start voltdb
        VoltProjectBuilder vpb = new VoltProjectBuilder();
        vpb.setUseDDLSchema(false);
        vpb.addProcedure(SwitchResourceProc.class);
        vpb.addLiteralSchema("create procedure from class org.voltdb_testprocs.catalog.resourceuse.UseResourceProc;");
        LocalCluster server = new LocalCluster("ddl.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        boolean success = server.compile(vpb);

        // create a catalog jarfile with one class, the UseResourceProc
        Catalog catalog = server.getInitialCatalog();
        String jarFileName = server.getTemplateCommandLine().jarFileName();
        InMemoryJarfile jarFile = new InMemoryJarfile(jarFileName);
        byte[] catalogBytes = jarFile.getFullJarBytes();
        Pair<byte[], String> ucChange1 = buildJarBytes(catalogBytes, "1", true);
        Pair<byte[], String> ucChange2 = buildJarBytes(catalogBytes, "2", true);

        assertTrue(success);

        server.setHasLocalServer(false);
        server.startUp();
        Client client = null;

        try {
            client = ClientFactory.createClient();
            client.createConnection(server.getListenerAddress(0));

            // load the new jar
            client.callProcedure("@UpdateApplicationCatalog", ucChange1.getFirst(), null);

            // check the proc can be called and that it uses the resource correctly
            ClientResponse cr = client.callProcedure(SwitchResourceProc.class.getSimpleName(), 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            assertEquals(1, cr.getResults().length);
            VoltTable t = cr.getResults()[0];
            assertEquals(1, t.getRowCount());
            assertEquals(1, t.getColumnCount());
            assertEquals(VoltType.STRING, t.getColumnType(0));

            VoltTableRow row = t.fetchRow(0);
            String resourceContentsStringRT = row.getString(0);
            assertEquals(resourceContentsStringRT, ucChange1.getSecond());

            // load the second jar to replace the resource
            client.callProcedure("@UpdateApplicationCatalog", ucChange2.getFirst(), null);

            // check the proc can be called and that it uses the resource correctly
            cr = client.callProcedure(SwitchResourceProc.class.getSimpleName(), 2);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            assertEquals(1, cr.getResults().length);
            t = cr.getResults()[0];
            assertEquals(1, t.getRowCount());
            assertEquals(1, t.getColumnCount());
            assertEquals(VoltType.STRING, t.getColumnType(0));

            row = t.fetchRow(0);
            resourceContentsStringRT = row.getString(0);
            assertEquals(resourceContentsStringRT, ucChange2.getSecond());
        } finally {
            if (client != null) {
                client.close();
            }
            server.shutDown();
        }
    }

}
