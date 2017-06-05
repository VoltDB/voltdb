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

package org.voltdb.catalog;

import java.io.File;
import java.net.URL;

import junit.framework.TestCase;

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
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb_testprocs.catalog.resourceuse.UseResourceProc;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.io.Resources;

public class TestResourcesInUpdateClasses extends TestCase {

    public void testBasic() throws Exception {
        // create a catalog jarfile with one class, the UseResourceProc
        CatalogBuilder builder = new CatalogBuilder();
        builder.addProcedures(UseResourceProc.class);
        byte[] rawCatalog = builder.compileToBytes();

        // turn it into an in memory JarFile
        InMemoryJarfile IMJF1 = new InMemoryJarfile(rawCatalog);

        // get a resource from our filesystem and add that resource to the jar
        URL resourceURL = UseResourceProc.class.getResource("resource.txt");
        byte[] resourceContents = Resources.toByteArray(resourceURL);
        String resourceContentsString = Resources.toString(resourceURL, Charsets.UTF_8);
        IMJF1.put("org/voltdb_testprocs/catalog/resourceuse/resource.txt", resourceContents);

        // write the new jar to disk
        File jarPath = new File(Configuration.getPathToCatalogForTest("jarWithResource.jar"));
        IMJF1.writeToFile(jarPath);

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
            client.updateClasses(jarPath, "");

            // create the procedure from loaded jar
            ClientResponse cr = client.callProcedure(
                    "@AdHoc",
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
            String resourceContentsStringRT = row.getString(0);
            assertTrue(resourceContentsString.equals(resourceContentsStringRT));
        }
        finally {
            if (client != null) {
                client.close();
            }
            server.shutDown();
        }
    }

}
