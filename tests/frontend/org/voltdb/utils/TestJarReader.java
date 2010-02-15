/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.utils;

import java.io.CharArrayReader;
import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;

import junit.framework.TestCase;

import org.voltdb.benchmark.tpcc.TPCCClient;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestJarReader extends TestCase {

    protected File jarPath;
    protected Catalog catalog;
    protected Database catalog_db;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        String schemaPath = "";
        try {
            URL url = TPCCClient.class.getResource("tpcc-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<users>" +
            "<user adhoc='true' groups='default' name='default' password='' sysproc='true'/>" +
            "</users>" +
            "<groups>" +
            "<group adhoc='true' name='default' sysproc='true'/>" +
            "</groups>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.TPCCTestProc' /></procedures>" +
            "<partitions><partition table='WAREHOUSE' column='W_ID' /></partitions>" +
            "</database>" +
            "</project>";

        System.out.println(simpleProject);

        File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        String projectPath = projectFile.getPath();

        VoltCompiler compiler = new VoltCompiler();
        ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");
        assertTrue(compiler.compile(projectPath, cluster_config,
                                    "testout.jar", System.out, null));

        // Now read the jar file back in and make sure that we can grab the
        // class file from it using JarClassLoader
        this.jarPath = new File("testout.jar");
        this.catalog = compiler.getCatalog();
        assertNotNull(this.catalog);
        this.catalog_db = this.catalog.getClusters().get("cluster").getDatabases().get("database");
        assertNotNull(this.catalog_db);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (jarPath != null) assertTrue(jarPath.delete());
    }

    /**
     *
     */
    public void testReadFileFromJarfile() throws IOException {
        String catalog0 = this.catalog.serialize();
        assertTrue(catalog0.length() > 0);

        String catalog1 = JarReader.readFileFromJarfile(this.jarPath.getAbsolutePath(), CatalogUtil.CATALOG_FILENAME);
        assertTrue(catalog1.length() > 0);

        assertEquals(catalog0.length(), catalog1.length());

        LineNumberReader reader0 = new LineNumberReader(new CharArrayReader(catalog0.toCharArray()));
        LineNumberReader reader1 = new LineNumberReader(new CharArrayReader(catalog1.toCharArray()));

        try {
            int lines = 0;
            while (reader0.ready()) {
                assertEquals(reader0.ready(), reader1.ready());
                assertEquals(reader0.readLine(), reader1.readLine());
                lines++;
            }
            assertTrue(lines > 0);
            reader0.close();
            reader1.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            assertTrue(false);
        }
    }
}
