/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import junit.framework.TestCase;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestInMemoryJarfile extends TestCase {

    protected File m_jarPath;
    protected Catalog m_catalog;
    protected Database m_catalogDb;
    // For backward compatibility test of <groups> and <group> elements.
    protected File m_jarPathWithGroupInsteadOfRole;
    protected Catalog m_catalogWithGroupInsteadOfRole;
    protected Database m_catalogDbWithGroupInsteadOfRole;

    private Catalog createTestJarFile(String jarFileName, boolean adhoc, String elemPfx)
    {
        String schemaPath = "";
        try {
            URL url = TPCCProjectBuilder.class.getResource("tpcc-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        String simpleProjectTmpl =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<%ss>" +
            "<%s adhoc='" + Boolean.toString(adhoc) + "' name='default' sysproc='true'/>" +
            "</%ss>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.TPCCTestProc' /></procedures>" +
            "<partitions><partition table='WAREHOUSE' column='W_ID' /></partitions>" +
            "</database>" +
            "</project>";
        String simpleProject = String.format(simpleProjectTmpl, elemPfx, elemPfx, elemPfx);
        System.out.println(simpleProject);
        File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        String projectPath = projectFile.getPath();
        VoltCompiler compiler = new VoltCompiler();
        assertTrue(compiler.compileWithProjectXML(projectPath, jarFileName));
        return compiler.getCatalog();
    }

    private Catalog createTestJarFile(String jarFileName, boolean adhoc) {
        return createTestJarFile(jarFileName, adhoc, "role");
    }

    @Override
    protected void setUp() throws Exception {
        System.out.print("START: " + System.currentTimeMillis());
        super.setUp();
        m_catalog = createTestJarFile("testout.jar", true, "role");
        assertNotNull(m_catalog);
        m_catalogDb = m_catalog.getClusters().get("cluster").getDatabases().get("database");
        assertNotNull(m_catalogDb);
        m_jarPath = new File("testout.jar");
        m_catalogWithGroupInsteadOfRole = createTestJarFile("testout_with_groups.jar", true, "group");
        assertNotNull(m_catalogWithGroupInsteadOfRole);
        m_catalogDbWithGroupInsteadOfRole = m_catalogWithGroupInsteadOfRole.getClusters().
                                                get("cluster").getDatabases().get("database");
        assertNotNull(m_catalogDbWithGroupInsteadOfRole);
        m_jarPathWithGroupInsteadOfRole = new File("testout_with_groups.jar");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (m_jarPath != null)
            assertTrue(m_jarPath.delete());
        File dupeFile = new File("testout-dupe.jar");
        dupeFile.delete();
    }

    /**
     *
     */
    public void testReadFileFromJarfile() throws IOException {
        String catalog0 = this.m_catalog.serialize();
        assertTrue(catalog0.length() > 0);

        InMemoryJarfile jarfile = new InMemoryJarfile(m_jarPath.getAbsolutePath());
        byte[] catalogBytes = jarfile.get(CatalogUtil.CATALOG_FILENAME);
        String catalog1 = new String(catalogBytes, "UTF-8");
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

    public void testIdenticalJarContentsMatchCRCs()
    throws IOException, InterruptedException
    {
        // Create a second jarfile with identical contents
        // Sleep for 5 seconds so the timestamps will differ
        // and cause different global CRCs
        // Use "group*" element names for backward compatibility test.
        Thread.sleep(5000);
        createTestJarFile("testout-dupe.jar", true);
        long crc1 = new InMemoryJarfile(m_jarPath).getCRC();
        long crc2 = new InMemoryJarfile("testout-dupe.jar").getCRC();
        assertEquals(crc1, crc2);

        // Check the modification times and make sure
        // that they differ in the two jars
        JarInputStream j_in = new JarInputStream(new FileInputStream(m_jarPath));
        JarEntry entry = j_in.getNextJarEntry();
        long time1 = entry.getTime();
        j_in.close();

        j_in = new JarInputStream(new FileInputStream("testout-dupe.jar"));
        entry = j_in.getNextJarEntry();
        long time2 = entry.getTime();
        assertFalse(time1 == time2);
    }

    public void testDifferentJarContentsDontMatchCRCs()
    throws IOException, InterruptedException
    {
        // Create a second jarfile with identical contents
        // Sleep for 5 seconds so the timestamps will differ
        // and cause different global CRCs
        Thread.sleep(5000);
        createTestJarFile("testout-dupe.jar", false);
        long crc1 = new InMemoryJarfile("testout.jar").getCRC();
        long crc2 = new InMemoryJarfile("testout-dupe.jar").getCRC();
        assertFalse(crc1 == crc2);
    }

    public void testIdenticalJarContentsWithGroupsMatchCRCs()
    throws IOException, InterruptedException
    {
        // Same as testIdenticalJarContentsMatchCRCs but with <groups>
        // instead of <roles>.
        // Create a second jarfile with identical contents
        // Sleep for 5 seconds so the timestamps will differ
        // and cause different global CRCs
        // Use "group*" element names for backward compatibility test.
        Thread.sleep(5000);
        createTestJarFile("testout-dupe-groups.jar", true, "group");
        long crc1 = new InMemoryJarfile(m_jarPathWithGroupInsteadOfRole).getCRC();
        long crc2 = new InMemoryJarfile("testout-dupe-groups.jar").getCRC();
        assertEquals(crc1, crc2);

        // Check the modification times and make sure
        // that they differ in the two jars
        JarInputStream j_in = new JarInputStream(new FileInputStream(m_jarPathWithGroupInsteadOfRole));
        JarEntry entry = j_in.getNextJarEntry();
        long time1 = entry.getTime();
        j_in.close();

        j_in = new JarInputStream(new FileInputStream("testout-dupe-groups.jar"));
        entry = j_in.getNextJarEntry();
        long time2 = entry.getTime();
        assertFalse(time1 == time2);
    }
}
