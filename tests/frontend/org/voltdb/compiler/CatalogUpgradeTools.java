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

package org.voltdb.compiler;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import junit.framework.TestCase;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;

public class CatalogUpgradeTools
{
    public static InMemoryJarfile loadFromPath(String path) throws IOException
    {
        return CatalogUtil.loadInMemoryJarFile(MiscUtils.fileToBytes(new File(path)));
    }

    public static String[] getBuildInfoLines(InMemoryJarfile memCatalog)
            throws UnsupportedEncodingException
    {
        TestCase.assertNotNull(memCatalog);
        TestCase.assertTrue(memCatalog.containsKey(CatalogUtil.CATALOG_BUILDINFO_FILENAME));
        byte[] buildInfoBytes = memCatalog.get(CatalogUtil.CATALOG_BUILDINFO_FILENAME);
        TestCase.assertNotNull(buildInfoBytes);
        String buildInfo = new String(buildInfoBytes, "UTF-8");
        String[] buildInfoLines = buildInfo.split("\n");
        TestCase.assertTrue(buildInfoLines.length == 5);
        return buildInfoLines;
    }

    /**
     * Force a version upgrade.
     * @param memCatalog
     * @throws IOException
     */
    public static void dorkVersion(InMemoryJarfile memCatalog) throws IOException
    {
        String[] bi = getBuildInfoLines(memCatalog);
        bi[0] = bi[0].substring(0, bi[0].lastIndexOf('.'));
        memCatalog.put(CatalogUtil.CATALOG_BUILDINFO_FILENAME, StringUtils.join(bi, '\n').getBytes());
    }

    public static void dorkDowngradeVersion(String srcJar, String dstJar, String buildstring)
        throws Exception
    {
        InMemoryJarfile memCatalog = CatalogUpgradeTools.loadFromPath(srcJar);
        String[] bi = getBuildInfoLines(memCatalog);
        bi[0] = buildstring;
        memCatalog.put(CatalogUtil.CATALOG_BUILDINFO_FILENAME, StringUtils.join(bi, '\n').getBytes());
        memCatalog.writeToFile(new File(dstJar));
    }

    /**
     * Inject DDL statement.
     * @param memCatalog
     * @param statement
     * @throws UnsupportedEncodingException
     */
    public static void dorkDDL(InMemoryJarfile memCatalog, String statement)
            throws UnsupportedEncodingException
    {
        String key = VoltCompiler.AUTOGEN_DDL_FILE_NAME;
        String ddl = String.format("%s;\n%s", statement, new String(memCatalog.get(key), "UTF-8"));
        memCatalog.put(key, ddl.getBytes());
    }

    public static void dorkJar(String srcJar, String dstJar, String statement)
            throws IOException
    {
        InMemoryJarfile memCatalog = CatalogUpgradeTools.loadFromPath(srcJar);
        dorkVersion(memCatalog);
        if (statement != null) {
            dorkDDL(memCatalog, statement);
        }
        memCatalog.writeToFile(new File(dstJar));
    }

    public static InMemoryJarfile loadCatalog(byte[] catalogBytes, boolean expectUpgrade)
            throws IOException
    {
        InMemoryJarfile jarfile = CatalogUtil.loadInMemoryJarFile(catalogBytes);
        // Let VoltCompiler do a version check and upgrade the catalog on the fly.
        // I.e. jarfile may be modified.
        VoltCompiler compiler = new VoltCompiler(false);
        String upgradeFromVersion = compiler.upgradeCatalogAsNeeded(jarfile);
        if (expectUpgrade) {
            TestCase.assertTrue(upgradeFromVersion!=null);
        }
        else {
            TestCase.assertTrue(upgradeFromVersion==null);
        }
        return jarfile;
    }
}
