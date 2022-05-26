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

import junit.framework.TestCase;

import org.voltdb.VoltDB;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;

public class TestCatalogVersionUpgrade extends TestCase {

    // The message substring we expect to find from an upgrade failure.
    private static String UPGRADE_ERROR_MESSAGE_SUBSTRING = "Catalog upgrade failed";

    public void testCatalogAutoUpgrade() throws Exception
    {
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addDefaultProcedures();

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String jarName = "compile-deployment.jar";
        String catalogJar = testDir + File.separator + jarName;
        assertTrue("Project failed to compile", project.compile(catalogJar));

        // Load the catalog to an in-memory jar and tweak the version.
        byte[] bytes = MiscUtils.fileToBytes(new File(catalogJar));
        InMemoryJarfile memCatalog = CatalogUpgradeTools.loadCatalog(bytes, false);
        CatalogUpgradeTools.dorkVersion(memCatalog);

        // Load/upgrade and check against the server version.
        InMemoryJarfile memCatalog2 = CatalogUpgradeTools.loadCatalog(memCatalog.getFullJarBytes(), true);
        assertNotNull(memCatalog2);
        String[] buildInfoLines2 = CatalogUpgradeTools.getBuildInfoLines(memCatalog2);
        String serverVersion = VoltDB.instance().getVersionString();
        assertTrue(serverVersion.equals(buildInfoLines2[0]));

        // Make sure the jar file is present.
        String jarName2 = String.format("catalog-%s.jar", serverVersion);
        File jar2 = new File(VoltDB.Configuration.getPathToCatalogForTest(jarName2));
        assertTrue(jar2.exists());
    }

    public void testCatalogAutoUpgradeFail() throws Exception
    {
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addDefaultProcedures();

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String jarName = "compile-deployment.jar";
        String catalogJar = testDir + File.separator + jarName;
        assertTrue("Project failed to compile", project.compile(catalogJar));

        // Load the catalog to an in-memory jar and tweak the version to make it incompatible.
        byte[] bytes = MiscUtils.fileToBytes(new File(catalogJar));
        InMemoryJarfile memCatalog = CatalogUpgradeTools.loadCatalog(bytes, false);
        CatalogUpgradeTools.dorkVersion(memCatalog);
        // Squizzle creation is no longer supported.
        CatalogUpgradeTools.dorkDDL(memCatalog, "CREATE SQUIZZLE");

        // Check the (hopefully) upgraded catalog version against the server version.
        try {
            CatalogUpgradeTools.loadCatalog(memCatalog.getFullJarBytes(), true);
            fail("Expected load to generate an exception");
        }
        catch (IOException e) {
            // Happy if the message mentions the bad create statement.
            String message = e.getMessage();
            assertTrue(message.contains(UPGRADE_ERROR_MESSAGE_SUBSTRING));
        }
    }
}
