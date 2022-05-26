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

package org.voltdb.planner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.AdHocQueryTester;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.settings.DbSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

public class TestLargeModeRatio extends AdHocQueryTester {

    private PlannerTool m_pt;
    private CatalogContext m_context;
    private int runQueryTestCount = 0;

    @Before
    public void setUp() throws Exception {
        // For planner-only testing, we shouldn't care about IV2
        VoltDB.Configuration config = setUpSPDB();
        byte[] bytes = MiscUtils.fileToBytes(new File(config.m_pathToCatalog));
        String serializedCatalog = CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(bytes, false).getFirst());
        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);
        DbSettings dbSettings = new DbSettings(
                config.asClusterSettings().asSupplier(),
                NodeSettings.create(config.asPathSettingsMap()));
        m_context = new CatalogContext(catalog, dbSettings, 0, 0, bytes, null, new byte[]{}, mock(HostMessenger.class));
    }

    @Test
    public void testLargeModeRatio() throws Exception {
        // if LARGE_MODE_RATIO environment variable is set, skip this test
        String sysEnv = System.getenv("LARGE_MODE_RATIO");
        if (sysEnv != null && !sysEnv.equals("-1")) {
            return;
        }
        String originalLargeModeRatio = System.getProperty("LARGE_MODE_RATIO", "0");

        // LARGE_MODE_RATIO = 0, no large mode flip
        System.setProperty("LARGE_MODE_RATIO", "0");
        m_pt = new PlannerTool(m_context.database, m_context.getCatalogHash());

        //DB is empty so the hashable numbers don't really seem to matter
        runAllAdHocSPtests(0, 1, 2, 3);
        assertEquals(0, m_pt.getAdHocLargeModeCount());

        // LARGE_MODE_RATIO = 1, always large mode flip
        System.setProperty("LARGE_MODE_RATIO", "1");
        m_pt = new PlannerTool(m_context.database, m_context.getCatalogHash());

        runQueryTestCount = 0;
        //DB is empty so the hashable numbers don't really seem to matter
        runAllAdHocSPtests(0, 1, 2, 3);
        assertEquals(runQueryTestCount, m_pt.getAdHocLargeModeCount());

        // LARGE_MODE_RATIO = 0.5, we need some large mode flip
        System.setProperty("LARGE_MODE_RATIO", "0.5");
        m_pt = new PlannerTool(m_context.database, m_context.getCatalogHash());

        runQueryTestCount = 0;
        //DB is empty so the hashable numbers don't really seem to matter
        runAllAdHocSPtests(0, 1, 2, 3);
        assertTrue(0 < m_pt.getAdHocLargeModeCount());
        assertTrue(runQueryTestCount > m_pt.getAdHocLargeModeCount());

        // set back the original system property
        System.setProperty("LARGE_MODE_RATIO", originalLargeModeRatio);
    }

    @Override
    public int runQueryTest(String query, int hash, int spPartialSoFar,
                            int expected, int validatingSPresult) {
        // note: always increase the counter before calling planSqlForTest
        // Otherwise if we got a runtime exception in planSqlForTest, we will miss count that one
        runQueryTestCount++;
        m_pt.planSqlForTest(query);
        return 0;
    }
}
