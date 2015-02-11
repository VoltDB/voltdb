/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.io.File;
import java.io.IOException;

import org.voltdb.AdHocQueryTester;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

public class TestAdHocPlans extends AdHocQueryTester {

    private PlannerTool m_pt;
    private boolean m_debugging_set_this_to_retry_failures = false;

    @Override
    protected void setUp() throws Exception {
        // For planner-only testing, we shouldn't care about IV2
        VoltDB.Configuration config = setUpSPDB();
        byte[] bytes = MiscUtils.fileToBytes(new File(config.m_pathToCatalog));
        String serializedCatalog = CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(bytes).getFirst());
        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);
        CatalogContext context = new CatalogContext(0, 0, catalog, bytes, new byte[] {}, 0);
        m_pt = new PlannerTool(context.cluster, context.database, context.getCatalogHash());
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSP() throws Exception {
        //DB is empty so the hashable numbers don't really seem to matter
        runAllAdHocSPtests(0, 1, 2, 3);
    }

    /**
     * For planner-only testing, most of the args are ignored.
     */
    @Override
    public int runQueryTest(String query, int hash, int spPartialSoFar,
            int expected, int validatingSPresult) throws IOException,
            NoConnectionsException, ProcCallException {
        AdHocPlannedStatement result = m_pt.planSqlForTest(query);
        boolean spPlan = result.toString().contains("ALL: null");
        if ((validatingSPresult == VALIDATING_SP_RESULT) != spPlan) {
            System.out.println("Missed: "+ query);
            System.out.println(result);
            // This is a good place for a breakpoint,
            // to set this debugging flag and step into the planner,
            // for a do-over after getting an unexpected result.
            if (m_debugging_set_this_to_retry_failures) {
                result = m_pt.planSqlForTest(query);
            }
        }
        assertEquals((validatingSPresult == VALIDATING_SP_RESULT), spPlan);
        return 0;
    }

}
