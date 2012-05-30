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

package org.voltdb.planner;

import java.io.File;
import java.io.IOException;

import org.voltdb.AdHocQueryTester;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.utils.CatalogUtil;

public class TestAdHocPlans extends AdHocQueryTester {

    private PlannerTool m_pt;

    @Override
    protected void setUp() throws Exception {
        VoltDB.Configuration config = setUpSPDB();
        byte[] bytes = CatalogUtil.toBytes(new File(config.m_pathToCatalog));
        String serializedCatalog = CatalogUtil.loadCatalogFromJar(bytes, null);
        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);
        CatalogContext context = new CatalogContext(0, catalog, bytes, 0, 0, 0);
        m_pt = new PlannerTool(context);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSP() throws Exception {
        runAllAdHocSPtests();
    }

    void erk() {
        /*
/* Not SP and not YET supported by executor.
* NOT SP
        compileAdHoc("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL != A.REPPEDVAL;");
*/
        /*
        spPartialCount = runQueryTest("SELECT * FROM PARTED1;", 0, 0, 2, NOT_VALIDATING_SP_RESULT);

        runQueryTest("SELECT * FROM PARTED1 WHERE PARTVAL != 0;", 1, spPartialCount-1, 1, VALIDATING_SP_RESULT);

        spPartialCount = runQueryTest("SELECT * FROM PARTED3;", 0, 0, 2, NOT_VALIDATING_SP_RESULT);
        runQueryTest("SELECT * FROM PARTED3 WHERE PARTVAL != 0;", 1, spPartialCount-1, 1, VALIDATING_SP_RESULT);

        runQueryTest("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and A.REPPEDVAL = B.PARTVAL;", 0, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL = A.REPPEDVAL;", 0, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest("SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = 0 and A.PARTVAL = B.REPPEDVAL;", 0, 0, 1, VALIDATING_SP_RESULT);
        runQueryTest("SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = 0 and B.REPPEDVAL = A.PARTVAL;", 0, 0, 1, VALIDATING_SP_RESULT);

        spPartialCount = runQueryTest("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL != A.REPPEDVAL;", 0, 0, 1, NOT_VALIDATING_SP_RESULT);
        runQueryTest("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL != A.REPPEDVAL;", 1, spPartialCount-1, 1, VALIDATING_SP_RESULT);
        */
    }

    /**
     * For planner-only testing, most of the args are ignored.
     */
    @Override
    public int runQueryTest(String query, int hash, int spPartialSoFar,
            int expected, int validatingSPresult) throws IOException,
            NoConnectionsException, ProcCallException {
        PlannerTool.Result result = m_pt.planSql(query, null);
        boolean spPlan = result.toString().contains("ALL: null");
        if ((validatingSPresult == VALIDATING_SP_RESULT) != spPlan) {
            System.out.println("Missed: "+ query);
            System.out.println(result);
        }
        assertEquals((validatingSPresult == VALIDATING_SP_RESULT), spPlan);
        return 0;
    }

}
