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

import junit.framework.TestCase;

import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

public class TestAdHocPlans extends TestCase {

    private PlannerTool m_pt;

    private void compileAdHoc(String sql)
    {
        PlannerTool.Result result = m_pt.planSql(sql, null);
        boolean spPlan = result.toString().contains("ALL: null");
        if (! spPlan) {
            System.out.println("Missed: "+ sql);
            System.out.println(result);
        }
        assertTrue(spPlan);
    }

    @Override
    protected void setUp() throws Exception {
        String spSchema =
                "create table PARTED1 (" +
                "PARTVAL bigint not null, " +
                "NONPART bigint not null," +
                "PRIMARY KEY(PARTVAL));" +

                "create table PARTED2 (" +
                "PARTVAL bigint not null, " +
                "NONPART bigint not null," +
                "PRIMARY KEY(PARTVAL));" +

                "create table PARTED3 (" +
                "PARTVAL bigint not null, " +
                "NONPART bigint not null," +
                "PRIMARY KEY(NONPART));" +

                "create table REPPED1 (" +
                "REPPEDVAL bigint not null, " +
                "NONPART bigint not null," +
                "PRIMARY KEY(REPPEDVAL));" +

                "create table REPPED2 (" +
                "REPPEDVAL bigint not null, " +
                "NONPART bigint not null," +
                "PRIMARY KEY(REPPEDVAL));";

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocsp.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocsp.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(spSchema);
        builder.addPartitionInfo("PARTED1", "PARTVAL");
        builder.addPartitionInfo("PARTED2", "PARTVAL");
        builder.addPartitionInfo("PARTED3", "PARTVAL");
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        byte[] bytes = CatalogUtil.toBytes(new File(pathToCatalog));
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
        compileAdHoc("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM REPPED1;");
        compileAdHoc("SELECT * FROM PARTED1 WHERE PARTVAL = 0;");
        compileAdHoc("SELECT * FROM PARTED3 WHERE PARTVAL = 0;");
        compileAdHoc("SELECT * FROM REPPED1 WHERE REPPEDVAL = 0;");

        compileAdHoc("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = 0 and B.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = 0 and B.REPPEDVAL = 0;");
        compileAdHoc("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = 0 and B.REPPEDVAL = 0;");

        compileAdHoc("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;");
        compileAdHoc("SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;");
        compileAdHoc("SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;");
        // compileAdHoc("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and A.REPPEDVAL = B.PARTVAL;");
        compileAdHoc("SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = 0 and A.PARTVAL = B.REPPEDVAL;");
        compileAdHoc("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = 0 and A.REPPEDVAL = B.REPPEDVAL;");

        compileAdHoc("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = B.PARTVAL and A.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = B.PARTVAL and A.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = B.PARTVAL and A.PARTVAL = 0;");
        // compileAdHoc("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = B.PARTVAL and A.REPPEDVAL = 0;");
        compileAdHoc("SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = B.REPPEDVAL and A.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = B.REPPEDVAL and A.REPPEDVAL = 0;");

        compileAdHoc("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;");
        compileAdHoc("SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;");
        compileAdHoc("SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;");
        // compileAdHoc("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL = A.REPPEDVAL;");
        compileAdHoc("SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = 0 and B.REPPEDVAL = A.PARTVAL;");
        compileAdHoc("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = 0 and B.REPPEDVAL = A.REPPEDVAL;");

        compileAdHoc("SELECT * FROM PARTED1 A, PARTED2 B WHERE B.PARTVAL = A.PARTVAL and A.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM PARTED3 A, PARTED2 B WHERE B.PARTVAL = A.PARTVAL and A.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM PARTED2 A, PARTED3 B WHERE B.PARTVAL = A.PARTVAL and A.PARTVAL = 0;");
        // compileAdHoc("SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = A.REPPEDVAL and A.REPPEDVAL = 0;");
        compileAdHoc("SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = A.PARTVAL and A.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM REPPED1 A, REPPED2 B WHERE B.REPPEDVAL = A.REPPEDVAL and A.REPPEDVAL = 0;");

        compileAdHoc("SELECT * FROM PARTED1 A, PARTED2 B WHERE B.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;");
        compileAdHoc("SELECT * FROM PARTED3 A, PARTED2 B WHERE B.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;");
        compileAdHoc("SELECT * FROM PARTED2 A, PARTED3 B WHERE B.PARTVAL = 0 and A.PARTVAL = B.PARTVAL;");
        compileAdHoc("SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = 0 and A.REPPEDVAL = B.PARTVAL;");
        // compileAdHoc("SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = 0 and A.PARTVAL = B.REPPEDVAL;");
        compileAdHoc("SELECT * FROM REPPED1 A, REPPED2 B WHERE B.REPPEDVAL = 0 and A.REPPEDVAL = B.REPPEDVAL;");

        compileAdHoc("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = B.PARTVAL and B.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = B.PARTVAL and B.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = B.PARTVAL and B.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = B.PARTVAL and B.PARTVAL = 0;");
        // compileAdHoc("SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = B.REPPEDVAL and B.REPPEDVAL = 0;");
        compileAdHoc("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = B.REPPEDVAL and B.REPPEDVAL = 0;");

        compileAdHoc("SELECT * FROM PARTED1 A, PARTED2 B WHERE B.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;");
        compileAdHoc("SELECT * FROM PARTED3 A, PARTED2 B WHERE B.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;");
        compileAdHoc("SELECT * FROM PARTED2 A, PARTED3 B WHERE B.PARTVAL = 0 and B.PARTVAL = A.PARTVAL;");
        compileAdHoc("SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = 0 and B.PARTVAL = A.REPPEDVAL;");
        // compileAdHoc("SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = 0 and B.REPPEDVAL = A.PARTVAL;");
        compileAdHoc("SELECT * FROM REPPED1 A, REPPED2 B WHERE B.REPPEDVAL = 0 and B.REPPEDVAL = A.REPPEDVAL;");

        compileAdHoc("SELECT * FROM PARTED1 A, PARTED2 B WHERE B.PARTVAL = A.PARTVAL and B.PARTVAL =0;");
        compileAdHoc("SELECT * FROM PARTED3 A, PARTED2 B WHERE B.PARTVAL = A.PARTVAL and B.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM PARTED2 A, PARTED3 B WHERE B.PARTVAL = A.PARTVAL and B.PARTVAL = 0;");
        compileAdHoc("SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = A.REPPEDVAL and B.PARTVAL = 0;");
        // compileAdHoc("SELECT * FROM PARTED2 A, REPPED1 B WHERE B.REPPEDVAL = A.PARTVAL and B.REPPEDVAL = 0;");
        compileAdHoc("SELECT * FROM REPPED1 A, REPPED2 B WHERE B.REPPEDVAL = A.REPPEDVAL and B.REPPEDVAL = 0;");

/* Not SP and not YET supported.
        compileAdHoc("SELECT * FROM PARTED1 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL != A.PARTVAL;");

        compileAdHoc("SELECT * FROM PARTED3 A, PARTED2 B WHERE A.PARTVAL = 0 and B.PARTVAL != A.PARTVAL;");

        compileAdHoc("SELECT * FROM PARTED2 A, PARTED3 B WHERE A.PARTVAL = 0 and B.PARTVAL != A.PARTVAL;");
*/
/* NOT SP
        compileAdHoc("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = 0 and B.PARTVAL != A.REPPEDVAL;");
*/

        compileAdHoc("SELECT * FROM PARTED2 A, REPPED1 B WHERE A.PARTVAL = 0 and B.REPPEDVAL != A.PARTVAL;");

        // TODO: Three-way join test cases are probably required to cover all code paths through AccessPaths.
    }

}
