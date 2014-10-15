/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.compilereport;

import java.io.File;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltCompiler.Feedback;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

public class TestReportMaker extends TestCase {

    private static Catalog getCatalog(String name) throws Exception {
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addDefaultSchema();
        builder.addDefaultPartitioning();
        builder.addStmtProcedure("NeedsEscape",
                "SELECT COUNT(*) FROM ORDER_LINE WHERE OL_QUANTITY<10 AND OL_QUANTITY>5");

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String retval = testDir + File.separator + "test-reportmaker-" + name + ".jar";
        builder.compile(retval);

        byte[] bytes = MiscUtils.fileToBytes(new File(retval));
        String serializedCatalog = CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(bytes).getFirst());
        assertNotNull(serializedCatalog);
        Catalog c = new Catalog();
        c.execute(serializedCatalog);

        return c;
    }

    public void testEscapeSqlText() throws Exception {
        Catalog catalog = getCatalog("escape-html");
        String report = ReportMaker.report(catalog, new ArrayList<Feedback>(), "");

        assertTrue(report.contains("SELECT COUNT(*) FROM ORDER_LINE WHERE OL_QUANTITY&lt;10 AND OL_QUANTITY&gt;5"));
        assertFalse(report.contains("OL_QUANTITY<10"));
        assertFalse(report.contains("OL_QUANTITY>5"));
    }

}
