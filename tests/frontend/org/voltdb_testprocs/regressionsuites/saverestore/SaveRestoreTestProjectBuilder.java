/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb_testprocs.regressionsuites.saverestore;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;

public class SaveRestoreTestProjectBuilder extends VoltProjectBuilder
{
    public static Class<?> PROCEDURES[] =
        new Class<?>[] { MatView.class, SaveRestoreSelect.class};

    public static String partitioning[][] =
        new String[][] {{"PARTITION_TESTER", "PT_ID"},
                        {"CHANGE_COLUMNS", "ID"},
                        {"JUMBO_ROW", "PKEY"}};

    public static final URL ddlURL =
        SaveRestoreTestProjectBuilder.class.getResource("saverestore-ddl.sql");
    public static final String jarFilename = "saverestore.jar";

    public void addDefaultProcedures()
    {
        addProcedures(PROCEDURES);
    }

    public void addDefaultPartitioning()
    {
        for (String pair[] : partitioning)
        {
            addPartitionInfo(pair[0], pair[1]);
        }
    }

    public void addDefaultSchema()
    {
        addSchema(ddlURL);
    }

    @Override
    public void addAllDefaults()
    {
        addDefaultProcedures();
        addDefaultPartitioning();
        addDefaultSchema();
        addStmtProcedure("JumboInsert", "INSERT INTO JUMBO_ROW VALUES ( ?, ?, ?)", "JUMBO_ROW.PKEY: 0");
        addStmtProcedure("JumboSelect", "SELECT * FROM JUMBO_ROW WHERE PKEY = ?", "JUMBO_ROW.PKEY: 0");
        addStmtProcedure("JumboCount", "SELECT COUNT(*) FROM JUMBO_ROW");
    }

    /*
     * Different default set for the test of ENG-696 TestReplicatedSaveRestoreSysprocSuite
     * Has no partitioned tables.
     */
    public void addAllDefaultsNoPartitioning() {
        addDefaultProcedures();
        addDefaultSchema();
    }

    public String getJARFilename()
    {
        return jarFilename;
    }

    public Catalog createSaveRestoreSchemaCatalog() throws IOException
    {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String catalogJar = testDir + File.separator + "saverestore-jni.jar";

        addDefaultSchema();
        addDefaultPartitioning();
        addDefaultProcedures();

        addStmtProcedure("JumboInsert", "INSERT INTO JUMBO_ROW VALUES ( ?, ?, ?)", "JUMBO_ROW.PKEY: 0");
        addStmtProcedure("JumboSelect", "SELECT * FROM JUMBO_ROW WHERE PKEY = ?", "JUMBO_ROW.PKEY: 0");
        addStmtProcedure("JumboCount", "SELECT COUNT(*) FROM JUMBO_ROW");

        boolean status = compile(catalogJar);
        assert(status);

        // read in the catalog
        byte[] bytes = CatalogUtil.toBytes(new File(catalogJar));
        String serializedCatalog = CatalogUtil.loadCatalogFromJar(bytes, null);
        assert(serializedCatalog != null);

        // create the catalog (that will be passed to the ClientInterface
        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);

        return catalog;
    }
}
