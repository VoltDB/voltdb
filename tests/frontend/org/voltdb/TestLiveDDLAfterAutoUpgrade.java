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

package org.voltdb;

import java.io.File;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.CatalogUpgradeTools;
import org.voltdb.compiler.DeploymentBuilder;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;

public class TestLiveDDLAfterAutoUpgrade extends AdhocDDLTestBase {

    public void testEng7357() throws Exception
    {
        String ddl =
                "create table FOO (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint pk_tree primary key (ID)" +
                ");\n" +
                "partition table FOO on column ID;\n" +
                "";
        DeploymentBuilder db = new DeploymentBuilder(2)
        .setUseAdHocDDL(true);
        ;
        Configuration config = Configuration.compile(getClass().getSimpleName(), ddl, db);
        assertNotNull("Configuration failed to compile", config);

        // Mutate the catalog to look old-school.  Revert version to 4.2.0.1
        // and move the autogen DDL to some other name.
        CatalogUpgradeTools.dorkDowngradeVersion(config, "4.2.0.1 voltdb-4.2.0.1");
        String pathToCatalog = config.m_pathToCatalog;
        File catFile = new File(pathToCatalog);
        InMemoryJarfile jarfile = CatalogUtil.loadInMemoryJarFile(MiscUtils.fileToBytes(catFile));
        byte[] sql = jarfile.get(VoltCompiler.AUTOGEN_DDL_FILE_NAME);
        jarfile.remove(VoltCompiler.AUTOGEN_DDL_FILE_NAME);
        jarfile.put("oldschool.sql", sql);
        catFile.delete();
        jarfile.writeToFile(catFile);

        try {
            startSystem(config);

            // Try to create a new table
            try {
                m_client.callProcedure("@AdHoc",
                        "create table bar (id integer);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to do DDL on an old loaded/upgraded catalog");
            }
            assertTrue(findTableInSystemCatalogResults("bar"));
        }
        finally {
            teardownSystem();
        }
    }
}
