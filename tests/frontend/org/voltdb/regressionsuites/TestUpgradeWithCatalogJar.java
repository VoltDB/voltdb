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

package org.voltdb.regressionsuites;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URL;

import org.junit.Test;
import org.voltdb.AdhocDDLTestBase;
import org.voltdb.StartAction;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestUpgradeWithCatalogJar extends AdhocDDLTestBase  {

    @Test
    public void testUpgradeWithCatalogJar() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
//        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();

        // jar was compiled with version v7.4 and will be upgraded to current version
        URL catalogJarPath = TestUpgradeWithCatalogJar.class.getResource("upgradeCatalogV7_4.jar");
        config.m_pathToCatalog = catalogJarPath.getPath();
        config.m_pathToDeployment = pathToDeployment;
        config.m_startAction = StartAction.CREATE;

        try {
            startSystem(config);

            assertFalse(findTableInSystemCatalogResults("FOO"));

            assertTrue(findTableInSystemCatalogResults("CONTESTANTS"));
            assertTrue(findTableInSystemCatalogResults("VOTES"));

            m_client.callProcedure("@AdHoc", "insert into votes values (4015671234, 'RI', 1);");
            m_client.callProcedure("VOTES.insert", 4012345678L, "RI", 2);

            VoltTable vt = m_client.callProcedure("@AdHoc", "select count(*) from votes;").getResults()[0];
            assertTrue(vt.asScalarLong() == 2);

        }
        finally {
            teardownSystem();
        }
    }
}
