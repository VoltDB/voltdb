/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.regressionsuites.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.voltdb.AdhocDDLTestBase;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestProcedureStats extends AdhocDDLTestBase {

    private void checkUAC(long timedInvocations) throws Exception {
        VoltTable vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
        assertEquals(1, vt.getRowCount());
        vt.advanceRow();
        assertEquals("org.voltdb.sysprocs.UpdateApplicationCatalog", vt.getString(5));
        assertEquals(timedInvocations, vt.getLong(7));
    }

    private void checkKeepedStats(String[] procs) throws Exception {
        VoltTable vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
        assertEquals(procs.length, vt.getRowCount());
        Set<String> procsSet = new HashSet<String>(Arrays.asList(procs));
        while (vt.advanceRow()) {
            assertTrue(procsSet.contains(vt.getString(5)));
        }
    }


    @Test
    public void testUACSystemStatsKeeped() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);

            VoltTable vt = null;

            vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
            assertEquals(0, vt.getRowCount());

            vt = m_client.callProcedure("@AdHoc", "create table tb1 (a int primary key);").getResults()[0];
            assertEquals(1, vt.getRowCount());
            checkUAC(1);

            vt = m_client.callProcedure("TB1.insert", 1).getResults()[0];
            assertEquals(1, vt.getRowCount());

            vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
            checkKeepedStats(new String[]{"TB1.insert", "org.voltdb.sysprocs.UpdateApplicationCatalog"});

            String ddl = "create table tb2 (a int not null unique); partition table tb2 on column a;";
            vt = m_client.callProcedure("@AdHoc", ddl).getResults()[0];
            // UAC called, only UAC system stats left
            checkUAC(2);

            //
            // call more user stats & other system stats
            //
            vt = m_client.callProcedure("TB1.insert", 2).getResults()[0];
            assertEquals(1, vt.getRowCount());
            vt = m_client.callProcedure("TB1.delete", 3).getResults()[0];
            assertEquals(1, vt.getRowCount());
            vt = m_client.callProcedure("TB1.upsert", 3).getResults()[0];
            assertEquals(1, vt.getRowCount());

            vt = m_client.callProcedure("TB2.insert", 1).getResults()[0];
            assertEquals(1, vt.getRowCount());
            vt = m_client.callProcedure("TB2.insert", 2).getResults()[0];
            assertEquals(1, vt.getRowCount());
            vt = m_client.callProcedure("TB2.insert", 3).getResults()[0];
            assertEquals(1, vt.getRowCount());
            vt = m_client.callProcedure("TB2.insert", 4).getResults()[0];
            assertEquals(1, vt.getRowCount());
            vt = m_client.callProcedure("TB2.insert", 5).getResults()[0];
            assertEquals(1, vt.getRowCount());

            // call other system stats
            m_client.callProcedure("@ExplainProc", "TB1.insert");

            vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
            checkKeepedStats(new String[]{"TB1.insert", "TB1.delete", "TB1.upsert",
                    "TB2.insert", "TB2.insert", // two records for each partition (SP procedure)
                    "org.voltdb.sysprocs.UpdateApplicationCatalog"});

            vt = m_client.callProcedure("@AdHoc", "alter table tb1 add column b int;").getResults()[0];
            assertEquals(1, vt.getRowCount());
            // UAC called, only UAC system stats left
            checkUAC(3);
        }
        finally {
            teardownSystem();
        }
    }
}
