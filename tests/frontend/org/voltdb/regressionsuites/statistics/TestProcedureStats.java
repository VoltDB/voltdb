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

package org.voltdb.regressionsuites.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.voltdb.AdhocDDLTestBase;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestProcedureStats extends AdhocDDLTestBase {

    private void checkStats() throws Exception {
        VoltTable vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
        assertEquals(0, vt.getRowCount());
    }

    private void checkKeepedStats(String[] procs) throws Exception {
        VoltTable vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
        assertEquals(procs.length, vt.getRowCount());
        Set<String> procsSet = new HashSet<String>(Arrays.asList(procs));
        while (vt.advanceRow()) {
            assertTrue(procsSet.contains(vt.getString(5)));
        }
    }

    /* check the procedure detail stats for a procedure
     * proc - name of the stored procedure
     * stmts - the number of statements for the stored procedure "proc"
     * partition - single or multi partitioned query
     * the tests seems to run on 2 partitions - so partition = 2 for multi partition query
     */
    private void checkDetailStats(String proc, int stmts, int partition) throws Exception {
        VoltTable vt = m_client.callProcedure("@Statistics", "PROCEDUREDETAIL", 0).getResults()[0];
        // map to store the count of number of times each statement is called in the "proc" stored procedure
        Map<String, Integer> stmtMap = new HashMap<String, Integer>();
        stmtMap.put("<ALL>", 0);

        for ( int i = 0; i < stmts ; i++ ) {
            stmtMap.put("sql"+ String.valueOf(i), 0);
        }

        while (vt.advanceRow()) {
            if(proc.equals(vt.getString(5))) {
                String sql = vt.getString(6);
                stmtMap.put(sql, stmtMap.get(sql) + 1);
            }
        }

        // get the count of number of times each sql statement was called
        // if its multipartitioned, it should be called once per each partition
        for ( int i = 0; i < stmts ; i++ ) {
            assertEquals(stmtMap.get("sql"+ String.valueOf(i)).intValue(), 1 * partition);
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
            checkStats();

            vt = m_client.callProcedure("TB1.insert", 1).getResults()[0];
            assertEquals(1, vt.getRowCount());

            vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
            checkKeepedStats(new String[]{"TB1.insert"});

            String ddl = "create table tb2 (a int not null unique); partition table tb2 on column a;";
            vt = m_client.callProcedure("@AdHoc", ddl).getResults()[0];
            // UAC called, only UAC system stats left
            checkStats();

            vt = m_client.callProcedure("@AdHoc", "create procedure mspSingle "
                    + "partition on table tb2 "
                    + "column a parameter 0 "
                    + "as begin "
                    + "insert into tb2 values (?); select * from tb2; end;").getResults()[0];
            assertEquals(1, vt.getRowCount());
            checkStats();

            vt = m_client.callProcedure("@AdHoc", "create procedure mspMultiple as begin "
                    + "insert into tb2 values (?); select * from tb2; end;").getResults()[0];
            assertEquals(1, vt.getRowCount());
            checkStats();

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

            vt = m_client.callProcedure("mspSingle", 6).getResults()[0];
            assertEquals(1, vt.getRowCount());

            vt = m_client.callProcedure("@Statistics", "PROCEDUREDETAIL", 0).getResults()[0];
            System.out.println(vt);

            vt = m_client.callProcedure("mspMultiple", 7).getResults()[0];
            assertEquals(1, vt.getRowCount());

            vt = m_client.callProcedure("@Statistics", "PROCEDUREDETAIL", 0).getResults()[0];
            System.out.println(vt);

            checkDetailStats("mspSingle", 2, 1);
            checkDetailStats("mspMultiple", 2, 2);

            // call other system stats
            m_client.callProcedure("@ExplainProc", "TB1.insert");

            vt = m_client.callProcedure("@Statistics", "PROCEDURE", 0).getResults()[0];
            checkKeepedStats(new String[]{"TB1.insert", "TB1.delete", "TB1.upsert",
                    "TB2.insert", "TB2.insert", // two records for each partition (SP procedure)
                    "mspSingle", "mspMultiple",
                    });

            vt = m_client.callProcedure("@AdHoc", "alter table tb1 add column b int;").getResults()[0];
            assertEquals(1, vt.getRowCount());
            // UAC called, all system stats are cleared
            checkStats();
        }
        finally {
            teardownSystem();
        }
    }
}
