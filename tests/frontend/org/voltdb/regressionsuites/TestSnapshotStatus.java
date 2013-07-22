/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.io.IOException;

import org.voltdb.client.Client;

import org.voltdb.VoltTable;

import org.voltdb_testprocs.regressionsuites.SaveRestoreBase;
import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestSnapshotStatus extends SaveRestoreBase {

    public TestSnapshotStatus(String name) {
        super(name);
    }

    public void testSnapshotStatus() throws Exception
    {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, 1).getResults();
        System.out.println(results[0]);
        // better be four rows, one for each execution site, in the blocking save results:
        assertEquals(4, results[0].getRowCount());
        results = client.callProcedure("@SnapshotStatus").getResults();
        System.out.println(results[0]);
        // better be four rows, one for each table at each node, in the status results:
        assertEquals(4, results[0].getRowCount());
        // better not be any zeros in the completion time
        while (results[0].advanceRow()) {
            long completed = results[0].getLong("END_TIME");
            assertTrue("END_TIME was not filled", completed != 0);
        }
    }

    // Regression test for ENG-4802
    public void testCsvSnapshotStatus() throws Exception
    {
        Client client = getClient();
        // Lazy man's JSON construction for snapshot opts
        String json = "{uripath:\"file:///" + TMPDIR + "\", nonce:\"" + TESTNONCE + "\",block:true, format:\"csv\"}";
        VoltTable[] results = client.callProcedure("@SnapshotSave", json).getResults();
        System.out.println(results[0]);
        results = client.callProcedure("@SnapshotStatus").getResults();
        System.out.println(results[0]);
        // better be three rows, one for the replicated table, and one for each partition of the partitioned table
        assertEquals(3, results[0].getRowCount());
        // better not be any zeros in the completion time
        while (results[0].advanceRow()) {
            long completed = results[0].getLong("END_TIME");
            assertTrue("END_TIME was not filled", completed != 0);
        }
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException
    {
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSnapshotStatus.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema("CREATE TABLE T(A1 INTEGER NOT NULL, A2 INTEGER, PRIMARY KEY(A1));" +
                "CREATE TABLE R(R1 INTEGER NOT NULL, R2 INTEGER);");
        project.addPartitionInfo("T", "A1");
        project.addStmtProcedure("InsertA", "INSERT INTO T VALUES(?,?);", "T.A1: 0");

        LocalCluster lcconfig = new LocalCluster("testsnapshotstatus.jar", 2, 2, 1,
                                               BackendTarget.NATIVE_EE_JNI);
        lcconfig.compile(project);
        builder.addServerConfig(lcconfig);

        return builder;
    }
}


