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
package org.voltdb.regressionsuites;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.replication.SelectEmptyTable;

public class TestFailureDetectSuite extends RegressionSuite
{
    public void testMultiPartitionTxnAfterFailure()
    throws Exception
    {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("InsertMultiPart", 1,
                                                   "desc", 100, 14.5).getResults();
        assertEquals(1, results[0].asScalarLong());
        results = client.callProcedure("SelectMultiPart").getResults();
        System.out.println(results[0].toString());
        assertEquals(1, results[0].getRowCount());
        LocalCluster config = (LocalCluster) m_config;
        config.killSingleHost(0);
        for (int i = 0; i < 100; i++)
        {
            results = client.callProcedure("SelectMultiPart").getResults();
            assertEquals(1, results[0].getRowCount());
        }
    }

    public void testSinglePartitionTxnAfterFailure()
    throws Exception
    {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("InsertSinglePart", 1,
                                                   "desc", 100, 14.5).getResults();
        assertEquals(1, results[0].asScalarLong());
        results = client.callProcedure("SelectSinglePart", 1).getResults();
        System.out.println(results[0].toString());
        assertEquals(1, results[0].getRowCount());
        LocalCluster config = (LocalCluster) m_config;
        config.killSingleHost(0);
        // It takes a few transactions for the external volt process to actually
        // die, so we'll just try to do a bunch of transactions, and this will
        // be roughly like pulling the plug
        for (int i = 0; i < 100; i++)
        {
            results = client.callProcedure("UpdateSinglePart", 1, 200).getResults();
            assertEquals(1, results[0].getRowCount());
        }
    }

    public TestFailureDetectSuite(String name)
    {
        super(name);
    }

    static public junit.framework.Test suite()
    {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestFailureDetectSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(SelectEmptyTable.class.getResource("replication-ddl.sql"));
        project.addPartitionInfo("P1", "ID");
        project.addStmtProcedure("InsertSinglePart",
                                 "INSERT INTO P1 VALUES (?, ?, ?, ?);",
                                 "P1.ID: 0");
        project.addStmtProcedure("UpdateSinglePart",
                                 "UPDATE P1 SET P1.NUM = ? WHERE P1.ID = ?",
                                 "P1.ID: 0");
        project.addStmtProcedure("SelectSinglePart",
                                 "SELECT * FROM P1 WHERE P1.ID = ?",
                                 "P1.ID: 0");
        project.addStmtProcedure("InsertMultiPart",
                                 "INSERT INTO P1 VALUES (?, ?, ?, ?);");
        project.addStmtProcedure("SelectMultiPart", "SELECT * FROM P1");
        project.addStmtProcedure("UpdateMultiPart",
                                 "UPDATE P1 SET P1.NUM = ?");
        project.addStmtProcedure("InsertMultiPartRepl",
                                 "INSERT INTO R1 VALUES (?, ?, ?, ?);");
        project.addStmtProcedure("SelectMultiPartRepl", "SELECT * FROM R1");
        project.addStmtProcedure("UpdateMultiPartRepl",
                                 "UPDATE R1 SET R1.NUM = ?");

        // CLUSTER, two hosts, each with two sites, replication of 1
        config = new LocalCluster("replication-1-cluster.jar", 2, 2,
                                  1, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);

        // CLUSTER, three hosts, each with two sites, replication of 2
        config = new LocalCluster("replication-2-cluster.jar", 2, 3,
                                  2, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);

//        // CLUSTER, four hosts, each with three sites, replication of 2
//        config = new LocalCluster("replication-2-cluster.jar", 3, 4,
//                                  2, BackendTarget.NATIVE_EE_JNI);
//        config.compile(project);
//        builder.addServerConfig(config);
//
//        // CLUSTER, 3 hosts, each with one site, replication of 1
//        config = new LocalCluster("replication-odd-cluster.jar", 1, 3,
//                                  1, BackendTarget.NATIVE_EE_JNI);
//        config.compile(project);
//        builder.addServerConfig(config);

        return builder;
    }
}
