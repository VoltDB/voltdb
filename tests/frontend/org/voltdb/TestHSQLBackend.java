/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
import java.io.IOException;
import java.net.UnknownHostException;

import junit.framework.TestCase;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.BuildDirectoryUtils;

public class TestHSQLBackend extends TestCase {

    /*public void testMilestoneOneHSQL() throws InterruptedException, IOException, ProcCallException {
        String ddl =
            "CREATE TABLE WAREHOUSE (" +
            "W_ID INTEGER DEFAULT '0' NOT NULL, "+
            "W_NAME VARCHAR(16) DEFAULT NULL, " +
            "PRIMARY KEY  (W_ID)" +
            ");";
        File ddlFile = VoltProjectBuilder.writeStringToTempFile(ddl);
        String ddlPath = ddlFile.getPath();

        String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + ddlPath + "' /></schemas>" +
            "<procedures>" +
            "<procedure class='org.voltdb.compiler.procedures.MilestoneOneInsert' />" +
            "<procedure class='org.voltdb.compiler.procedures.MilestoneOneSelect' />" +
            "<procedure class='org.voltdb.compiler.procedures.MilestoneOneCombined' />" +
            "</procedures>" +
            "</database>" +
            "</project>";

        System.out.println(simpleProject);

        File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        String projectPath = projectFile.getPath();

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String catalogJar = testDir + File.separator + "milestoneOneHSQLDB.jar";

        VoltCompiler compiler = new VoltCompiler();
        ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        boolean success = compiler.compile(projectPath, cluster_config,
                                           catalogJar, System.out, null);

        assertTrue(success);

        // start VoltDB server using hsqlsb backend
        ServerThread server = new ServerThread( catalogJar, BackendTarget.HSQLDB_BACKEND);
        server.start();
        server.waitForInitialization();

        // run the test
        Client client = ClientFactory.createClient();
        client.createConnection("localhost", "program", "none");

        // call the insert procedure
        VoltTable[] results = client.callProcedure("MilestoneOneCombined", 99L, "TEST");
        // check one table was returned
        assertTrue(results.length == 1);
        // check one tuple was modified
        VoltTable result = results[0];
        VoltTableRow row = result.fetchRow(0);
        String resultStr = row.getString(0);
        assertTrue(resultStr.equals("TEST"));

        // stop execution
        VoltDB.instance().shutdown(server);
    }

    public void testAdHocEmptyQuery() throws InterruptedException, IOException, ProcCallException {
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addDefaultSchema();
        builder.addDefaultPartitioning();
        builder.addProcedures(SelectAll.class);

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String catalogJar = testDir + File.separator + "tpcchsql.jar";

        builder.compile(catalogJar, 1, 1, 0, "localhost");

        ServerThread server = new ServerThread(catalogJar, BackendTarget.HSQLDB_BACKEND);
        server.start();
        server.waitForInitialization();

        // run the test
        Client client = ClientFactory.createClient();
        client.createConnection("localhost", "program", "none");

        // call the insert procedure
        VoltTable[] results = client.callProcedure("@AdHoc", "select * from WAREHOUSE");
        // check one table was returned
        assertTrue(results.length > 0);
        assertTrue(results[0].getRowCount() == 0);

        server.shutdown();
        server.join();

        // stop execution
        VoltDB.instance().shutdown(server);
    }*/

    public void testDateInsertionAsLong() throws UnknownHostException, IOException, ProcCallException, InterruptedException {
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addDefaultSchema();
        builder.addDefaultPartitioning();
        System.out.println("\n\n\n STARTING STMT PROC ADD\n\n\n");
        builder.addStmtProcedure("InsertHistory", "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?);", "HISTORY.H_W_ID: 4");

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String catalogJar = testDir + File.separator + "tpcchsql.jar";

        builder.compile(catalogJar, 1, 1, 0, "localhost");

        ServerThread server = new ServerThread(catalogJar, BackendTarget.HSQLDB_BACKEND);
        server.start();
        server.waitForInitialization();

        // run the test
        Client client = ClientFactory.createClient();
        client.createConnection("localhost", "program", "none");

        // call the insert procedure
        VoltTable[] results = client.callProcedure("InsertHistory", 5, 5, 5, 5, 5, 100000L, 2.5, "nada");
        // check one table was returned
        assertTrue(results.length > 0);
        assertTrue(results[0].getRowCount() == 1);

        server.shutdown();
        server.join();

        // stop execution
        VoltDB.instance().shutdown(server);
    }

    /*public void testAdHocDateInsertionAsLong() throws UnknownHostException, IOException, ProcCallException, InterruptedException {
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addDefaultSchema();
        builder.addDefaultPartitioning();
        builder.addProcedures(SelectAll.class);

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String catalogJar = testDir + File.separator + "tpcchsql.jar";

        builder.compile(catalogJar, 1, 1, 0, "localhost");

        ServerThread server = new ServerThread(catalogJar, BackendTarget.HSQLDB_BACKEND);
        server.start();
        server.waitForInitialization();

        // run the test
        Client client = ClientFactory.createClient();
        client.createConnection("localhost", "program", "none");

        // call the insert procedure
        VoltTable[] results = client.callProcedure("@AdHoc", "INSERT INTO HISTORY VALUES (5, 5, 5, 5, 5, 100000, 2.5, 'nada');");
        // check one table was returned
        assertTrue(results.length > 0);
        assertTrue(results[0].getRowCount() == 1);

        server.shutdown();
        server.join();
        client.shutdown();

        // stop execution
        VoltDB.instance().shutdown(server);
    }*/
}
