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
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestIndexMemoryOwnershipSuite extends RegressionSuite {

    VoltTable getTable(ClientResponse response) {
        assertTrue(response.getStatus() == ClientResponse.SUCCESS);
        VoltTable[] tables = response.getResults();
        assertTrue(tables.length == 1);
        return tables[0];
    }

    VoltTable getSingleRowTable(ClientResponse response) {
        VoltTable table = getTable(response);
        assertEquals(1, table.getRowCount());
        return table;
    }

    long getLongFromResponse(ClientResponse response) {
        return getSingleRowTable(response).asScalarLong();
    }

    public void testNonIndexHittingUpdates() throws Exception {
        final Client client = this.getClient();
        ClientResponse response;
        VoltTable table;

        response = client.callProcedure("InsertT1", "a", "b", "c");
        assertEquals(1, getLongFromResponse(response));

        response = client.callProcedure("UpdateT1c", "c2", "a");
        assertEquals(1, getLongFromResponse(response));

        response = client.callProcedure("LookupT1b", "b");
        table = getSingleRowTable(response);
        System.out.println(table.toJSONString());
    }

    public void testMultimapKeyOwnership() throws Exception {
        final Client client = this.getClient();
        ClientResponse response;
        VoltTable table;

        response = client.callProcedure("InsertT1", "a1", "b", "c1");
        assertEquals(1, getLongFromResponse(response));

        response = client.callProcedure("InsertT1", "a2", "b", "c2");
        assertEquals(1, getLongFromResponse(response));

        response = client.callProcedure("DeleteT1", "c1");
        assertEquals(1, getLongFromResponse(response));

        response = client.callProcedure("LookupT1b", "b");
        table = getSingleRowTable(response);
        System.out.println(table.toJSONString());

        // Try to repro a string memory management related crash when a no-op update corrupts an index.
        response = client.callProcedure("UpdateT1b", "b", "a2");
        assertEquals(1, getLongFromResponse(response));

        // This will cause a server fatal error if the corruption happened.
        response = client.callProcedure("UpdateT1b", "b2", "a2");
        assertEquals(1, getLongFromResponse(response));
    }

    public void testMatViewUpdates() throws Exception {
        final Client client = this.getClient();
        ClientResponse response;
        VoltTable table;

        System.out.println("RUNNING InsertT1");
        System.out.flush();
        response = client.callProcedure("InsertT1", "a", "b", "c1");
        assertEquals(1, getLongFromResponse(response));

        System.out.println("RUNNING InsertT1");
        System.out.flush();
        response = client.callProcedure("InsertT1", "a", "b", "c2");
        assertEquals(1, getLongFromResponse(response));

        System.out.println("RUNNING MVAll");
        System.out.flush();
        response = client.callProcedure("MVAll");
        table = getSingleRowTable(response);
        System.out.println(table.toJSONString());

        System.out.println("RUNNING MVLookup");
        System.out.flush();
        response = client.callProcedure("MVLookup", "b", "a");
        table = getSingleRowTable(response);
        System.out.println(table.toJSONString());

        System.out.println("RUNNING DeleteT1");
        System.out.flush();
        response = client.callProcedure("DeleteT1", "c1");
        assertEquals(1, getLongFromResponse(response));

        System.out.println("RUNNING MVLookup");
        System.out.flush();
        response = client.callProcedure("MVLookup", "b", "a");
        table = getSingleRowTable(response);
        System.out.println(table.toJSONString());
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestIndexMemoryOwnershipSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestIndexMemoryOwnershipSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestIndexMemoryOwnershipSuite.class.getResource("testindexmemoryownership-ddl.sql"));
        project.addPartitionInfo("t1", "a");
        project.addStmtProcedure("InsertT1", "insert into t1 values (?, ?, ?);", "t1.a:0");
        project.addStmtProcedure("UpdateT1c", "update t1 set c = ? where a = ?;", "t1.a:1");
        project.addStmtProcedure("UpdateT1b", "update t1 set b = ? where a = ?;", "t1.a:1");
        project.addStmtProcedure("DeleteT1", "delete from t1 where c = ?;");
        project.addStmtProcedure("LookupT1b", "select * from t1 where b = ?;");
        project.addStmtProcedure("MVLookup", "select * from mv where b = ? and a = ?;", "t1.a:1");
        project.addStmtProcedure("MVAll", "select * from mv;");

        boolean success;

        // JNI
        config = new LocalCluster("updatememoryownership.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
