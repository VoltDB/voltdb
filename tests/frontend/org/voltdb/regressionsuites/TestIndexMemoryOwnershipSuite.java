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

import junit.framework.TestCase;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.CatalogBuilder;

public class TestIndexMemoryOwnershipSuite extends RegressionSuite {
    private static final Class<? extends TestCase> TESTCASECLASS = TestIndexMemoryOwnershipSuite.class;

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
        CatalogBuilder cb = new CatalogBuilder()
        .addSchema(TESTCASECLASS.getResource("testindexmemoryownership-ddl.sql"))
        .addPartitionInfo("t1", "a")
        .addStmtProcedure("InsertT1", "insert into t1 values (?, ?, ?);", "t1.a", 0)
        .addStmtProcedure("UpdateT1c", "update t1 set c = ? where a = ?;", "t1.a", 1)
        .addStmtProcedure("UpdateT1b", "update t1 set b = ? where a = ?;", "t1.a", 1)
        .addStmtProcedure("DeleteT1", "delete from t1 where c = ?;")
        .addStmtProcedure("LookupT1b", "select * from t1 where b = ?;")
        .addStmtProcedure("MVLookup", "select * from mv where b = ? and a = ?;", "t1.a", 1)
        .addStmtProcedure("MVAll", "select * from mv;")
        ;
        LocalCluster cluster = LocalCluster.configure(TESTCASECLASS.getSimpleName(), cb, 1);
        assertNotNull("LocalCluster compile failed", cluster);
        return new MultiConfigSuiteBuilder(TESTCASECLASS, cluster);
    }
}
