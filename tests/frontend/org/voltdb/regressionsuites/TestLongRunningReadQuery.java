/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.voltdb.BackendTarget;
import org.voltdb.LRRHelper;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.CSVLoader;

public class TestLongRunningReadQuery extends RegressionSuite {

    private static int initTableSize = 100;
    private int m_insertCount = 0;
    private int m_deleteCount = 0;
    private int m_errorCount = 0;
    private static int m_primaryKeyId = 1;
    private volatile boolean m_receivedResponse = false;
    private static Random m_random = new Random();
    private static HashMap<String,ArrayList<Integer>> availableKeys = new HashMap<String,ArrayList<Integer>>();

    private final int OP_DELETE = 0;
    private final int OP_UPDATE = 1;
    private final int OP_INSERT = 2;


    private void resetStats() {

        initTableSize = initTableSize + m_insertCount - m_deleteCount;
        m_insertCount = 0;
        m_deleteCount = 0;
        m_receivedResponse = false;
        m_errorCount = 0;
    }

    private static String getTupleStringFromPrimaryKey(int primaryKey) {
        String tuple = "(" + primaryKey;
        for (int i = 0; i < 9; i++) {
            tuple += "," + m_random.nextInt();
        }
        tuple += ")";
        return tuple;
    }

    private void doRandomTableManipulation(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        int type = m_random.nextInt(3);
        if (type == OP_DELETE) {
            doRandomDelete(client, tableName);
        } else if (type == OP_UPDATE) {
            doRandomUpdate(client, tableName);
        } else if (type == OP_INSERT) {
            doRandomInsert(client, tableName);
        } else {
            assert(false);
        }
    }

    private void doRandomUpdate(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        if (availableKeys.get(tableName).isEmpty())
            return;
        int keyIdx = m_random.nextInt(availableKeys.get(tableName).size());
        String sql = "update " + tableName + " set " + tableName + ".col1 = " + m_random.nextInt()
            + "where " + tableName + ".id = " + availableKeys.get(tableName).get(keyIdx);
        client.callProcedure("@AdHoc", sql);
    }

    private void doRandomInsert(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {

        String sql = "insert into " + tableName + " values " + getTupleStringFromPrimaryKey(m_primaryKeyId);
        availableKeys.get(tableName).add(m_primaryKeyId);
        client.callProcedure("@AdHoc",sql);
        m_primaryKeyId++;
        m_insertCount++;
    }

    private void doRandomDelete(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        if (availableKeys.get(tableName).isEmpty())
            return;
        int keyIdx = m_random.nextInt(availableKeys.get(tableName).size());
        String sql = "delete from " + tableName + " where " + tableName + ".id = " + availableKeys.get(tableName).get(keyIdx);
        availableKeys.get(tableName).remove(keyIdx);
        client.callProcedure("@AdHoc",sql);
        m_deleteCount++;
    }

    private String loadTableAdHoc(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        ArrayList<Integer> keys = new ArrayList<Integer>(initTableSize);
        availableKeys.put(tableName, keys);
        String sql = "";
        for (int i = 0; i < initTableSize; i++) {
            sql += "INSERT INTO " + tableName + " VALUES " + getTupleStringFromPrimaryKey(m_primaryKeyId) + ";";
            availableKeys.get(tableName).add(m_primaryKeyId);
            m_primaryKeyId++;
        }
        client.callProcedure("@AdHoc", sql);
        return sql;
    }

    private void loadTableCSV(String tableName, String filePath) throws IOException, InterruptedException{
        String []myOptions = {
                "-f" + filePath,
                "--port=21312",
                "--limitrows=" + initTableSize,
                tableName
        };
        CSVLoader.testMode = true;
        CSVLoader.main(myOptions);

        ArrayList<Integer> keys = new ArrayList<Integer>(initTableSize);
        for (int i = 0; i < initTableSize; i++) {
            keys.add(i);
        }
        availableKeys.put(tableName, keys);

    }

    public void testLongRunningReadQuery() throws IOException, ProcCallException, InterruptedException {
         System.out.println("testLongRunningReadQuery...");

         Client client = getClient();

         loadTableAdHoc(client, "R1");

         resetStats();

         subtest1Select(client);

         resetStats();

         subtest2ConcurrentReadError(client);

    }


    private void executeLRR(Client client, String sql) throws NoConnectionsException, IOException {
        client.callProcedure(new ProcedureCallback() {
            @Override
            public void clientCallback(final ClientResponse clientResponse)
                    throws Exception {
                VoltTable vt;
                VoltTable files;

                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    assert(clientResponse.getStatusString().equals("Concurrent @ReadOnlySlow calls are not supported."));
                    ++m_errorCount;
                    return;
                }
                files = clientResponse.getResults()[0];
                vt = LRRHelper.getTableFromFileTable(files);
                assertEquals(initTableSize,vt.getRowCount());
                m_receivedResponse = true;
            }
        },"@ReadOnlySlow", sql);
    }

    public void subtest1Select(Client client) throws IOException, ProcCallException {
        System.out.println("subtest1Select...");
        String sql;
        m_receivedResponse = false;

        sql = "SELECT * FROM R1;";
        executeLRR(client,sql);
        while (!m_receivedResponse) {
            // Do random ad hoc queries until the long running read returns
            doRandomTableManipulation(client, "R1");

        }
        //System.out.println(initTableSize + " " + m_insertCount + " " + m_deleteCount);
        VoltTable vt = client.callProcedure("@AdHoc",sql).getResults()[0];
        assertEquals(initTableSize + m_insertCount - m_deleteCount,vt.getRowCount());
    }

    public void subtest2ConcurrentReadError(Client client) throws IOException, ProcCallException {
        System.out.println("subtest2ConcurrentReadError...");
        String sql;
        m_receivedResponse = false;

        sql = "SELECT * FROM R1;";
        executeLRR(client,sql);
        executeLRR(client,sql);
        while (!m_receivedResponse) {
            // Wait for LRR to finish
        }
        assertEquals(1, m_errorCount);
        VoltTable vt = client.callProcedure("@AdHoc",sql).getResults()[0];
        assertEquals(initTableSize + m_insertCount - m_deleteCount,vt.getRowCount());
    }


    //
    // Suite builder boilerplate
    //

    public TestLongRunningReadQuery(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestLongRunningReadQuery.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        String literalSchema =
                "CREATE TABLE R1 ( " +
                " ID INT DEFAULT 0 NOT NULL"
                + ", COL1 INT "
                + ", COL2 INT "
                + ", COL3 INT "
                + ", COL4 INT "
                + ", COL5 INT "
                + ", COL6 INT "
                + ", COL7 INT "
                + ", COL8 INT "
                + ", COL9 INT "
                + ");"
                + ""
                + "CREATE TABLE R2 ( "
                + " ID INT DEFAULT 0 NOT NULL"
                + ", COL1 INT "
                + ", COL2 INT "
                + ", COL3 INT "
                + ", COL4 INT "
                + ", COL5 INT "
                + ", COL6 INT "
                + ", COL7 INT "
                + ", COL8 INT "
                + ", COL9 INT "
                + ");"
                + ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        boolean success;
        project.setLongReadSettings(10);

        config = new LocalCluster("longreads-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
