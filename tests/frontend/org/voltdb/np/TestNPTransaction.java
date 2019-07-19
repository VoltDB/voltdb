/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.np;

import static org.junit.Assert.*;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;


public class TestNPTransaction {
    public LocalCluster cluster;
    public Client client;

    @Test
    public void Test2PartitionTxn() throws Exception {
        setup();
        class NullProcedureCallback implements ProcedureCallback {
            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                assertEquals(ClientResponse.SUCCESS, clientResponse.getStatus());
            }
        }

        NullProcedureCallback nullcallback = new NullProcedureCallback();

        for (int i = 0; i < 1000; i++) {
            client.callProcedure("@AdHoc", "INSERT INTO table1 VALUES (" + i + ", 1000, 'wx" + i + "');");
        }

        for (int i = 0; i < 200; i += 2) {
            client.callProcedure(nullcallback, "TestNPTransaction$Transfer", i, 500 + i, 1);

            if ( i % 4 == 0) {
                client.callProcedure(nullcallback, "@AdHoc", "INSERT INTO table1 VALUES (" + i + 1000 + ", 100, 'wx" + i + "');");
            }
            if ( i % 4 == 1) {
                client.callProcedure(nullcallback, "@AdHoc", "select * from table1 where id < 300 order by id limit 10;");
            }
        }

        client.drain();

        cleanup();
    }

    private void setup() throws Exception {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.setUseDDLSchema(true);

        final String schema = "CREATE TABLE table1 (id int PRIMARY KEY not null, value int, dummy varchar(30));"
                + "PARTITION TABLE table1 ON COLUMN id;"
                + "";

        builder.addLiteralSchema(schema);
        builder.addSupplementalClasses(TestNPTransaction.Transfer.class);

        cluster = new LocalCluster("test2ptxn.jar", 8, 2, 0,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING,
                true,  null);
        cluster.setNewCli(true);
        cluster.setHasLocalServer(false);

        assertTrue(cluster.compile(builder));
        cluster.startUp();

        ClientConfig config = new ClientConfig();
        config.setProcedureCallTimeout(1000 * 1000);
        config.setConnectionResponseTimeout(1000 * 1000);
        client = ClientFactory.createClient();
        for (String s : cluster.getListenerAddresses()) {
            client.createConnection(s);
        }

        client.callProcedure("@AdHoc", "CREATE PROCEDURE PARTITION ON TABLE table1 COLUMN id AND ON TABLE table1 COLUMN id FROM "
                + "CLASS org.voltdb.np.TestNPTransaction$Transfer");
    }

    private void cleanup() throws Exception {
        try {
            if (client != null) { client.close(); }
        } finally {
            if (cluster != null) { cluster.shutDown(); }
        }
    }

    public static class Transfer extends VoltProcedure {
        public final SQLStmt get = new SQLStmt("SELECT * FROM table1 WHERE id = ?;");

        public final SQLStmt update = new SQLStmt("UPDATE table1 SET " +
                " value = ?" +
                " WHERE id = ?;");

        public long run(int id1, int id2, int value) throws VoltAbortException {
            voltQueueSQL(get, id1);
            voltQueueSQL(get, id2);
            VoltTable results[] = voltExecuteSQL(false);
            if (results.length != 2) {
                throw new VoltAbortException("No id account found for " + id1 + ", " + id2);
            }

            VoltTableRow r1 = results[0].fetchRow(0);
            int val1 = (int) r1.getLong(1);

            VoltTableRow r2 = results[0].fetchRow(0);
            int val2 = (int) r2.getLong(1);

            if (val1 < value) {
                throw new VoltAbortException("Negative value after transfer " + value);
            }

            voltQueueSQL(update, val1 - value, id1);
            voltQueueSQL(update, val2 + value, id2);
            voltExecuteSQL(true);
            return 1;
        }
    }
}