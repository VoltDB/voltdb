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

package org.voltdb.planner;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.voltdb.BackendTarget;
import org.voltdb.ReplicationRole;
import org.voltdb.SQLStmt;
import org.voltdb.TableHelper;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

/**
 * Used for manually testing ENG-4009 performance impact.
 * Eventually this kind of test should be part of a broader performance tracking framework.
 */
public class ScanPerfTest extends TestCase {

    public static class ScanTable extends VoltProcedure {
        // count how many rows to scan
        final SQLStmt count = new SQLStmt("select count(*) as c from P");
        // scan all the rows
        final SQLStmt scan = new SQLStmt("select sum(VALUE) as s from P");
        // just to make the proc a write procedure
        final SQLStmt write = new SQLStmt("insert into P values (1,1)");

        public VoltTable run(long id) {
            voltQueueSQL(count);
            long count = voltExecuteSQL()[0].asScalarLong();

            voltQueueSQL(scan);
            long now = System.nanoTime();
            voltExecuteSQL();
            long duration = System.nanoTime() - now;
            //System.err.printf("duration: %d, %f\n", duration, duration / 1000000.0);

            VoltTable retval = new VoltTable(
                    new VoltTable.ColumnInfo("rowcount", VoltType.BIGINT),
                    new VoltTable.ColumnInfo("nanos", VoltType.BIGINT));
            retval.addRow(count, duration);
            return retval;
        }
    }

    static AtomicLong nanos = new AtomicLong(0);
    static AtomicLong rows = new AtomicLong(0);
    static AtomicLong scans = new AtomicLong(0);

    class ScanCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                System.err.printf(clientResponse.getStatusString());
                System.err.flush();
                System.exit(-1);
            }
            VoltTableRow row = clientResponse.getResults()[0].fetchRow(0);
            rows.addAndGet(row.getLong(0));
            nanos.addAndGet(row.getLong(1));
            scans.incrementAndGet();
        }
    }

    public static void fillTable(int mb, Client client, Random rand) throws Exception {
        final AtomicInteger outstanding = new AtomicInteger(0);

        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                outstanding.decrementAndGet();
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    System.err.printf(clientResponse.getStatusString());
                    System.err.flush();
                    System.exit(-1);
                }
            }
        };

        int i = 0;
        while (MiscUtils.getMBRss(client) < mb) {
            System.out.println("Loading 100000 rows");
            for (int j = 0; j < 100000; j++) {
                long rvalue = rand.nextInt();
                long absvalue = Math.abs(rvalue);
                long shift = absvalue << 30L;
                long id = shift + i++;
                outstanding.incrementAndGet();
                client.callProcedure(callback, "P.insert", id, 0);
            }
            while (outstanding.get() > 0) {
                Thread.yield();
            }
        }
    }

    public void testHaltLiveRejoinOnOverflow() throws Exception {

        LocalCluster cluster = null;
        Client client = null;

        VoltTable pTable = TableHelper.quickTable("P (ID:BIGINT-N, VALUE:BIGINT-N) PK(ID)");

        // build and compile a catalog
        System.out.println("Compiling catalog.");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(TableHelper.ddlForTable(pTable));
        builder.addLiteralSchema("PARTITION TABLE P ON COLUMN ID;\n" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.planner.ScanPerfTest$ScanTable;\n" +
                "PARTITION PROCEDURE ScanPerfTest$ScanTable ON TABLE P COLUMN ID;\n");
        cluster = new LocalCluster("scanperf.jar", 8, 1, 0, BackendTarget.NATIVE_EE_JNI);
        //cluster.setMaxHeap(10);
        boolean success = cluster.compile(builder);
        assertTrue(success);

        //fail();

        System.out.println("Starting cluster.");
        cluster.setHasLocalServer(false);
        cluster.overrideAnyRequestForValgrind();
        cluster.startUp(true, ReplicationRole.NONE);

        System.out.println("Getting client connected.");
        ClientConfig clientConfig = new ClientConfig();
        client = ClientFactory.createClient(clientConfig);
        for (String address : cluster.getListenerAddresses()) {
            client.createConnection(address);
        }

        System.out.println("Loading");

        Random r = new Random();

        // load up > 1gb data
        fillTable(6000, client, r);
        System.out.println("100% loaded.");

        client.drain();
        client.close();

        System.out.println("Getting client re-connected.");
        clientConfig = new ClientConfig();
        clientConfig.setProcedureCallTimeout(Long.MAX_VALUE);
        clientConfig.setMaxOutstandingTxns(50);
        client = ClientFactory.createClient(clientConfig);
        for (String address : cluster.getListenerAddresses()) {
            client.createConnection(address);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < 12; i++) {
            while ((System.currentTimeMillis() - start) < ((i+1) * 5000)) {
                client.callProcedure(new ScanCallback(), "ScanPerfTest$ScanTable", r.nextLong());
            }
            System.out.printf("Scanned at %.2f rows/sec when %.0f%% done.\n",
                    rows.get() / (nanos.get() / 1000000000.0), ((i+1) / 12.0) * 100);
            System.out.printf("%d scans on an average of %d rows/partition\n",
                    scans.get(), rows.get() / scans.get());
            System.out.printf("%.2f scans per second\n",
                    scans.get() / (nanos.get() / 1000000000.0));
        }

        System.out.println("Draining.");

        client.drain();
        client.close();

        System.out.printf("Scanned at %f rows/sec after drain.\n",
                rows.get() / (nanos.get() / 1000000000.0));

        cluster.shutDown();
    }

}
