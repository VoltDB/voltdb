/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.benchmark.appupdate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.ClientMain;
import org.voltdb.benchmark.Verification.ForeignKeyConstraintBase;
import org.voltdb.benchmark.appupdate.procs.InsertA;
import org.voltdb.benchmark.appupdate.procs.InsertB;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.NotImplementedException;

/**
 * This benchmark uses three tables: A, B, FK. Table FK is replicated and maps
 * an int and a string to a payload. InsertA does a lookup on payload using the
 * int and inserts the corresponding payload into A. InsertB does a lookup in FK
 * of the payload using the string and inserts the corresponding payload into B.
 * Concurrently with this workload, with some small probability, an
 * UpdateApplicationCatalog procedure is run to change at random to a catalog
 * defining A, B, or both A and B.
 *
 * Consequently, lots of calls to InsertA and InsertB should fail. This failure
 * is expected and is counted. Likewise, some number of catalog updates will
 * fail - they will diff v. an out of date catalog and be caught by the
 * ExecutionSite as stale. These are also counted.
 *
 * Note that this is an insert only workload. The system will run out of memory
 * unless tables are correctly dropped as a consequence of a catalog update.
 *
 * Finally, the payload lookup scheme estalishes a foreign key constraint
 * between A and FK and B and FK. This is validated by the checkTransaction
 * constraint benchmark feature, which uses snapshots.
 *
 * This test could/should be extended to include: * export only tables * export
 * clients * drops of replicated tables * multi-partition workload (maybe
 * UpdatePayload). * AdHoc (see ENG-635)
 */

public class AppUpdate extends ClientMain {
    // Data read via reflection by BenchmarkController
    public static final Class<? extends VoltProjectBuilder> m_projectBuilderClass = AppUpdateProjectBuilder.class;
    public static final Class<? extends ClientMain> m_loaderClass = null;

    public static enum Transaction {
        InsertA("InsertA"),
        InsertAFail("InsertA Failed"),
        InsertB("InsertB"),
        InsertBFail("InsertB Failed"),
        Load("Load"),
        CatalogAB("appupdate_ab.jar"),
        CatalogA("appupdate_a.jar"),
        CatalogB("appupdate_b.jar"),
        CatalogABFail("appupdate_ab.jar Failed"),
        CatalogAFail("appupdate_a.jar Failed"),
        CatalogBFail("appupdate_b.jar Failed");
        private Transaction(String name) {
            this.name = name;
        }

        public final String name;
    }

    /**
     * Project builder for app update.
     */
    public static class AppUpdateProjectBuilder extends VoltProjectBuilder {

        public AppUpdateProjectBuilder() {
        }

        /**
         * Produce 3 catalogs for table A and table B in the combinations of
         * (A), (B) and (A,B).
         */
        @Override
        public String[] compileAllCatalogs(int sitesPerHost, int length,
                int kFactor, String voltRoot) {
            String tableA = "CREATE TABLE A (PID INTEGER NOT NULL, I INTEGER, PAYLOAD VARCHAR(128));";
            String tableB = "CREATE TABLE B (PID INTEGER NOT NULL, S VARCHAR(6), PAYLOAD VARCHAR(128));";
            String tableFK = "CREATE TABLE FK (I INTEGER, S VARCHAR(6), PAYLOAD VARCHAR(128));";

            String loadSQL = "INSERT INTO FK VALUES (?,?,?);";

            try {
                VoltProjectBuilder pba = new VoltProjectBuilder();
                pba.addProcedures(InsertA.class);
                pba.addStmtProcedure(Transaction.Load.name, loadSQL);
                pba.addLiteralSchema(tableA);
                pba.addLiteralSchema(tableFK);
                pba.addPartitionInfo("A", "PID");
                if (!pba.compile(Transaction.CatalogA.name, sitesPerHost,
                        length, kFactor)) {
                    throw new RuntimeException(
                            "AppUpdate project builder failed app compilation (a).");
                }

                VoltProjectBuilder pbb = new VoltProjectBuilder();
                pbb.addProcedures(InsertB.class);
                pbb.addStmtProcedure(Transaction.Load.name, loadSQL);
                pbb.addLiteralSchema(tableB);
                pbb.addLiteralSchema(tableFK);
                pbb.addPartitionInfo("B", "PID");
                if (!pbb.compile(Transaction.CatalogB.name, sitesPerHost,
                        length, kFactor)) {
                    throw new RuntimeException(
                            "AppUpdate project builder failed app compilation (b)");
                }

                VoltProjectBuilder pb = this; // new VoltProjectBuilder();
                pb.addProcedures(InsertA.class, InsertB.class);
                pb.addStmtProcedure(Transaction.Load.name, loadSQL);
                pb.addLiteralSchema(tableA);
                pb.addLiteralSchema(tableB);
                pb.addLiteralSchema(tableFK);
                pb.addPartitionInfo("A", "PID");
                pb.addPartitionInfo("B", "PID");

                if (!pb.compile(Transaction.CatalogAB.name, sitesPerHost,
                        length, kFactor)) {
                    throw new RuntimeException(
                            "AppUpdate project builder failed app compilation (ab)");
                }

            } catch (IOException e) {
                throw new RuntimeException(
                        "AppUpdate project builder failed app compilation (io)");
            }
            return new String[] { Transaction.CatalogAB.name,
                    Transaction.CatalogA.name, Transaction.CatalogB.name };
        }

        @Override
        public void addAllDefaults() {
            throw new NotImplementedException(
                    "Not implemented for AppUpdate Project Builder.");
        }
    }

    class AppUpdateCallback implements ProcedureCallback {
        final Transaction t;

        AppUpdateCallback(Transaction t) {
            this.t = t;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() == ClientResponse.CONNECTION_LOST) {
                return;
            }

            if (!checkTransaction(t.name, clientResponse, false, false)) {
                if (clientResponse.getStatusString() != null) {
                    if (clientResponse.getStatusString().contains("Procedure does not exist: InsertA") ||
                        clientResponse.getStatusString().contains("Procedure InsertA was not found"))
                    {
                        m_counts[Transaction.InsertAFail.ordinal()].incrementAndGet();
                    }
                    else if (clientResponse.getStatusString().contains("Procedure does not exist: InsertB") ||
                             clientResponse.getStatusString().contains("Procedure InsertB was not found"))
                    {
                        m_counts[Transaction.InsertBFail.ordinal()].incrementAndGet();
                    }
                    else if (clientResponse.getStatusString().contains("Trying to update main catalog context with diff commands generated for an out-of date catalog."))
                    {
                        if (t == Transaction.CatalogA)
                            m_counts[Transaction.CatalogAFail.ordinal()].incrementAndGet();
                        else if (t == Transaction.CatalogB)
                            m_counts[Transaction.CatalogBFail.ordinal()].incrementAndGet();
                        else if (t == Transaction.CatalogAB)
                            m_counts[Transaction.CatalogABFail.ordinal()].incrementAndGet();
                        else
                            throw new RuntimeException("Unexpected failure: " + clientResponse.getStatusString());
                    }
                    else {
                        throw new RuntimeException("Error. Unexpected failure from harness: " + clientResponse.getStatusString());
                    }
                }

                if (clientResponse.getException() != null) {
                    clientResponse.getException().printStackTrace();
                }
            }
            else {
                m_counts[t.ordinal()].incrementAndGet();
            }
        }
    }

    public AppUpdate(Client client) {
        super(client);
        setupVerificationConstraints();
    }

    public AppUpdate(String args[]) {
        super(args);
        setupVerificationConstraints();
    }

    @Override
    protected String[] getTransactionDisplayNames() {
        String names[] = new String[Transaction.values().length];
        int ii = 0;
        for (Transaction transaction : Transaction.values()) {
            names[ii++] = transaction.name;
        }
        return names;
    }

    @Override
    protected String getApplicationName() {
        return "AppUpdate";
    }

    @Override
    protected String getSubApplicationName() {
        return "Client";
    }

    @Override
    protected void runLoop() throws IOException, InterruptedException {
        while (true) {
            m_voltClient.backpressureBarrier();
            runOnce();
        }
    }

    Random m_rand = new Random(); // generates all random numbers
    int tuplesLoaded = 0; // number of tuples loaded
    int nextA = 0; // next A.PID
    int nextB = 0; // next B.PID

    // need 10,000 unique strings for 10,000 unique ints.
    // seems pretty straightforward
    String getStringKey(int fkrow) {
        return new Integer(fkrow).toString();
    }

    // Need 128 byte string
    String getPayload(int fkrow) {
        String[] subs = { "BAT", "CAT", "DAT", "EAT", "FAT", "GAT", "HAT",
                "MAT", "NAT", "PAT", "RAT", "SAT", "TAT", "VAT" };
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 40; i++) {
            sb.append(subs[m_rand.nextInt(13)]);
        }
        return sb.toString();
    }

    boolean updateToCatalog(int catalog) throws NoConnectionsException,
            IOException {
        // 0 = ab, 1 = a, 2 = b
        // allowed to update to current catalog (no change)
        Transaction nextCatalog;
        switch (catalog) {
        case 0:
            nextCatalog = Transaction.CatalogAB;
            break;
        case 1:
            nextCatalog = Transaction.CatalogA;
            break;
        case 2:
            nextCatalog = Transaction.CatalogB;
            break;
        default:
            throw new UnsupportedOperationException(
                    "Invalid catalog update requested");
        }

        System.err.println("Updating to catalog name: " + nextCatalog.name
                + " using deployment file: "
                + m_deploymentFilePath);

        return m_voltClient.callProcedure(new AppUpdateCallback(nextCatalog),
                "@UpdateApplicationCatalog", nextCatalog.name, m_deploymentFilePath);
    }

    @Override
    protected boolean runOnce() throws IOException {
        boolean queued = false;
        // loading
        if (tuplesLoaded < 10000) {
            queued = m_voltClient.callProcedure(new AppUpdateCallback(
                    Transaction.Load), "Load", tuplesLoaded,
                    getStringKey(tuplesLoaded), getPayload(tuplesLoaded));

            if (queued) {
                tuplesLoaded += 1;
                if (tuplesLoaded % 100000 == 0) {
                    System.err.println("Loaded: " + tuplesLoaded);
                }
            }
        }

        // change catalogs with a small probability
        else if (m_rand.nextInt(100000) < 10) {
            // checkTables();
            queued = updateToCatalog(m_rand.nextInt(1000) % 3);
        }

        // 50% inserts to A
        else if (m_rand.nextInt(100) < 50) {
            // need a key for the payload lookup
            int key = m_rand.nextInt(tuplesLoaded);
            queued = m_voltClient.callProcedure(new AppUpdateCallback(
                    Transaction.InsertA), "InsertA", nextA++, key);
        }

        // 50% inserts to B
        else {
            // need a string for the payload lookup
            String key = getStringKey(m_rand.nextInt(tuplesLoaded));
            queued = m_voltClient.callProcedure(new AppUpdateCallback(
                    Transaction.InsertB), "InsertB", nextB++, key);
        }

        return queued;
    }

    public static void main(String[] args) {
        ClientMain.main(AppUpdate.class, args, false);
        System.err.println("Exiting AppUpdate.");
    }

    private static class ForeignKeyConstraints extends ForeignKeyConstraintBase {

        private static final HashMap<Integer, String> intLookup = new HashMap<Integer, String>();
        private static final HashMap<String, String> strLookup = new HashMap<String, String>();

        public ForeignKeyConstraints(String table) {
            super(table);
        }

        @Override
        public <T> Object evaluate(T tuple) {
            VoltTable row = (VoltTable) tuple;

            // build the FK table relationships in memory
            if (m_table.equalsIgnoreCase("FK")) {
                final int int_index = row.getColumnIndex("I");
                final int str_index = row.getColumnIndex("S");
                final int pay_index = row.getColumnIndex("PAYLOAD");
                intLookup.put((Integer) row.get(int_index, VoltType.INTEGER),
                        (String) row.get(pay_index, VoltType.STRING));
                strLookup.put((String) row.get(str_index, VoltType.STRING),
                        (String) row.get(pay_index, VoltType.STRING));
                return true;
            } else if (m_table.equalsIgnoreCase("A")) {
                final int int_index = row.getColumnIndex("I");
                final int pay_index = row.getColumnIndex("PAYLOAD");

                Integer int_value = (Integer) row.get(int_index,
                        VoltType.INTEGER);
                String pay_value = (String) row.get(pay_index, VoltType.STRING);
                return intLookup.get(int_value).equals(pay_value);
            } else if (m_table.equalsIgnoreCase("B")) {
                final int str_index = row.getColumnIndex("S");
                final int pay_index = row.getColumnIndex("PAYLOAD");

                String str_value = (String) row.get(str_index, VoltType.STRING);
                String pay_value = (String) row.get(pay_index, VoltType.STRING);
                return strLookup.get(str_value).equals(pay_value);
            }
            return false;
        }
    }

    private void setupVerificationConstraints() {
        // must process table f first to populate the constraint hashes
        System.err.println("ADDING TABLE CONSTRAINTS");
        addTableConstraint("FK", new ForeignKeyConstraints("FK"));
        addTableConstraint("A", new ForeignKeyConstraints("A"));
        addTableConstraint("B", new ForeignKeyConstraints("B"));
    }

}
