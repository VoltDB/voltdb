/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package schemachange;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.CLIConfig;
import org.voltdb.ClientResponseImpl;
import org.voltdb.TableHelper;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.utils.MiscUtils;

public class SchemaChangeClient {

    static VoltLogger log = new VoltLogger("HOST");

    Client client = null;
    private SchemaChangeConfig config = null;
    private Random rand = new Random(0);
    private Topology topo = null;
    private AtomicInteger totalConnections = new AtomicInteger(0);
    private AtomicInteger fatalLevel = new AtomicInteger(0);

    private int schemaVersionNo = 0;

    private long startTime;

    private static String _F(String str, Object... parameters) {
        return String.format(str, parameters);
    }

    static void logStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        t.printStackTrace(pw);
        log.error(sw.toString());
    }

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class SchemaChangeConfig extends CLIConfig {
        @Option(desc = "Maximum number of rows to load (times sites for replicated tables).")
        long targetrowcount = 100000;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Run Time.")
        int duration = 300 * 60;

        @Option(desc = "Time (secs) to end run if no progress is being made.")
        int noProgressTimeout = 600;

        @Override
        public void validate() {
            if (targetrowcount <= 0) exitWithMessageAndUsage("targetrowcount must be > 0");
            if (duration < 0) exitWithMessageAndUsage("duration must be >= 0");
        }
    }

    /**
     * Call a procedure and check the return code.
     * Success just returns the result to the caller.
     * Unpossible errors end the process.
     * Some errors will retry the call until the global progress timeout with various waits.
     * After the global progress timeout, the process is killed.
     */
    ClientResponse callROProcedureWithRetry(String procName, Object... params) {
        long startTime = System.currentTimeMillis();
        long now = startTime;

        while (now - startTime < (config.noProgressTimeout * 1000)) {
            ClientResponse cr = null;

            try {
                cr = client.callProcedure(procName, params);
            }
            catch (ProcCallException e) {
                log.debug("callROProcedureWithRetry operation exception:", e);
                cr = e.getClientResponse();
            }
            catch (NoConnectionsException e) {
                log.debug("callROProcedureWithRetry operation exception:", e);
                // wait a bit to retry
                try { Thread.sleep(1000); } catch (InterruptedException e1) {}
            }
            catch (IOException e) {
                log.debug("callROProcedureWithRetry operation exception:", e);
                // IOException is not cool man
                logStackTrace(e);
                System.exit(-1);
            }

            if (cr != null) {
                if (cr.getStatus() != ClientResponse.SUCCESS) {
                    log.debug("callROProcedureWithRetry operation failed: " + ((ClientResponseImpl)cr).toJSONString());
                }
                switch (cr.getStatus()) {
                case ClientResponse.SUCCESS:
                    // hooray!
                    return cr;
                case ClientResponse.CONNECTION_LOST:
                case ClientResponse.CONNECTION_TIMEOUT:
                    // can retry after a delay
                    try { Thread.sleep(5 * 1000); } catch (Exception e) {}
                    break;
                case ClientResponse.RESPONSE_UNKNOWN:
                    // can try again immediately - cluster is up but a node died
                    break;
                case ClientResponse.SERVER_UNAVAILABLE:
                    // shouldn't be in admin mode (paused) in this app, but can retry after a delay
                    try { Thread.sleep(30 * 1000); } catch (Exception e) {}
                    break;
                case ClientResponse.GRACEFUL_FAILURE:
                    //log.error(_F("GRACEFUL_FAILURE response in procedure call for: %s", procName));
                    //log.error(((ClientResponseImpl)cr).toJSONString());
                    //logStackTrace(new Throwable());
                    return cr; // caller should always check return status
                case ClientResponse.UNEXPECTED_FAILURE:
                case ClientResponse.USER_ABORT:
                    // for starters, I'm assuming these errors can't happen for reads in a sound system
                    String ss = cr.getStatusString();
                    if (ss.contains("Statement: select count(*) from")) {
                        // We might need to retry
                        log.warn(ss);
                        if ((ss.matches("(?s).*AdHoc transaction [0-9]+ wasn.t planned against the current catalog version.*") ||
                             ss.matches("(?s).*Invalid catalog update.  Catalog or deployment change was planned against one version of the cluster configuration but that version was no longer live.*")
                             )) {
                            log.info("retrying...");
                        } else {
                            log.error(String.format("Error in procedure call for: %s", procName));
                            log.error(((ClientResponseImpl)cr).toJSONString());
                            assert(false);
                            System.exit(-1);
                        }
                    }
                }
            }

            now = System.currentTimeMillis();
        }

        log.error(_F("Error no progress timeout reached, terminating"));
        System.exit(-1);
        return null;
    }

    SchemaChangeClient(SchemaChangeConfig config) {
        this.config = config;
    }

    /**
     * Perform a schema change to a mutated version of the current table (80%) or
     * to a new table entirely (20%, drops and adds the new table).
     */
    private Pair<VoltTable,TableHelper.ViewRep> catalogChange(VoltTable t1, boolean newTable, TableHelper.ViewRep view) throws Exception {
        CatalogBuilder builder = new CatalogBuilder();
        VoltTable t2 = null;
        String currentName = t1 == null ? "B" : TableHelper.getTableName(t1);
        String newName = currentName;

        // add an empty table with the schema version number in it
        VoltTable versionT = TableHelper.quickTable(String.format("V%s (BIGINT)", schemaVersionNo + 1));

        if (newTable) {
            newName = currentName.equals("A") ? "B" : "A";
            t2 = TableHelper.getTotallyRandomTable(newName, rand);
        }
        else {
            t2 = TableHelper.mutateTable(t1, true, rand);
        }

        log.info(_F("New Schema:\n%s", TableHelper.ddlForTable(t2)));

        // handle views
        if (view == null) {
            view = TableHelper.ViewRep.viewRepForTable("MV", t2, rand);
        }
        else {
            if (!view.compatibleWithTable(t2)) {
                view = null;
            }
        }
        if (view != null) {
            log.info(_F("New View:\n%s", view.ddlForView()));
        }
        else {
            log.info("New View: NULL");
        }

        builder.addLiteralSchema(TableHelper.ddlForTable(t2));
        if (view != null) {
            builder.addLiteralSchema(view.ddlForView());
        }
        builder.addLiteralSchema(TableHelper.ddlForTable(versionT));
        // make tables name A partitioned and tables named B replicated
        if (newName.equalsIgnoreCase("A")) {
            int pkeyIndex = TableHelper.getBigintPrimaryKeyIndexIfExists(t2);
            builder.addPartitionInfo(newName, t2.getColumnName(pkeyIndex));
            builder.addProcedures(VerifySchemaChangedA.class);
        }
        else {
            builder.addProcedures(VerifySchemaChangedB.class);
        }
        byte[] catalogData = builder.compileToBytes();
        assert(catalogData != null);

        long count = tupleCount(t1);
        long start = System.nanoTime();

        if (newTable) {
            log.info(_F("Starting catalog update to swap tables."));
        }
        else {
            log.info(_F("Starting catalog update to change schema."));
        }

        boolean success = false;
        ClientResponse cr = null;
        try {
            cr = client.callProcedure("@UpdateApplicationCatalog", catalogData, null);
        }
        catch (NoConnectionsException e) {
            // failure
        }
        catch (IOException e) {
            // IOException is not cool man
            logStackTrace(e);
            System.exit(-1);
        }
        catch (ProcCallException e) {
            cr = e.getClientResponse();
        }

        if (cr != null) {
            switch (cr.getStatus()) {
            case ClientResponse.SUCCESS:
                // hooray!
                success = true;
                log.info("Catalog update was reported to be successful");
                break;
            case ClientResponse.CONNECTION_LOST:
            case ClientResponse.CONNECTION_TIMEOUT:
            case ClientResponse.RESPONSE_UNKNOWN:
            case ClientResponse.SERVER_UNAVAILABLE:
                // can try again after a break
                break;
            case ClientResponse.UNEXPECTED_FAILURE:
            case ClientResponse.GRACEFUL_FAILURE:
            case ClientResponse.USER_ABORT:
                // should never happen
                log.error(_F("USER_ABORT in procedure call for Catalog update"));
                log.error(((ClientResponseImpl)cr).toJSONString());
                assert(false);
                System.exit(-1);
            }
        }

        // don't actually trust the call... manually verify
        int obsCatVersion = verifyAndGetSchemaVersion();

        if (obsCatVersion == schemaVersionNo) {
            if (success == true) {
                log.error(_F("Catalog update was reported to be successful but did not pass verification: expected V%d, observed V%d", schemaVersionNo+1, obsCatVersion));
                assert(false);
                System.exit(-1);
            }

            // UAC didn't work
            return null;
        }

        // UAC worked
        if (obsCatVersion == schemaVersionNo+1) schemaVersionNo++;
        else {
            assert(false);
            System.exit(-1);
        }

        long end = System.nanoTime();
        double seconds = (end - start) / 1000000000.0;

        if (newTable) {
            log.info(_F("Completed catalog update that swapped tables in %.4f seconds",
                    seconds));
        }
        else {
            log.info(_F("Completed catalog update of %d tuples in %.4f seconds (%d tuples/sec)",
                    count, seconds, (long) (count / seconds)));
        }

        return new Pair<VoltTable,TableHelper.ViewRep>(t2, view, false);
    }

    private static class Topology {
        final int hosts;
        final int sites;
        final int partitions;

        Topology(int hosts, int sites, int partitions) {
            assert (hosts > 0);
            assert (sites > 0);
            assert (partitions > 0);
            this.hosts = hosts;
            this.sites = sites;
            this.partitions = partitions;
        }
    }

    private Topology getCluterTopology() {
        int hosts = -1;
        int sitesPerHost = -1;
        int k = -1;

        ClientResponse cr = callROProcedureWithRetry("@SystemInformation", "DEPLOYMENT");
        assert(cr.getStatus() == ClientResponse.SUCCESS);
        VoltTable result = cr.getResults()[0];
        result.resetRowPosition();
        while (result.advanceRow()) {
            String key = result.getString(0);
            String value = result.getString(1);
            if (key.equals("hostcount")) {
                hosts = Integer.parseInt(value);
            }
            if (key.equals("sitesperhost")) {
                sitesPerHost = Integer.parseInt(value);
            }
            if (key.equals("kfactor")) {
                k = Integer.parseInt(value);
            }
        }

        return new Topology(hosts, hosts * sitesPerHost, (hosts * sitesPerHost) / (k + 1));
    }

    /**
     * Get a list of tables from the system and verify that the dummy table added for versioning
     * is the right one.
     */
    private int verifyAndGetSchemaVersion() {
        // start with the last schema id we thought we had verified
        int version = schemaVersionNo;
        if (!isSchemaVersionObservable(version)) {
            if (!isSchemaVersionObservable(++version)) {
                // version should be one of these two values
                log.error("Catalog version is out of range");
                assert(false);
                System.exit(-1);
            }
        }
        //log.info(_F("detected catalog version is: %d", version));
        return version;
    }

    private boolean isSchemaVersionObservable(int schemaid) {
        ClientResponse cr = callROProcedureWithRetry("@AdHoc",
                String.format("select count(*) from V%d;", schemaid));
        return (cr.getStatus() == ClientResponse.SUCCESS);
    }

    /**
     * Count the number of tuples in the table.
     */
    private long tupleCount(VoltTable t) {
        if (t == null) {
            return 0;
        }
        ClientResponse cr = callROProcedureWithRetry("@AdHoc",
                String.format("select count(*) from %s;", TableHelper.getTableName(t)));
        assert(cr.getStatus() == ClientResponse.SUCCESS);
        VoltTable result = cr.getResults()[0];
        return result.asScalarLong();
    }

    /**
     * Find the largest pkey value in the table.
     */
    public long maxId(VoltTable t) {
        if (t == null) {
            return 0;
        }
        ClientResponse cr = callROProcedureWithRetry("@AdHoc",
                String.format("select pkey from %s order by pkey desc limit 1;", TableHelper.getTableName(t)));
        assert(cr.getStatus() == ClientResponse.SUCCESS);
        VoltTable result = cr.getResults()[0];
        return result.getRowCount() > 0 ? result.asScalarLong() : 0;
    }

    /**
     * Add rows until RSS or rowcount target met.
     * Delete some rows rows (triggers compaction).
     * Re-add odd rows until RSS or rowcount target met (makes buffers out of order).
     */
    private void loadTable(VoltTable t) {
        // if #partitions is odd, delete every 2 - if even, delete every 3
        //int n = 3 - (topo.partitions % 2);

        int redundancy = topo.sites / topo.partitions;
        long realRowCount = (config.targetrowcount * topo.hosts) / redundancy;
        // if replicated
        if (TableHelper.getTableName(t).equals("B")) {
            realRowCount /= topo.partitions;
        }

        long max = maxId(t);

        TableLoader loader = new TableLoader(this, t, rand);

        log.info(_F("loading table"));
        loader.load(max + 1, realRowCount);
    }

    /**
     * Grab some random rows that aren't on the first EE page for the table.
     */
    private VoltTable sample(long offset, VoltTable t) {
        VoltTable t2 = t.clone(4096 * 1024);

        ClientResponse cr = callROProcedureWithRetry("@AdHoc",
                String.format("select * from %s where pkey >= %d order by pkey limit 100;",
                        TableHelper.getTableName(t), offset));
        assert(cr.getStatus() == ClientResponse.SUCCESS);
        VoltTable result = cr.getResults()[0];
        result.resetRowPosition();
        while (result.advanceRow()) {
            t2.add(result);
        }

        return t2;
    }



    /**
     * Connect to a single server with retry. Limited exponential backoff. No
     * timeout. This will run until the process is killed if it's not able to
     * connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    private void connectToOneServerWithRetry(String server) {
        /*
         * Possible exceptions are: 1) Exception: java.net.ConnectException:
         * Connection refused 2) Exception: java.io.IOException: Failed to
         * authenticate to rejoining node 3) Exception: java.io.IOException:
         * Cluster instance id mismatch. The third one could indicate a bug.
         */
        int sleep = 1000;
        boolean flag = true;
        String msg;
        if (fatalLevel.get() > 0) {
            log.error(_F("In connectToOneServerWithRetry, don't bother to try reconnecting to this host: %s",
                            server));
            flag = false;
        }
        while (flag) {
            try {
                client.createConnection(server);
                totalConnections.incrementAndGet();
                msg = "Connected to VoltDB node at: " + server + ", IDs: "
                        + client.getInstanceId()[0] + " - "
                        + client.getInstanceId()[1] + ", totalConnections = "
                        + totalConnections.get();
                log.info(_F(msg));
                break;
            } catch (Exception e) {
                msg = "Connection to " + server + " failed - retrying in "
                        + sleep / 1000 + " second(s)";
                log.info(_F(msg));
                try {
                    Thread.sleep(sleep);
                } catch (Exception interruted) {
                }
                if (sleep < 4000)
                    sleep += sleep;
            }
        }
    }

    /**
     * Provides a callback to be notified on node failure.
     * This example only logs the event.
     */
    private class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
                log.warn(_F("Lost connection to %s:%d.", hostname, port));
                totalConnections.decrementAndGet();

                // reset the connection id so the client will connect to a recovered cluster
                // this is a bit of a hack
                if (connectionsLeft == 0) {
                    ((ClientImpl) client).resetInstanceId();
                }

                // setup for retry
                final String server = MiscUtils.getHostnameColonPortString(hostname, port);
                class ReconnectThread extends Thread {
                    @Override
                    public void run() {
                        connectToOneServerWithRetry(server);
                    }
                };

                ReconnectThread th = new ReconnectThread();
                th.setDaemon(true);
                th.start();
        }
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @param servers
     *            A comma separated list of servers using the hostname:port
     *            syntax (where :port is optional).
     * @throws InterruptedException
     *             if anything bad happens with the threads.
     */
    private void connect(String servers) throws InterruptedException {
        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(
                serverArray.length);
        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(server);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
    }

    private void runTestWorkload() throws Exception {

        startTime = System.currentTimeMillis();

        class watchDog extends Thread {
            @Override
            public void run() {
                if (config.duration == 0)
                    return;
                try { Thread.sleep(config.duration * 1000); }
                catch (Exception e) { }
                log.info("Duration limit reached, terminating run");
                System.exit(0);
            }
        };
        watchDog th = new watchDog();
        th.setDaemon(true);
        th.start();

        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());
        //clientConfig.setProcedureCallTimeout(30 * 60 * 1000); // 30 min
        clientConfig.setMaxOutstandingTxns(512);
        client = ClientFactory.createClient(clientConfig);
        connect(config.servers);

        // get the topo
        topo = getCluterTopology();

        // kick this off with a random schema
        VoltTable t = null;
        TableHelper.ViewRep v = null;
        while (t == null) {
            Pair<VoltTable, TableHelper.ViewRep> schema = catalogChange(null, true, null);
            t = schema.getFirst();
            v = schema.getSecond();
        }

        while (true) {

            // make sure the table is full and mess around with it
            loadTable(t);

            for (int j = 0; j < 3; j++) {

                String tableName = TableHelper.getTableName(t);

                // deterministically sample some rows
                VoltTable preT = null;
                long max = maxId(t);
                long sampleOffset = -1;
                if (max > 0) {
                    if (max <= 100)
                        sampleOffset = 0;
                    else
                        sampleOffset = Math.min((long) (max * .75), max - 100);
                    assert(max >= 0);
                    preT = sample(sampleOffset, t);
                    assert(preT.getRowCount() > 0);
                    log.info(_F("Sampled table %s from offset %d limit 100 and found %d rows.",
                            tableName, sampleOffset, preT.getRowCount()));
                }
                //log.info(_F("First sample:\n%s", preT.toFormattedString()));

                // move to an entirely new table or migrated schema
                VoltTable newT = null;
                TableHelper.ViewRep newV = null;
                boolean isNewTable = (j == 0) && (rand.nextInt(5) == 0);
                while (newT == null) {
                    Pair<VoltTable, TableHelper.ViewRep> schema = catalogChange(t, isNewTable, v);
                    if (schema == null) {
                            log.info(_F("Retrying an unsuccessful catalog update."));
                            continue;   // try again
                    }
                    newT = schema.getFirst();
                    newV = schema.getSecond();
                }
                t = newT;
                v = newV;

                // if the table has been migrated, check the data
                if (!isNewTable && (preT != null)) {
                    VoltTable guessT = t.clone(4096 * 1024);
                    //log.info(_F("Empty clone:\n%s", guessT.toFormattedString()));

                    TableHelper.migrateTable(preT, guessT);
                    //log.info(_F("Java migration:\n%s", guessT.toFormattedString()));

                    // deterministically sample the same rows
                    assert(sampleOffset >= 0);
                    ClientResponse cr = callROProcedureWithRetry(
                            "VerifySchemaChanged" + tableName, sampleOffset, guessT);
                    assert(cr.getStatus() == ClientResponse.SUCCESS);
                    VoltTable result = cr.getResults()[0];
                    boolean success = result.fetchRow(0).getLong(0) == 1;
                    String err = result.fetchRow(0).getString(1);
                    if (!success) {
                        log.error(_F(err));
                        assert(false);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        VoltDB.setDefaultTimezone();

        SchemaChangeConfig config = new SchemaChangeConfig();
        config.parse("SchemaChangeClient", args);

        SchemaChangeClient schemaChange = new SchemaChangeClient(config);
        schemaChange.runTestWorkload();
    }
}
