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

package schemachange;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.voltdb.CLIConfig;
import org.voltdb.TableHelper;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.CatalogBuilder;

public class SchemaChangeClient {

    Client client = null;
    private SchemaChangeConfig config = null;
    private Random rand = new Random(0);
    private Topology topo = null;
    private AtomicInteger totalConnections = new AtomicInteger(0);
    private AtomicInteger fatalLevel = new AtomicInteger(0);

    private int schemaVersionNo = 0;

    private long startTime;

    private static String _F(String str, Object... parameters) {
        return String.format(DateFormatUtils.format(System.currentTimeMillis(), "MM/dd/yyyy HH:mm:ss") + "\t" + str, parameters);
    }

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class SchemaChangeConfig extends CLIConfig {
        @Option(desc = "Maximum number of rows to load (times sites for replicated tables).")
        long targetrowcount = Long.MAX_VALUE;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Run Time.")
        int duration = 30 * 60;

        @Option(desc = "Time (secs) to end run if no progress is being made.")
        int noProgressTimeout = 600;

        @Option(desc = "Interval (secs) to check if progress is being made")
        int checkInterval = 60;

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
                cr = e.getClientResponse();
            }
            catch (NoConnectionsException e) {
                // wait a bit to retry
                try { Thread.sleep(1000); } catch (InterruptedException e1) {}
            } catch (IOException e) {
                // IOException is not cool man
                e.printStackTrace();
                System.exit(-1);
            }

            if (cr != null) {
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
                case ClientResponse.UNEXPECTED_FAILURE:
                case ClientResponse.USER_ABORT:
                    // for starters, I'm assuming these errors can't happen for reads in a sound system
                    assert(false);
                    System.exit(-1);
                }
            }

            now = System.currentTimeMillis();
        }

        assert(false);
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
    private VoltTable catalogChange(VoltTable t1, boolean newTable) throws Exception {
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
            t2 = TableHelper.mutateTable(t1, false, rand);
        }

        System.out.printf(_F("New Schema:\n%s\n", TableHelper.ddlForTable(t2)));

        builder.addLiteralSchema(TableHelper.ddlForTable(t2));
        builder.addLiteralSchema(TableHelper.ddlForTable(versionT));
        // make tables name A partitioned and tables named B replicated
        if (newName.equalsIgnoreCase("A")) {
            int pkeyIndex = TableHelper.getBigintPrimaryKeyIndexIfExists(t2);
            builder.addPartitionInfo(newName, t2.getColumnName(pkeyIndex));
        }
        byte[] catalogData = builder.compileToBytes();
        assert(catalogData != null);

        long count = tupleCount(t1);
        long start = System.nanoTime();

        if (newTable) {
            System.out.println(_F("Starting catalog update to swap tables."));
        }
        else {
            System.out.println(_F("Starting catalog update to change schema."));
        }

        boolean success = false;
        ClientResponse cr = null;
        try {
            cr = client.callProcedure("@UpdateApplicationCatalog", catalogData, null);
        }
        catch (NoConnectionsException e) {
            // failure
        } catch (IOException e) {
            // IOException is not cool man
            e.printStackTrace();
            System.exit(-1);
        }

        if (cr != null) {
            switch (cr.getStatus()) {
            case ClientResponse.SUCCESS:
                // hooray!
                success = true;
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
                assert(false);
                System.exit(-1);
            }
        }

        // don't actually trust the call... manually verify
        int versionObserved = verifyAndGetSchemaVersion();

        // did not update
        if (versionObserved == schemaVersionNo) {
            // make sure the system didn't say it worked
            assert(success == false);

            // signal to the caller this didn't work
            return null;
        }
        // success!
        else {
            assert(versionObserved == (schemaVersionNo + 1));
            schemaVersionNo++;

            long end = System.nanoTime();
            double seconds = (end - start) / 1000000000.0;

            if (newTable) {
                System.out.printf(_F("Completed catalog update that swapped tables in %.4f seconds\n",
                        seconds));
            }
            else {
                System.out.printf(_F("Completed catalog update of %d tuples in %.4f seconds (%d tuples/sec)\n",
                        count, seconds, (long) (count / seconds)));
            }

            return t2;
        }
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

        VoltTable result = callROProcedureWithRetry("@SystemInformation", "DEPLOYMENT").getResults()[0];
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
        VoltTable result = callROProcedureWithRetry("@Statistics", "TABLE", 0).getResults()[0];
        result.resetRowPosition();
        int version = -1;
        while (result.advanceRow()) {
            String tableName = result.getString("TABLE_NAME");
            if (tableName.startsWith("V")) {
                int rowVersion = Integer.parseInt(tableName.substring(1));
                if (version >= 0) {
                    assert(rowVersion == version);
                }
                version = rowVersion;
            }
        }
        assert(version >= 0);
        return version;
    }

    /**
     * Count the number of tuples in the table.
     */
    private long tupleCount(VoltTable t) {
        if (t == null) {
            return 0;
        }
        VoltTable result = callROProcedureWithRetry("@AdHoc",
                String.format("select count(*) from %s;", TableHelper.getTableName(t))).getResults()[0];
        return result.asScalarLong();
    }

    /**
     * Find the largest pkey value in the table.
     */
    private long maxId(VoltTable t) {
        if (t == null) {
            return 0;
        }
        VoltTable result = callROProcedureWithRetry("@AdHoc",
                String.format("select pkey from %s order by pkey desc limit 1;", TableHelper.getTableName(t))).getResults()[0];
        return result.getRowCount() > 0 ? result.asScalarLong() : 0;
    }

    /**
     * Add rows until RSS or rowcount target met.
     * Delete some rows rows (triggers compaction).
     * Re-add odd rows until RSS or rowcount target met (makes buffers out of order).
     */
    private void loadTable(VoltTable t) {
        // if #partitions is odd, delete every 2 - if even, delete every 3
        int n = 3 - (topo.partitions % 2);

        int redundancy = topo.sites / topo.partitions;
        long realRowCount = (config.targetrowcount * topo.hosts) / redundancy;
        // if replicated
        if (TableHelper.getTableName(t).equals("B")) {
            realRowCount /= topo.partitions;
        }

        System.out.printf(_F("loading table\n"));
        long max = maxId(t);

        TableLoader loader = new TableLoader(this, t, rand);
        loader.load(max + 1, realRowCount, 1);
        loader.delete(1, realRowCount, n);
        loader.load(1, realRowCount, n);
    }

    /**
     * Grab some random rows that aren't on the first EE page for the table.
     */
    private VoltTable sample(VoltTable t) {
        VoltTable t2 = t.clone(4096 * 1024);

        ClientResponse cr = callROProcedureWithRetry("@AdHoc",
                String.format("select * from %s where pkey >= 100000 order by pkey limit 100;",
                        TableHelper.getTableName(t)));
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
            System.err
                    .printf(_F("In connectToOneServerWithRetry, don't bother to try reconnecting to this host: %s\n",
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
                System.out.println(_F(msg));
                break;
            } catch (Exception e) {
                msg = "Connection to " + server + " failed - retrying in "
                        + sleep / 1000 + " second(s)";
                System.out.println(_F(msg));
                try {
                    Thread.sleep(sleep);
                } catch (Exception interruted) {
                }
                if (sleep < 8000)
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
            // if the benchmark is still active
            long currentTime = System.currentTimeMillis();
            if ((currentTime - startTime) < (config.duration * 1000)) {
                System.err.printf(_F("Lost connection to %s:%d.\n", hostname, port));
                totalConnections.decrementAndGet();
                // setup for retry
                final String server = hostname;
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        connectToOneServerWithRetry(server);
                    }
                }).start();
            }
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
        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());
        //clientConfig.setProcedureCallTimeout(30 * 60 * 1000); // 30 min
        client = ClientFactory.createClient(clientConfig);
        connect(config.servers);

        // get the topo
        topo = getCluterTopology();

        // kick this off with a random schema
        VoltTable t = null;
        while (t == null) {
            t = catalogChange(null, true);
        }

        startTime = System.currentTimeMillis();

        while (config.duration == 0 || (System.currentTimeMillis() - startTime < (config.duration * 1000))) {
            // make sure the table is full and mess around with it
            loadTable(t);

            for (int j = 0; j < 3; j++) {

                String tableName = TableHelper.getTableName(t);

                // deterministically sample some rows
                VoltTable preT = sample(t);
                //System.out.printf(_F("First sample:\n%s\n", preT.toFormattedString()));

                // move to an entirely new table or migrated schema
                VoltTable newT = null;
                boolean isNewTable = (j == 0) && (rand.nextInt(5) == 0);
                while (newT == null) {
                    newT = catalogChange(t, isNewTable);
                }
                t = newT;

                // if the table has been migrated, check the data
                if (TableHelper.getTableName(t).equals(tableName)) {
                    VoltTable guessT = t.clone(4096 * 1024);
                    //System.out.printf(_F("Empty clone:\n%s\n", guessT.toFormattedString()));

                    TableHelper.migrateTable(preT, guessT);
                    //System.out.printf(_F("Java migration:\n%s\n", guessT.toFormattedString()));

                    // deterministically sample the same rows
                    VoltTable postT = sample(t);
                    //System.out.printf(_F("Second sample:\n%s\n", postT.toFormattedString()));

                    postT.resetRowPosition();
                    preT.resetRowPosition();
                    StringBuilder sb = new StringBuilder();
                    if (!TableHelper.deepEqualsWithErrorMsg(postT, guessT, sb)) {
                        System.err.println(_F(sb.toString()));
                        assert(false);
                    }
                }
            }
        }

        client.close();
    }

    public static void main(String[] args) throws Exception {
        VoltDB.setDefaultTimezone();

        SchemaChangeConfig config = new SchemaChangeConfig();
        config.parse("SchemaChangeClient", args);

        SchemaChangeClient schemaChange = new SchemaChangeClient(config);
        schemaChange.runTestWorkload();
    }
}
