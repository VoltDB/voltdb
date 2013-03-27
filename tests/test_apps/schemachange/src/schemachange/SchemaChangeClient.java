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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.voltdb.CLIConfig;
import org.voltdb.ClientResponseImpl;
import org.voltdb.TableHelper;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.AbstractProcedureArgumentCacher;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ClientStatusListenerExt.DisconnectCause;

public class SchemaChangeClient {

    static subClient client = null;
    static AtomicLong nextKeyToInsert = new AtomicLong(0);
    static AtomicLong maxInsertedKey = new AtomicLong(0);
    static SchemaChangeConfig config = null;
    static String deploymentString = null;
    static Random rand = new Random(0);
    static Topology topo = null;
    private static AtomicInteger totalConnections = new AtomicInteger(0);
    private static AtomicInteger fatalLevel = new AtomicInteger(0);
    static long lastSuccessTime = 0;
    static Timer timer;
    static boolean noProgressCB = false;
    static long startTime;

    public static String _F(String str, Object... parameters) {
        return String.format(DateFormatUtils.format(System.currentTimeMillis(), "MM/dd/yyyy HH:mm:ss") + "\t" + str, parameters);
    }

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class SchemaChangeConfig extends CLIConfig {
        @Option(desc = "Target RSS per server in MB.")
        int targetrssmb = 1024 * 4;

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
            if (targetrssmb < 0) exitWithMessageAndUsage("targetrssmb must be >= 0");
            if (targetrowcount < 0) exitWithMessageAndUsage("targetrowcount must be >= 0");
            if (duration < 0) exitWithMessageAndUsage("duration must be >= 0");
        }
    }

    /**
     * Subclass the VoltDB Client interface
     * for control of tx failure and connection loss in cases of
     * node and cluster failure.
     */
    public static class subClient implements Client {
        Client client;

        public subClient(ClientConfig config) {
            client = ClientFactory.createClient(config);
        }

        @Override
        public void createConnection(String host) throws UnknownHostException, IOException {
            client.createConnection(host);
        }

        @Override
        public void createConnection(String host, int port) throws UnknownHostException, IOException {
            client.createConnection(host, port);
        }

        @Override
        public ClientResponse callProcedure(String procName,
                Object... parameters) throws IOException,
                NoConnectionsException, ProcCallException {
            ClientResponse r = null;
            while (!noProgressCB) {
                try {
                    r = client.callProcedure(procName, parameters);
                    lastSuccessTime = System.currentTimeMillis();
                    return r;
                } catch (NoConnectionsException e) {
                    System.err.println(_F("Caught NoConnectionsException: " + e.getMessage()));
                    // retry this case
                    // throw e
                } catch (ProcCallException e) {
                    System.err.println(_F("Caught ProcCallException: " + e.getMessage()));
                    // reflect this case
                    // can get 'connection lost before a response was received here' the tx may be committed???
                    if (! e.getMessage().startsWith("Connection to database host"))
                        throw e;
                } catch (IOException e) {
                    System.err.println(_F("Caught IOException: " + e.getMessage()));
                    // reflect this case
                    throw e;
                } finally {
                    try { Thread.sleep(3); }
                    catch (Exception e) { }
                }
            }
            // Exit with an error, a message was already produced
            terminate(1);
            return null;
        }

        @Override
        public boolean callProcedure(ProcedureCallback callback,
                String procName, Object... parameters) throws IOException,
                NoConnectionsException {

            class Callback implements ProcedureCallback {
                ProcedureCallback callback;
                String procName;
                Object[] parameters;
                long tries;

                Callback(ProcedureCallback callback, String procName,
                        Object... parameters) {
                    this.callback = callback;
                    this.procName = procName;
                    this.parameters = parameters;
                    this.tries = 0;
                }

                @Override
                public void clientCallback(ClientResponse clientResponse) throws Exception {
                    this.tries++;
                    byte s = clientResponse.getStatus();
                    if (s != ClientResponse.SUCCESS) {
                        System.err.println(_F(((ClientResponseImpl) clientResponse).toJSONString()));
                        switch (s) {
                            case ClientResponse.CONNECTION_LOST:
                            case ClientResponse.CONNECTION_TIMEOUT:
                            case ClientResponse.UNEXPECTED_FAILURE:
                                System.err.printf(_F("retrying lost/timeout connection %d\n", this.tries));
                                if (noProgressCB) break; // don't retry if circuit breaker has tripped
                                Thread.sleep(1);
                                client.callProcedure(this, procName, parameters); // retry the call
                                break;
                            default:
                                this.callback.clientCallback(clientResponse); // reflect to caller
                                break;
                        }
                    } else {
                        lastSuccessTime = System.currentTimeMillis();
                        if (this.tries > 1) System.err.println(_F("success retrying"));
                        this.callback.clientCallback(clientResponse); // reflect to caller
                    }
                }
            };

            ProcedureCallback cb = new Callback(callback, procName, parameters);

            boolean r = false;
            while (!noProgressCB) {
                try {
                    return client.callProcedure(cb, procName, parameters);
                } catch (NoConnectionsException e) {
                    System.err.println(_F("Caught NoConnectionsException: "
                            + e.getMessage()));
                    // throw e;
                } catch (IOException e) {
                    System.err.println(_F("Caught IOException: " + e.getMessage()));
                    throw e;
                } finally {
                    try {
                        Thread.sleep(3);
                    } catch (Exception e) {
                    }
                }
            }
            // Exit with an error, a message was already produced
            terminate(1);
            return false;
        }

        @Override
        public boolean callProcedure(ProcedureCallback callback, int expectedSerializedSize, String procName,
                                        Object... parameters) throws IOException, NoConnectionsException {
            assert (false); // NI
            return client.callProcedure(callback, expectedSerializedSize, procName, parameters);
        }

        @Override
        public int calculateInvocationSerializedSize(String procName, Object... parameters) {
            return client.calculateInvocationSerializedSize(procName, parameters);
        }

        @Override
        public ClientResponse updateApplicationCatalog(File catalogPath, File deploymentPath) throws IOException,
                NoConnectionsException, ProcCallException {
            // TODO Auto-generated method stub
            return client.updateApplicationCatalog(catalogPath, deploymentPath);
        }

        @Override
        public boolean updateApplicationCatalog(ProcedureCallback callback,
                File catalogPath, File deploymentPath) throws IOException, NoConnectionsException {
            return false;
        }

        @Override
        public void drain() throws NoConnectionsException, InterruptedException {
            client.drain();
        }

        @Override
        public void close() throws InterruptedException {
            client.close();

        }

        @Override
        public void backpressureBarrier() throws InterruptedException {
            client.backpressureBarrier();

        }

        @Override
        public ClientStatsContext createStatsContext() {
            return client.createStatsContext();
        }

        @Override
        public Object[] getInstanceId() {
            return client.getInstanceId();
        }

        @Override
        public String getBuildString() {
            return client.getBuildString();
        }

        @Override
        public void configureBlocking(boolean blocking) {
            client.configureBlocking(blocking);
        }

        @Override
        public boolean blocking() {
            return client.blocking();
        }

        @Override
        public int[] getThroughputAndOutstandingTxnLimits() {
            return client.getThroughputAndOutstandingTxnLimits();
        }

        @Override
        public List<InetSocketAddress> getConnectedHostList() {
            return client.getConnectedHostList();
        }

        @Override
        public void writeSummaryCSV(ClientStats stats, String path)
                throws IOException {
            client.writeSummaryCSV(stats, path);
        }
    }

    /**
     * Perform a schema change to a mutated version of the current table (80%) or
     * to a new table entirely (20%, drops and adds the new table).
     */
    static VoltTable catalogChange(VoltTable t1, boolean newTable) throws Exception {
        CatalogBuilder builder = new CatalogBuilder();
        VoltTable t2 = null;
        String currentName = t1 == null ? "B" : TableHelper.getTableName(t1);
        String newName = currentName;

        if (newTable) {
            newName = currentName.equals("A") ? "B" : "A";
            t2 = TableHelper.getTotallyRandomTable(newName, rand);
        }
        else {
            t2 = TableHelper.mutateTable(t1, false, rand);
        }

        System.out.printf(_F("New Schema:\n%s\n", TableHelper.ddlForTable(t2)));

        builder.addLiteralSchema(TableHelper.ddlForTable(t2));
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

        ClientResponse cr = client.callProcedure("@UpdateApplicationCatalog", catalogData, null);
        assert(cr.getStatus() == ClientResponse.SUCCESS);

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

        //System.out.println(_F("Sleeping for 5s"));
        //Thread.sleep(5000);

        return t2;
    }

    static class Topology {
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

    static Topology getCluterTopology(subClient client) throws Exception {
        int hosts = -1;
        int sitesPerHost = -1;
        int k = -1;

        VoltTable result = client.callProcedure("@SystemInformation", "DEPLOYMENT").getResults()[0];
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
     * Count the number of tuples in the table.
     */
    static long tupleCount(VoltTable t) throws Exception {
        if (t == null) {
            return 0;
        }
        VoltTable result = client.callProcedure("@AdHoc",
                String.format("select count(*) from %s;", TableHelper.getTableName(t))).getResults()[0];
        return result.asScalarLong();
    }

    /**
     * Find the largest pkey value in the table.
     */
    static long maxId(VoltTable t) throws Exception {
        if (t == null) {
            return 0;
        }
        VoltTable result = client.callProcedure("@AdHoc",
                String.format("select pkey from %s order by pkey desc limit 1;", TableHelper.getTableName(t))).getResults()[0];
        return result.getRowCount() > 0 ? result.asScalarLong() : 0;
    }

    /**
     * Add rows until RSS target met.
     * Delete all odd rows (triggers compaction).
     * Re-add odd rows until RSS target met (makes buffers out of order).
     */
    static void loadTable(VoltTable t) throws Exception {
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
        TableHelper.fillTableWithBigintPkey(t, config.targetrssmb, realRowCount, client, rand, max + 1, 1);
        TableHelper.deleteEveryNRows(t, client, n);
        TableHelper.fillTableWithBigintPkey(t, config.targetrssmb, realRowCount, client, rand, 1, n);
    }

    /**
     * Grab some random rows that aren't on the first EE page for the table.
     */
    public static VoltTable sample(VoltTable t) throws Exception {
        VoltTable t2 = t.clone(4096 * 1024);

        ClientResponse cr = client.callProcedure("@AdHoc",
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
     * @param server
     *            hostname:port or just hostname (hostname can be ip).
     */
    static void connectToOneServerWithRetry(String server) {
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

    // For retry connections
    private static ExecutorService es = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable arg0) {
            Thread thread = new Thread(arg0, "Retry Connection");
            thread.setDaemon(true);
            return thread;
        }
    });

    /**
     * Provides a callback to be notified on node failure.
     * This example only logs the event.
     */
    static class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            // if the benchmark is still active
            long currentTime = System.currentTimeMillis();
            if ((currentTime - startTime) < (config.duration * 1000)) {
                System.err.printf(_F("Lost connection to %s:%d.\n", hostname, port));
                totalConnections.decrementAndGet();
                // setup for retry
                final String server = hostname;
                es.execute(new Runnable() {
                    @Override
                    public void run() {
                        connectToOneServerWithRetry(server);
                    }
                });
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
    public static void connect(String servers) throws InterruptedException {
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

    /**
     * Create a Timer task to
     *
     */
    public static void schedulePeriodicChecks() {
        timer = new Timer(true); // true means its on a daemon thread
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() {
                periodicChecks();
            }
        };

        timer.scheduleAtFixedRate(statsPrinting, config.checkInterval * 1000,
                config.checkInterval * 1000);
    }

    /**
     * Prints a one line update on performance that can be printed periodically
     * during a benchmark.
     */
    public static synchronized void periodicChecks() {
        if (totalConnections.get() > 0) {
            if (System.currentTimeMillis() - lastSuccessTime > config.noProgressTimeout * 1000) {
                System.out.println(_F("Periodic cehck - fail: No progress timeout has been reached"));
                noProgressCB = true;
            }
            else
                System.out.println(_F("Periodic check - pass"));
        }
    }

    public static void terminate(int returncode) {
        timer.cancel();
        System.exit(returncode);
    }

    public static void main(String[] args) throws Exception {
        VoltDB.setDefaultTimezone();

        config = new SchemaChangeConfig();
        config.parse("SchemaChangeClient", args);

        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());
        clientConfig.setProcedureCallTimeout(30 * 60 * 1000); // 30 min
        Client c = ClientFactory.createClient(clientConfig);
        client = new subClient(clientConfig);
        connect(config.servers);
        schedulePeriodicChecks();

        // get the topo
        topo = getCluterTopology(client);

        // kick this off with a random schema
        VoltTable t = catalogChange(null, true);

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
                t = catalogChange(t, (j == 0) && (rand.nextInt(5) == 0));

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
}
