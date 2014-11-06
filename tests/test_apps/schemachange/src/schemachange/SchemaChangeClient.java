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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.CLIConfig;
import org.voltdb.ClientResponseImpl;
import org.voltdb.TableHelper;
import org.voltdb.TableHelper.ViewRep;
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
import org.voltdb.compiler.VoltCompilerUtils;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;

public class SchemaChangeClient {

    static VoltLogger log = new VoltLogger("HOST");

    Client client = null;
    private SchemaChangeConfig config = null;
    private final Random rand = new Random(0);
    private Topology topo = null;
    private final AtomicInteger totalConnections = new AtomicInteger(0);
    private final AtomicInteger fatalLevel = new AtomicInteger(0);

    // If catalog update is chosen deployment.xml must not have schema="ddl" in the cluster element.
    private final boolean useCatalogUpdate = false;
    SchemaChanger schemaChanger = useCatalogUpdate ? new CatalogSchemaChanger() : new DDLSchemaChanger();

    // Current active tables, view and verification proc. Supports dropping before re-creating.
    Set<String> activeTableNames = new HashSet<String>();
    TableHelper.ViewRep activeViewRep = null;
    Class<?> activeVerifyProc = null;

    private int schemaVersionNo = 0;

    @SuppressWarnings("unused")
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
                    log.error(_F("Error in procedure call for: %s", procName));
                    log.error(((ClientResponseImpl)cr).toJSONString());
                    // for starters, I'm assuming these errors can't happen for reads in a sound system
                    assert(false);
                    System.exit(-1);
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
    private Pair<VoltTable,TableHelper.ViewRep> catalogChange(VoltTable t1, boolean newTable, TableHelper.ViewRep viewIn) throws Exception {
        VoltTable t2 = null;
        String currentName = t1 == null ? "B" : TableHelper.getTableName(t1);
        String newName = currentName;

        // add an empty table with the schema version number in it
        VoltTable versionT = TableHelper.quickTable(_F("V%d (BIGINT)", schemaVersionNo + 1));

        if (newTable) {
            newName = currentName.equals("A") ? "B" : "A";
            t2 = TableHelper.getTotallyRandomTable(newName, rand, false);
        }
        else {
            t2 = TableHelper.mutateTable(t1, true, rand);
        }

        // handle views
        TableHelper.ViewRep newViewRep = viewIn;
        if (newViewRep == null) {
            newViewRep = TableHelper.ViewRep.viewRepForTable("MV", t2, rand);
        }
        else {
            if (!newViewRep.compatibleWithTable(t2)) {
                newViewRep = null;
            }
        }

        // Do the drops as a separate batch. It's a NOP for the catalog update method.
        schemaChanger.beginBatch();
        try {
            schemaChanger.dropTables(_F("V%d", schemaVersionNo));
            if (activeViewRep != null) {
                schemaChanger.dropViews(activeViewRep.viewName);
                activeViewRep = null;
            }
            activeViewRep = newViewRep;
            if (activeVerifyProc != null) {
                schemaChanger.dropProcedures(activeVerifyProc.getName());
                activeVerifyProc = null;
            }
            if (newTable) {
                if (activeTableNames.contains(newName)) {
                    schemaChanger.dropTables(newName);
                    activeTableNames.remove(newName);
                }
            }
            if (!schemaChanger.executeBatch(this.client)) {
                return null;
            }
        }
        catch (IOException e) {
            return null;
        }

        // Now do the creates and alters.
        schemaChanger.beginBatch();
        long count = 0;
        long start = 0;
        try {
            schemaChanger.createTables(versionT);
            // make tables name A partitioned and tables named B replicated
            boolean partitioned = newName.equalsIgnoreCase("A");
            if (newTable) {
                schemaChanger.createTables(t2);
                activeTableNames.add(newName);
            }
            else {
                schemaChanger.updateTable(t1, t2);
            }
            if (partitioned) {
                schemaChanger.addTablePartitionInfo(t2, newName);
                activeVerifyProc = VerifySchemaChangedA.class;
            }
            else {
                activeVerifyProc = VerifySchemaChangedB.class;
            }
            schemaChanger.createProcedures(client, activeVerifyProc);
            if (activeViewRep != null) {
                schemaChanger.createViews(activeViewRep);
            }

            count = tupleCount(t1);
            start = System.nanoTime();

            if (newTable) {
                log.info("Starting to swap tables.");
            }
            else {
                log.info("Starting to change schema.");
            }

            if (!schemaChanger.executeBatch(this.client)) {
                return null;
            }
        }
        catch (IOException e) {
            return null;
        }

        // don't actually trust the call... manually verify
        int obsCatVersion = verifyAndGetSchemaVersion();
        // UAC worked
        if (obsCatVersion == schemaVersionNo) {
            log.error(_F("Catalog update was reported to be successful but did not pass "
                       + "verification: expected V%d, observed V%d",
                         schemaVersionNo+1, obsCatVersion));
            assert(false);
            System.exit(-1);
        }

        if (obsCatVersion == schemaVersionNo+1) {
            schemaVersionNo++;
        }
        else {
            assert(false);
            System.exit(-1);
        }

        long end = System.nanoTime();
        double seconds = (end - start) / 1000000000.0;

        if (newTable) {
            log.info(_F("Completed table swap in %.4f seconds", seconds));
        }
        else {
            log.info(_F("Completed %d tuples in %.4f seconds (%d tuples/sec)",
                        count, seconds, (long) (count / seconds)));
        }
        return new Pair<VoltTable,TableHelper.ViewRep>(t2, newViewRep, false);
    }

    interface SchemaChanger {
        void addProcedureClasses(Client client, Class<?>... procedures) throws IOException;
        void beginBatch();
        void createTables(VoltTable... tables) throws IOException;
        void createViews(ViewRep... views) throws IOException;
        void createProcedures(Client client, Class<?>... procedures) throws IOException;
        void dropTables(String... names) throws IOException;
        void dropViews(String... names) throws IOException;
        void dropProcedures(String... names) throws IOException;
        void updateTable(VoltTable t1, VoltTable t2) throws IOException;
        void addTablePartitionInfo(VoltTable table, String name) throws IOException;
        boolean executeBatch(Client client) throws IOException;
    }

    /**
     * SchemaChanger subclass that uses catalog updates.
     */
    static class CatalogSchemaChanger implements SchemaChanger {

        CatalogBuilder builder = null;
        byte[] catalogData = null;
        boolean isNOP = true;   // Set to false with the first real action of a batch

        CatalogSchemaChanger() {
        }

        @Override
        public void addProcedureClasses(Client client, Class<?>... procedures) throws IOException {
        }

        @Override
        public void beginBatch() {
            this.isNOP = true;
            this.builder = new CatalogBuilder();
        }

        @Override
        public void createTables(VoltTable... tables) throws IOException {
            this.isNOP = false;
            for (VoltTable table : tables) {
                log.info(_F("New Table:\n%s", TableHelper.ddlForTable(table)));
                this.builder.addLiteralSchema(TableHelper.ddlForTable(table));
            }
        }

        @Override
        public void createViews(ViewRep... views) throws IOException {
            this.isNOP = false;
            for (ViewRep view : views) {
                log.info(_F("New View:\n%s", view.ddlForView()));
                this.builder.addLiteralSchema(view.ddlForView());
            }
        }

        @Override
        public void createProcedures(Client client, Class<?>... procedures) throws IOException {
            this.isNOP = false;
            this.builder.addProcedures(procedures);
        }

        @Override
        public void dropTables(String... names) throws IOException {
        }

        @Override
        public void dropViews(String... names) throws IOException {
        }

        @Override
        public void dropProcedures(String... names) throws IOException {
        }

        @Override
        public void updateTable(VoltTable t1, VoltTable t2) throws IOException {
            this.isNOP = false;
            log.info(_F("Update Table:\n%s", TableHelper.ddlForTable(t2)));
            this.createTables(t2);
        }

        @Override
        public void addTablePartitionInfo(VoltTable table, String name) throws IOException {
            int pkeyIndex = TableHelper.getBigintPrimaryKeyIndexIfExists(table);
            this.builder.addPartitionInfo(name, table.getColumnName(pkeyIndex));
        }

        @Override
        public boolean executeBatch(Client client) throws IOException {
            boolean success = true;
            try {
                if (!this.isNOP) {
                    this.catalogData = this.builder.compileToBytes();
                    assert(this.catalogData != null);
                    success = execUpdate(client, "@UpdateApplicationCatalog", catalogData, false);
                }
            }
            finally {
                this.builder = null;
                this.isNOP = true;  // belt and suspenders
            }
            return success;
        }
    }

    /**
     * SchemaChanger subclass that uses ad hoc DDL.
     */
    static class DDLSchemaChanger implements SchemaChanger {

        StringBuilder ddl = null;

        DDLSchemaChanger() {
        }

        @Override
        public void addProcedureClasses(Client client, Class<?>... procedures) throws IOException {
            // Use @UpdateClasses to inject procedure(s).
            InMemoryJarfile jar = VoltCompilerUtils.createClassesJar(procedures);
            // true => fail hard with exception.
            execUpdate(client, "@UpdateClasses", jar.getFullJarBytes(), true);
        }

        @Override
        public void beginBatch() {
            this.ddl = new StringBuilder();
        }

        @Override
        public void createTables(VoltTable... tables) throws IOException {
            for (VoltTable table : tables) {
                this.add(TableHelper.ddlForTable(table));
            }
        }

        @Override
        public void createViews(ViewRep... views) throws IOException {
            for (ViewRep view : views) {
                this.add(view.ddlForView());
            }
        }

       @Override
       public void createProcedures(Client client, Class<?>... procedures) throws IOException {
           // Prepare CREATE PROCEDURE DDL statement(s).
           for (final Class<?> procedure : procedures) {
               this.add(_F("CREATE PROCEDURE FROM CLASS %s", procedure.getName()));
           }
       }

        @Override
        public void dropTables(String... names) throws IOException {
            for (String name : names) {
                this.add(_F("DROP TABLE %s", name));
            }
        }

        @Override
        public void dropViews(String... names) throws IOException {
            for (String name : names) {
                this.add(_F("DROP VIEW %s", name));
            }
        }

        @Override
        public void dropProcedures(String... names) throws IOException {
            for (String name : names) {
                this.add(_F("DROP PROCEDURE %s", name));
            }
        }

        @Override
        public void updateTable(VoltTable t1, VoltTable t2) throws IOException {
            this.add(TableHelper.getAlterTableDDLToMigrate(t1, t2));
        }

        @Override
        public void addTablePartitionInfo(VoltTable table, String name) throws IOException {
        }

        @Override
        public boolean executeBatch(Client client) throws IOException {
            String ddlString = this.ddl.toString();
            boolean success = true;
            try {
                if (ddl.length() > 0) {
                    log.info(_F("::: DDL Batch (BEGIN) :::\n%s\n::: DDL Batch (END) :::", ddlString));
                    try {
                        ClientResponse cr = client.callProcedure("@AdHoc", ddlString);
                        success = (cr.getStatus() == ClientResponse.SUCCESS);
                    }
                    catch (ProcCallException e) {
                        throw new IOException(e);
                    }
                }
            }
            finally {
                this.ddl = null;
            }
            return success;
        }

        void add(String query) {
            if (this.ddl.length() > 0) {
                this.ddl.append(_F(";%n"));
            }
            this.ddl.append(query);
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

    private Topology getClusterTopology() {
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
        String query = _F("select count(*) from V%d;", schemaid);
        ClientResponse cr = callROProcedureWithRetry("@AdHoc", query);
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
        topo = getClusterTopology();

        // The ad hoc DDL mechanism requires the procs be available on the server.
        schemaChanger.addProcedureClasses(client, VerifySchemaChangedA.class, VerifySchemaChangedB.class);

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
                    ClientResponse cr = callROProcedureWithRetry("VerifySchemaChanged" + tableName, sampleOffset, guessT);
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

    /**
     * Perform @Update... call.
     */
    private static boolean execUpdate(Client client, String procName, byte[] bytes, boolean hardFail) throws IOException
    {
        boolean success = false;
        ClientResponse cr = null;
        try {
            cr = client.callProcedure(procName, bytes, null);
            success = true;
        }
        catch (NoConnectionsException e) {
        }
        catch (ProcCallException e) {
            log.error(_F("Procedure %s call exception: %s", procName, e.getMessage()));
            cr = e.getClientResponse();
        }

        if (success && cr != null) {
            switch (cr.getStatus()) {
            case ClientResponse.SUCCESS:
                // hooray!
                log.info("Catalog update was reported to be successful");
                break;
            case ClientResponse.CONNECTION_LOST:
            case ClientResponse.CONNECTION_TIMEOUT:
            case ClientResponse.RESPONSE_UNKNOWN:
            case ClientResponse.SERVER_UNAVAILABLE:
                // can try again after a break
                success = false;
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

        // Fail hard or allow retries?
        if (!success && hardFail) {
            String msg = (cr != null ? ((ClientResponseImpl)cr).toJSONString() : _F("Unknown %s failure", procName));
            throw new IOException(msg);
        }

        return success;
    }
}
