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

        @Option(desc = "Percent forced failures to trigger retry logic.")
        int retryForcedPercent = 0;

        @Option(desc = "Maximum number of retries.")
        int retryLimit = 20;

        @Option(desc = "Seconds between retries.")
        int retrySleep = 10;

        @Override
        public void validate() {
            if (targetrowcount <= 0) exitWithMessageAndUsage("targetrowcount must be > 0");
            if (duration < 0) exitWithMessageAndUsage("duration must be >= 0");
            if (retryForcedPercent < 0 || retryForcedPercent > 100) exitWithMessageAndUsage("retryForcedPercent must be >= 0 and <= 100");
            if (retryLimit < 0) exitWithMessageAndUsage("retryLimit must be >= 0");
            if (retrySleep < 0) exitWithMessageAndUsage("retrySleep must be >= 0");
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
     * Test driver that initiates catalog changes and tracks state, e.g. for retries.
     */
    private class CatalogChangeTestDriver {

        // Keep track of retries to limit how many occur.
        private int retryCount = 0;

        /*
         * These members have the remaining state related to tables and views
         * that allows a retry attempt to perform the identical operation.
         */
        private VoltTable t2 = null;
        private String currentName = null;
        private String newName = null;
        private VoltTable versionT = null;
        private TableHelper.ViewRep newViewRep = null;
        private long count = 0;
        private long start = 0;
        private Class<?> provisionalActiveVerifyProc = null;


        // Forces random failures.
        class FailBot {
            // Number of possible places to generate failures.
            final int failPoints;
            final boolean enabled;

            FailBot(int failPoints, boolean enabled) {
                this.failPoints = failPoints;
                this.enabled = enabled;
            }

            boolean failHere(String desc) {
                // Divide by the number of possible places to force failures.
                if (this.enabled && (config.retryForcedPercent / this.failPoints) > rand.nextInt(100)) {
                    log.info(_F("Forcing failure: %s", desc));
                    return true;
                }
                return false;
            }
        }

        /**
         * Perform a schema change to a mutated version of the current table (80%) or
         * to a new table entirely (20%, drops and adds the new table).
         */
        Pair<VoltTable,TableHelper.ViewRep> catalogChange(
                VoltTable t1,
                boolean newTable,
                TableHelper.ViewRep viewIn,
                boolean isRetry) throws Exception {

            // There are 3 possible forced failure points. Don't force failures during
            // the initial call.
            FailBot failBot = new FailBot(3, t1 != null);

            // A retry may skip the drops if they had succeeded.
            boolean skipDrops = false;

            if (isRetry) {
                log.info(_F("Catalog update retry #%d for V%d in %d seconds...",
                            this.retryCount+1, schemaVersionNo+1, config.retrySleep));
                Thread.sleep(config.retrySleep * 1000);

                if (++this.retryCount >= config.retryLimit) {
                    throw new IOException("Retry limit reached");
                }
                assert(this.t2 != null);
                assert(this.currentName != null);
                assert(this.newName != null);
                assert(this.versionT != null);
                assert(this.provisionalActiveVerifyProc != null);
                // this.newViewRep can be null

                // Figure out what we have to redo, if anything. Non-transaction-related
                // problems can return errors while still completing the transation.

                // If V<next> is present there is nothing to retry, just finish what we started.
                // isSchemaVersionObservable() retries internally if the connection is still bad.
                if (isSchemaVersionObservable(schemaVersionNo+1)) {
                    log.info(_F("The new version table V%d is present, not retrying.", schemaVersionNo+1));
                    return finishUpdate(newTable);
                }

                if (isSchemaVersionObservable(schemaVersionNo)) {
                    // If V<previous> is present we need to repeat the drop batch.
                    log.info(_F("The old version table V%d is present, enabling drop batch.", schemaVersionNo));
                }
                else {
                    // If V<previous> is not present there is no need to repeat the drop batch.
                    log.info(_F("The old version table V%d is not present, disabling drop batch.", schemaVersionNo));
                    skipDrops = true;
                }
            }

            // We may have determined that a retry isn't really necessary, e.g. if a
            // batch succeeds but has a communication problem at the tail end.
            if (this.retryCount == 0) {
                this.t2 = null;
                this.currentName = t1 == null ? "B" : TableHelper.getTableName(t1);
                this.newName = this.currentName;

                // add an empty table with the schema version number in the name
                this.versionT = TableHelper.quickTable(_F("V%d (BIGINT)", schemaVersionNo + 1));

                if (newTable) {
                    this.newName = this.currentName.equals("A") ? "B" : "A";
                    this.t2 = TableHelper.getTotallyRandomTable(this.newName, rand, false);
                }
                else {
                    this.t2 = TableHelper.mutateTable(t1, true, rand);
                }

                // handle views
                this.newViewRep = viewIn;
                if (this.newViewRep == null) {
                    this.newViewRep = TableHelper.ViewRep.viewRepForTable("MV", this.t2, rand);
                }
                else {
                    if (!this.newViewRep.compatibleWithTable(this.t2)) {
                        this.newViewRep = null;
                    }
                }
            }

            // Do the drops as a separate batch because the UAC-based mechanism currently
            // doesn't deal well with a batch dropping and re-creating an existing table.
            // The catalog update implementation ignores this stage. The stage is skipped
            // during retries when V# isn't found because the drop batch had succeeded.
            if (!skipDrops) {
                this.logStage("drop");
                schemaChanger.beginBatch();
                try {
                    // Force failure?
                    if (failBot.failHere("in drop batch")) {
                        schemaChanger.addForcedFailure();
                    }
                    schemaChanger.dropTables(_F("V%d", schemaVersionNo));
                    if (activeViewRep != null) {
                        schemaChanger.dropViews(activeViewRep.viewName);
                    }
                    activeViewRep = this.newViewRep;
                    if (activeVerifyProc != null) {
                        schemaChanger.dropProcedures(activeVerifyProc.getName());
                    }
                    if (newTable) {
                        if (activeTableNames.contains(this.newName)) {
                            schemaChanger.dropTables(this.newName);
                            activeTableNames.remove(this.newName);
                        }
                    }
                    if (!schemaChanger.executeBatch(client)) {
                        return null;
                    }
                    activeViewRep = null;
                    activeVerifyProc = null;
                }
                catch (IOException e) {
                    // All SchemaChanger problems become IOExceptions.
                    // This is a normal error return that is handled by the caller.
                    return null;
                }
            }

            // Now do the creates and alters.
            this.logStage("create/alter");
            schemaChanger.beginBatch();
            this.count = 0;
            this.start = 0;
            this.provisionalActiveVerifyProc = null;
            try {
                // Force failure?
                if (failBot.failHere("in create/alter batch")) {
                    schemaChanger.addForcedFailure();
                }
                schemaChanger.createTables(this.versionT);
                // make tables name A partitioned and tables named B replicated
                boolean partitioned = this.newName.equalsIgnoreCase("A");
                if (newTable) {
                    schemaChanger.createTables(this.t2);
                }
                else {
                    schemaChanger.updateTable(t1, this.t2);
                }
                if (partitioned) {
                    schemaChanger.addTablePartitionInfo(this.t2, this.newName);
                    this.provisionalActiveVerifyProc = VerifySchemaChangedA.class;
                }
                else {
                    this.provisionalActiveVerifyProc = VerifySchemaChangedB.class;
                }
                schemaChanger.createProcedures(client, this.provisionalActiveVerifyProc);
                if (activeViewRep != null) {
                    schemaChanger.createViews(activeViewRep);
                }

                this.count = tupleCount(t1);
                this.start = System.nanoTime();

                if (newTable) {
                    log.info("Starting to swap tables.");
                }
                else {
                    log.info("Starting to change schema.");
                }

                if (!schemaChanger.executeBatch(client)) {
                    return null;
                }

                // Exercise the retry logic by simulating a communication failure after
                // successfully executing the batch. (t1 test avoids failing initial call)
                if (failBot.failHere("after create/alter batch")) {
                    return null;
                }
            }
            catch (IOException e) {
                // All SchemaChanger problems become IOExceptions.
                // This is a normal error return that is handled by the caller.
                return null;
            }

            return finishUpdate(newTable);
        }

        private Pair<VoltTable, TableHelper.ViewRep> finishUpdate(boolean newTable)
        {
            if (newTable) {
                activeTableNames.add(this.newName);
            }
            activeVerifyProc = this.provisionalActiveVerifyProc;

            // We presumably succeeded, so it's no longer a retry situation.
            this.retryCount = 0;

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
            double seconds = (end - this.start) / 1000000000.0;

            if (newTable) {
                log.info(_F("Completed table swap in %.4f seconds", seconds));
            }
            else {
                log.info(_F("Completed %d tuples in %.4f seconds (%d tuples/sec)",
                            this.count, seconds, (long) (this.count / seconds)));
            }
            return new Pair<VoltTable,TableHelper.ViewRep>(this.t2, this.newViewRep, false);
        }

        private void logStage(final String action) {
            // Only log with retries because a stage might be skipped.
            if (this.retryCount > 0) {
                log.info(_F("Retry #%d: %s", this.retryCount, action));
            }
        }
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
        void addForcedFailure() throws IOException;
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
        public void addForcedFailure() throws IOException {
            // Unimplemented.
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
                this.add(_F("DROP TABLE %s IF EXISTS", name));
            }
        }

        @Override
        public void dropViews(String... names) throws IOException {
            for (String name : names) {
                this.add(_F("DROP VIEW %s IF EXISTS", name));
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
        public void addForcedFailure() throws IOException {
            this.add("DRIZZLE GOBBLE GUNK");
        }

        @Override
        public boolean executeBatch(Client client) throws IOException {
            String ddlString = this.ddl.toString();
            boolean success = true;
            try {
                if (ddl.length() > 0) {
                    log.info(_F("\n::: DDL Batch (BEGIN) :::\n%s\n::: DDL Batch (END) :::", ddlString));
                    success = execLiveDDL(client, ddlString, false);
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

        CatalogChangeTestDriver testDriver = new CatalogChangeTestDriver();

        // kick this off with a random schema
        VoltTable t = null;
        TableHelper.ViewRep v = null;
        while (t == null) {
            Pair<VoltTable, TableHelper.ViewRep> schema = testDriver.catalogChange(null, true, null, false);
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
                boolean isRetry = false;
                while (newT == null) {
                    Pair<VoltTable, TableHelper.ViewRep> schema = testDriver.catalogChange(t, isNewTable, v, isRetry);
                    if (schema == null) {
                        isRetry = true;
                    } else {
                        isRetry = false;
                        newT = schema.getFirst();
                        newV = schema.getSecond();
                    }
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
     * Perform an @UpdateApplicationCatalog or @UpdateClasses call.
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

    /**
     * Execute live DDL.
     */
    private static boolean execLiveDDL(Client client, String ddl, boolean hardFail) throws IOException
    {
        boolean success = false;
        ClientResponse cr = null;
        try {
            cr = client.callProcedure("@AdHoc", ddl);
            success = true;
        }
        catch (NoConnectionsException e) {
        }
        catch (ProcCallException e) {
            log.error(_F("Procedure @AdHoc call exception: %s", e.getMessage()));
            cr = e.getClientResponse();
        }

        if (success && cr != null) {
            switch (cr.getStatus()) {
            case ClientResponse.SUCCESS:
                // hooray!
                log.info("Live DDL execution was reported to be successful");
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
                log.error(_F("USER_ABORT in procedure call for live DDL"));
                log.error(((ClientResponseImpl)cr).toJSONString());
                assert(false);
                System.exit(-1);
            }
        }

        // Fail hard or allow retries?
        if (!success && hardFail) {
            String msg = (cr != null ? ((ClientResponseImpl)cr).toJSONString() : _F("Unknown @AdHoc failure"));
            throw new IOException(msg);
        }

        return success;
    }
}
