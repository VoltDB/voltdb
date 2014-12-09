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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.VoltLogger;
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
import org.voltdb.compiler.VoltCompilerUtils;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;

public class SchemaChangeClient {

    static VoltLogger log = new VoltLogger("HOST");

    Client client = null;
    private SchemaChangeConfig config = null;
    private final Random rand = new Random(0);
    private final TableHelper helper;
    private Topology topo = null;
    private final AtomicInteger totalConnections = new AtomicInteger(0);
    private final AtomicInteger fatalLevel = new AtomicInteger(0);

    // Current active tables, view and verification proc. Supports dropping before re-creating.
    Set<String> activeTableNames = new HashSet<String>();
    TableHelper.ViewRep activeViewRep = null;
    Class<?> activeVerifyProc = null;

    private int schemaVersionNo = 0;

    private boolean addAlternateKey = true;

    @SuppressWarnings("unused")
    private long startTime;

    private static String _F(String str, Object... parameters) {
        return String.format(str, parameters);
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
     * Call a R/O procedure with retries and check the return code. See SchemaChangeUtility for more info.
     */
    ClientResponse callROProcedureWithRetry(String procName, Object... params) {
        return SchemaChangeUtility.callROProcedureWithRetry(client, procName, config.noProgressTimeout, params);
    }

    SchemaChangeClient(SchemaChangeConfig config) {
        this.config = config;
        TableHelper.Configuration tableHelperConfig = new TableHelper.Configuration();
        tableHelperConfig.rand = rand;
        tableHelperConfig.numAdditionalUniqueColumns = addAlternateKey ? 1 : 0;
        // Partitioning is handled here.
        tableHelperConfig.randomPartitioning = TableHelper.RandomPartitioning.CALLER;
        this.helper = new TableHelper(tableHelperConfig);
    }

    // Immutable schema change data
    private static class CatalogChangeSchema
    {
        final VoltTable table;
        final TableHelper.ViewRep viewRep;
        final String tableName;
        final boolean partitioned;
        final int pkeyIndex;
        final String pkeyName;
        final Class<?> verifyProc;

        /**
         * Fully-specified constructor.
         */
        CatalogChangeSchema(
                final VoltTable table,
                final TableHelper.ViewRep viewRep,
                final String tableName,
                final boolean partitioned,
                final int pkeyIndex,
                final String pkeyName,
                final Class<?> verifyProc) {
            this.table = table;
            this.viewRep = viewRep;
            this.tableName = tableName;
            this.partitioned = partitioned;
            this.pkeyIndex = pkeyIndex;
            this.pkeyName = pkeyName;
            this.verifyProc = verifyProc;
        }

        /**
         * Constructor for mutated table/view.
         */
        CatalogChangeSchema mutate(final VoltTable mutatedTable, TableHelper.ViewRep mutatedViewRep) {
            return new CatalogChangeSchema(
                    mutatedTable,
                    mutatedViewRep,
                    this.tableName,
                    this.partitioned,
                    this.pkeyIndex,
                    this.pkeyName,
                    this.verifyProc);
        }
    }

    /**
     * Test driver that initiates catalog changes and tracks state, e.g. for retries.
     */
    private class CatalogChangeTestDriver {

        /*
         * These members have the remaining state related to tables and views
         * that allows a retry attempt to perform the identical operation.
         */
        private CatalogChangeSchema currentSchema = null;
        private CatalogChangeSchema newSchema = null;
        private VoltTable versionT = null;
        private long count = 0;
        private long start = 0;

        /**
         * Add rows until RSS or rowcount target met.
         * Delete some rows rows (triggers compaction).
         * Re-add odd rows until RSS or rowcount target met (makes buffers out of order).
         */
        void loadTable() {
            // if #partitions is odd, delete every 2 - if even, delete every 3
            //int n = 3 - (topo.partitions % 2);

            int redundancy = topo.sites / topo.partitions;
            long realRowCount = (config.targetrowcount * topo.hosts) / redundancy;
            String tableName = this.getTableName();
            // if replicated
            if (tableName.equals("B")) {
                realRowCount /= topo.partitions;
            }

            long max = this.maxId();

            TableLoader loader = new TableLoader(client, this.currentSchema.table,
                                                 rand, config.noProgressTimeout);

            log.info(_F("Loading table %s", tableName));
            loader.load(max + 1, realRowCount);
        }

        /**
         * Grab some random rows that aren't on the first EE page for the table.
         */
        private VoltTable sample(long offset) {
            VoltTable t2 = this.currentSchema.table.clone(4096 * 1024);

            ClientResponse cr = callROProcedureWithRetry("@AdHoc",
                    String.format("select * from %s where pkey >= %d order by pkey limit 100;",
                            TableHelper.getTableName(this.currentSchema.table), offset));
            assert(cr.getStatus() == ClientResponse.SUCCESS);
            VoltTable result = cr.getResults()[0];
            result.resetRowPosition();
            while (result.advanceRow()) {
                t2.add(result);
            }

            return t2;
        }

        class SampleResults {
            VoltTable table = null;
            long sampleOffset = -1;
        }

        // deterministically sample some rows
        SampleResults sampleRows() {
            SampleResults results = new SampleResults();
            long max = this.maxId();
            if (max > 0) {
                if (max <= 100)
                    results.sampleOffset = 0;
                else
                    results.sampleOffset = Math.min((long) (max * .75), max - 100);
                assert(max >= 0);
                results.table = this.sample(results.sampleOffset);
                assert(results.table.getRowCount() > 0);
                log.info(_F("Sampled table %s from offset %d limit 100 and found %d rows.",
                        this.getTableName(), results.sampleOffset, results.table.getRowCount()));
            }
            return results;
        }

        String getTableName() {
            return TableHelper.getTableName(this.currentSchema.table);
        }

        long maxId() {
            return SchemaChangeUtility.maxId(client, this.currentSchema.table, config.noProgressTimeout);
        }

        /**
         * Implements DDL batch execution and error tracking.
         */
        class DDLBatch {
            StringBuilder ddl = null;
            String lastSuccessfulDDL = null;
            String lastFailureDDL = null;
            String lastFailureError = null;
            boolean failureForced = false;

            void begin() {
                this.ddl = new StringBuilder();
                this.failureForced = false;
            }

            void add(String queryFmt, Object... params) {
                if (this.ddl.length() > 0) {
                    this.ddl.append(_F(";%n"));
                }
                if (queryFmt.endsWith(";")) {
                    queryFmt = queryFmt.substring(0, queryFmt.length() - 1);
                }
                this.ddl.append(_F(queryFmt, params));
            }

            boolean execute() throws IOException {
                String ddlString = this.ddl.toString();
                boolean success = true;
                try {
                    if (ddlString.length() > 0) {
                        log.info(_F("\n::: DDL Batch (BEGIN) :::\n%s\n::: DDL Batch (END) :::", ddlString));
                        String error = execLiveDDL(client, ddlString, false);
                        if (error == null) {
                            this.lastSuccessfulDDL = ddlString;
                        }
                        else if (!this.failureForced) {
                            this.lastFailureDDL = ddlString;
                            this.lastFailureError = error;
                            success = false;
                        }
                        else {
                            success = false;
                        }
                    }
                }
                finally {
                    this.ddl = null;
                }
                return success;
            }
        }

        // DDL batch handler is reused for all batches.
        DDLBatch batch = new DDLBatch();

        /**
         * Perform schema changes with retry logic.
         *
         * Perform a schema change to a mutated version of the current table (80%) or
         * to a new table entirely (20%, drops and adds the new table).
         */
        void catalogChange(boolean newTable) throws Exception {
            int retryCount = 0;
            while (!this.catalogChangeInternal(newTable)) {
                retryCount++;
                if (retryCount > config.retryLimit) {
                    this.die("Retry limit (%d) exceeded.", config.retryLimit);
                }
                log.info(_F("Sleeping %d seconds before retry attempt...", config.retrySleep));
                Thread.sleep(config.retrySleep * 1000);
            }
        }

        /**
         * Internal implementation of catalogChange() that may or may not be dealing
         * with a retry.
         */
        private boolean catalogChangeInternal(Boolean newTable) throws Exception {

            log.info(_F("::::: Catalog Change (newTable=%s, firstTime=%s, retry=%s) :::::",
                        newTable.toString(),
                        this.currentSchema == null ? "yes" : "no",
                        this.newSchema != null ? "yes" : "no"));

            // table mutation requires a current schema to mutate
            assert(newTable || this.currentSchema != null);

            // retries may decide not to rerun the batch(es)
            boolean skipBatchExecution = false;

            // newSchema is null for first run or after success, and not null when retrying
            if (this.newSchema == null) {
                // add empty table with schema version number in name
                this.versionT = TableHelper.quickTable(_F("V%d (BIGINT)", schemaVersionNo + 1));
                // create or mutate table
                if (newTable) {
                    this.newSchema = createSchema();
                }
                else {
                    this.newSchema = mutateSchema();
                }
            }
            else {
                // retry may be unnecessary if transaction succeeded despite error
                skipBatchExecution = isRetryNeeded();
            }

            // becomes true when DDL batch fails
            boolean ddlBatchFailed = false;

            if (!skipBatchExecution) {
                /*
                 * Perform the drops as a separate batch because the catalog diff doesn't  deal well
                 * with a drop and create of an existing object in the same batch. E.g. dropping and
                 * re-creating a table ends up altering the table without dropping it.
                 *
                 * It's always safe to re-run the drop batch, e.g. during a retry, because all the
                 * DROP statements have IF EXISTS clauses. I.e. don't need to know whether the
                 * failure occured during the drop or the create/alter batch.
                 */
                batch.begin();
                try {
                    batch.add("DROP TABLE %s IF EXISTS", _F("V%d", schemaVersionNo));
                    if (activeViewRep != null) {
                        batch.add("DROP VIEW %s IF EXISTS", activeViewRep.viewName);
                    }
                    if (activeVerifyProc != null) {
                        batch.add("DROP PROCEDURE %s IF EXISTS", activeVerifyProc.getName());
                    }
                    if (newTable) {
                        if (activeTableNames.contains(this.newSchema.tableName)) {
                            batch.add("DROP TABLE %s IF EXISTS", this.newSchema.tableName);
                        }
                    }
                    log.info("Starting to drop database objects.");
                    if (!batch.execute()) {
                        ddlBatchFailed = true;
                    }
                }
                catch (IOException e) {
                    ddlBatchFailed = true;
                }

                // Now do the creates and alters.
                if (!ddlBatchFailed) {
                    batch.begin();
                    try {
                        // Force failure before executing the batch?
                        // Don't do on the first run (currentSchema!=null).
                        if (this.currentSchema != null && config.retryForcedPercent > rand.nextInt(100)) {
                            log.info(_F("Forcing failure"));
                            batch.add("CREATE DEATH FOR BATCH");
                            batch.failureForced = true;
                        }

                        // create version table
                        batch.add(TableHelper.ddlForTable(this.versionT));

                        // create or mutate test table (A or B)
                        if (newTable) {
                            batch.add(TableHelper.ddlForTable(this.newSchema.table));
                            this.count = 0;
                        }
                        else {
                            // already asserted currentSchema != null
                            batch.add(TableHelper.getAlterTableDDLToMigrate(
                                            this.currentSchema.table, this.newSchema.table));
                            this.count = tupleCount(this.currentSchema.table);
                        }

                        // partition table
                        if (this.newSchema.partitioned) {
                            batch.add("PARTITION TABLE %s ON COLUMN %s",
                                      this.newSchema.tableName, this.newSchema.pkeyName);
                        }

                        // create verify procedure
                        batch.add("CREATE PROCEDURE FROM CLASS %s", this.newSchema.verifyProc.getName());
                        if (this.newSchema.viewRep != null) {
                            batch.add(this.newSchema.viewRep.ddlForView());
                        }

                        //TODO
                        //String colList = StringUtils.join(",", colNames);
                        //this.add("CREATE UNIQUE INDEX IX_%s_%d ON %s (%s)", tableName, indexNumber, tableName, colList);

                        this.start = System.nanoTime();

                        if (newTable) {
                            log.info("Starting to swap tables.");
                        }
                        else {
                            log.info("Starting to change schema.");
                        }

                        if (!batch.execute()) {
                            ddlBatchFailed = true;
                        }
                    }
                    catch (IOException e) {
                        // All SchemaChanger problems become IOExceptions.
                        // This is a normal error return that is handled by the caller.
                        ddlBatchFailed = true;
                    }
                }
            }

            if (!ddlBatchFailed) {
                // prepare for next change set after success
                catalogChangeComplete(newTable);
            }

            return !ddlBatchFailed;
        }

        private void catalogChangeComplete(Boolean newTable)
        {
            if (newTable) {
                if (activeTableNames.contains(this.newSchema.tableName)) {
                    activeTableNames.remove(this.newSchema.tableName);
                }
                activeTableNames.add(this.newSchema.tableName);
            }
            activeViewRep = this.newSchema.viewRep;
            activeVerifyProc = this.newSchema.verifyProc;

            // don't actually trust the call... manually verify
            int obsCatVersion = verifyAndGetSchemaVersion();
            // UAC worked
            if (obsCatVersion == schemaVersionNo) {
                this.die("Catalog update was reported to be successful but did not pass "
                        + "verification: expected V%d, observed V%d",
                        schemaVersionNo+1, obsCatVersion);
            }

            if (obsCatVersion == schemaVersionNo+1) {
                schemaVersionNo++;
            }
            else {
                SchemaChangeUtility.die(false, null);
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

            // the new/mutated schema becomes the current one
            this.currentSchema = this.newSchema;
            this.newSchema = null;
        }

        private CatalogChangeSchema mutateSchema()
        {
            assert(this.currentSchema != null);
            VoltTable mutatedTable = helper.mutateTable(this.currentSchema.table, true);
            TableHelper.ViewRep mutatedViewRep = this.makeViewRep(mutatedTable);
            CatalogChangeSchema schema = this.currentSchema.mutate(mutatedTable, mutatedViewRep);
            return schema;
        }

        private CatalogChangeSchema createSchema()
        {
            // Invert the partitioning and flip the table name.
            // make tables name A partitioned and tables named B replicated
            boolean partitioned = this.currentSchema == null || !this.currentSchema.partitioned;
            String tableName = partitioned ? "A" : "B";
            TableHelper.RandomTable ranTable = helper.getTotallyRandomTable(tableName, partitioned);
            TableHelper.ViewRep viewRep = this.makeViewRep(ranTable.table);
            Class<?> verifyClass = partitioned ? VerifySchemaChangedA.class : VerifySchemaChangedB.class;
            CatalogChangeSchema schema = new CatalogChangeSchema(
                    ranTable.table,
                    viewRep,
                    tableName,
                    partitioned,
                    ranTable.bigintPrimaryKey,
                    ranTable.table.getColumnName(ranTable.bigintPrimaryKey),
                    verifyClass);
            return schema;
        }

        private boolean isRetryNeeded() {
            // all sorts of things expected to be just so during a retry
            assert(this.currentSchema != null);
            assert(this.newSchema != null);
            assert(this.versionT != null);
            // this.newViewRep can be null

            // If V<next> is present there is nothing to retry, just finish what we started.
            // isSchemaVersionObservable() retries internally if the connection is still bad.
            if (isSchemaVersionObservable(schemaVersionNo+1)) {
                log.info(_F("The new version table V%d is present, not retrying.", schemaVersionNo+1));
                return true;
            }
            return false;
        }

        TableHelper.ViewRep makeViewRep(VoltTable table) {
            TableHelper.ViewRep viewRep = this.currentSchema != null ? this.currentSchema.viewRep : null;
            if (viewRep == null) {
                viewRep = helper.viewRepForTable("MV", table);
            }
            else {
                if (!viewRep.compatibleWithTable(table)) {
                    viewRep = null;
                }
            }
            return viewRep;
        }

        /**
         * Check sample and return error string on failure.
         */
        String checkSample(SampleResults sampleResults) throws Exception {
            VoltTable guessT = this.currentSchema.table.clone(4096 * 1024);
            //log.info(_F("Empty clone:\n%s", guessT.toFormattedString()));

            TableHelper.migrateTable(sampleResults.table, guessT);
            //log.info(_F("Java migration:\n%s", guessT.toFormattedString()));

            // deterministically sample the same rows
            assert(sampleResults.sampleOffset >= 0);
            ClientResponse cr = callROProcedureWithRetry("VerifySchemaChanged" + this.getTableName(),
                                                         sampleResults.sampleOffset, guessT);
            assert(cr.getStatus() == ClientResponse.SUCCESS);
            VoltTable result = cr.getResults()[0];
            if (result.fetchRow(0).getLong(0) != 1) {
                return result.fetchRow(0).getString(1);
            }
            return null;
        }

        // Dump the schema to help with debugging.
        void dumpSchema(StringBuilder sb) {
            sb.append(":::: Schema Dump ::::\n\n");
            sb.append(":: TABLES ::\n\n");
            ClientResponse cr = callROProcedureWithRetry("@SystemCatalog", "TABLES");
            assert(cr.getStatus() == ClientResponse.SUCCESS);
            for (VoltTable t : cr.getResults()) {
                while (t.advanceRow()) {
                    sb.append(_F("%s: %s\n", t.getString("TABLE_TYPE"), t.getString("TABLE_NAME")));
                }
            }
            sb.append("\n");
            sb.append(":: COLUMNS ::\n\n");
            cr = callROProcedureWithRetry("@SystemCatalog", "COLUMNS");
            String lastTableName = null;
            assert(cr.getStatus() == ClientResponse.SUCCESS);
            for (VoltTable t : cr.getResults()) {
                while (t.advanceRow()) {
                    String tableName = t.getString("TABLE_NAME");
                    if (lastTableName != null && !lastTableName.equals(tableName)){
                        sb.append("\n");
                    }
                    lastTableName = tableName;
                    sb.append(_F("%s.%s %s\n", tableName, t.getString("COLUMN_NAME"), t.getString("TYPE_NAME")));
                }
            }
            sb.append("\n");
            sb.append(":: PROCEDURES ::\n\n");
            cr = callROProcedureWithRetry("@SystemCatalog", "PROCEDURES");
            assert(cr.getStatus() == ClientResponse.SUCCESS);
            for (VoltTable t : cr.getResults()) {
                while (t.advanceRow()) {
                    String procName = t.getString("PROCEDURE_NAME");
                    if (!procName.endsWith(".insert") && !procName.endsWith(".update") && !procName.endsWith(".delete") && !procName.endsWith(".upsert")) {
                        sb.append(_F("%s\n", procName));
                    }
                }
            }
        }

        void die(final String format, Object... params) {

            StringBuilder sb = new StringBuilder("FATAL ERROR\n\n");
            this.dumpSchema(sb);
            sb.append("\n");
            sb.append(":::: Live DDL Post-Mortem ::::\n\n");
            if (this.batch.lastSuccessfulDDL != null) {
                sb.append(_F(":: Last successful DDL ::\n\n%s\n", this.batch.lastSuccessfulDDL));
            }
            sb.append("\n");
            if (this.batch.lastFailureDDL != null) {
                sb.append(_F(":: Last (unforced) failure DDL ::\n\n%s\n", this.batch.lastFailureDDL));
            }
            sb.append("\n");
            if (this.batch.lastFailureError != null) {
                sb.append(_F(":: Last (unforced) failure error ::\n\n%s\n", this.batch.lastFailureError));
            }
            sb.append("\n:::: Error ::::\n\n");
            sb.append(_F(format, params));
            sb.append("\n");
            SchemaChangeUtility.die(false, sb.toString());
        }

        void addProcedureClasses(Client client, Class<?>... procedures) throws IOException {
            // Use @UpdateClasses to inject procedure(s).
            InMemoryJarfile jar = VoltCompilerUtils.createClassesJar(procedures);
            // true => fail hard with exception.
            execUpdate(client, "@UpdateClasses", jar.getFullJarBytes(), true);
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
                SchemaChangeUtility.die(false, "Catalog version is out of range");
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
    private long tupleCount(VoltTable table) {
        if (table == null) {
            return 0;
        }
        ClientResponse cr = callROProcedureWithRetry("@AdHoc",
                String.format("select count(*) from %s;", TableHelper.getTableName(table)));
        assert(cr.getStatus() == ClientResponse.SUCCESS);
        VoltTable result = cr.getResults()[0];
        return result.asScalarLong();
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
                SchemaChangeUtility.die(true, "Duration limit reached, terminating run");
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

        CatalogChangeTestDriver testDriver = new CatalogChangeTestDriver();

        // The ad hoc DDL mechanism requires the procs be available on the server.
        testDriver.addProcedureClasses(client, VerifySchemaChangedA.class, VerifySchemaChangedB.class);

        // kick off with a random new table schema
        testDriver.catalogChange(true);

        // Main test loop. Exits by exception.
        while (true) {

            // make sure the table is full and mess around with it
            testDriver.loadTable();

            for (int j = 0; j < 3; j++) {
                // deterministically sample some rows
                CatalogChangeTestDriver.SampleResults sampleResults = testDriver.sampleRows();
                //log.info(_F("First sample:\n%s", preT.toFormattedString()));

                // move to an entirely new table or migrated schema
                boolean isNewTable = (j == 0) && (rand.nextInt(5) == 0);
                testDriver.catalogChange(isNewTable);

                // if the table has been migrated, check the sampled data
                if (!isNewTable && (sampleResults.table != null)) {
                    String err = testDriver.checkSample(sampleResults);
                    if (err != null) {
                        SchemaChangeUtility.die(false, err);
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
                SchemaChangeUtility.die(false,
                        "USER_ABORT in procedure call for Catalog update: %s",
                        ((ClientResponseImpl)cr).toJSONString());
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
     * Execute DDL and returns an error string for failure or null for success.
     */
    private static String execLiveDDL(Client client, String ddl, boolean hardFail) throws IOException
    {
        String error = null;
        ClientResponse cr = null;
        try {
            cr = client.callProcedure("@AdHoc", ddl);
        }
        catch (NoConnectionsException e) {
            error = e.getLocalizedMessage();
        }
        catch (ProcCallException e) {
            error = _F("Procedure @AdHoc call exception: %s", e.getLocalizedMessage());
            cr = e.getClientResponse();
        }

        if (error == null && cr != null) {
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
                error = String.format("Communication error: %s", cr.getStatusString());
                break;
            case ClientResponse.UNEXPECTED_FAILURE:
            case ClientResponse.GRACEFUL_FAILURE:
            case ClientResponse.USER_ABORT:
                // should never happen
                SchemaChangeUtility.die(false,
                        "USER_ABORT in procedure call for live DDL: %s",
                        ((ClientResponseImpl)cr).toJSONString());
            }
        }

        if (error != null) {
            log.error(error);
            // Fail hard (or allow retries)?
            if (hardFail) {
                String msg = (cr != null ? ((ClientResponseImpl)cr).toJSONString() : _F("Unknown @AdHoc failure"));
                throw new IOException(msg);
            }
        }

        return error;
    }
}
