/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sysprocs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CatalogContext;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ReplicationRole;
import org.voltdb.StatsSelector;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.exceptions.SpecifiedException;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.VoltTableUtil;

/**
 * Formally "UpdateApplicationCatalog", this proc represents the transactional
 * part of making schema/deployment changes to VoltDB.
 *
 * It's called by UpdateApplicationCatalog, UpdateClasses, Promote and by the
 * restore planner.
 *
 */
public class UpdateCore extends VoltSystemProcedure {
    VoltLogger log = new VoltLogger("HOST");

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{
            SysProcFragmentId.PF_updateCatalogPrecheckAndSync,
            SysProcFragmentId.PF_updateCatalogPrecheckAndSyncAggregate,
            SysProcFragmentId.PF_updateCatalog,
            SysProcFragmentId.PF_updateCatalogAggregate};
    }

    /**
     * Use EE stats to get the row counts for all tables in this partition.
     * Check the provided list of tables that need to be empty against actual
     * row counts. If any of them aren't empty, stop the catalog update and
     * return the pre-provided error message that corresponds to the non-empty
     * tables.
     *
     * Each of the tablesThatMustBeEmpty strings represents a set of tables.
     * This is is a sequence of names separated by plus signs (+).  For example,
     * "A+B+C" is the set {A, B, C}, and "A" is the singleton set {A}.  In
     * these sets, only one needs to be empty.
     *
     * @param tablesThatMustBeEmpty List of sets of table names that must include
     *                              an empty table.
     * @param reasonsForEmptyTables Error messages to return if that table isn't
     * empty.
     * @param context
     */
    protected void checkForNonEmptyTables(String[] tablesThatMustBeEmpty,
                                          String[] reasonsForEmptyTables,
                                          SystemProcedureExecutionContext context)
    {
        assert(tablesThatMustBeEmpty != null);
        // no work to do if no tables need to be empty
        if (tablesThatMustBeEmpty.length == 0) {
            return;
        }
        assert(reasonsForEmptyTables != null);
        assert(reasonsForEmptyTables.length == tablesThatMustBeEmpty.length);

        // fetch the id of the tables that must be empty from the
        //  current catalog (not the new one).
        CatalogMap<Table> tables = context.getDatabase().getTables();
        List<List<String>> allTableSets = decodeTables(tablesThatMustBeEmpty);
        Map<String, Boolean> allTables = collapseSets(allTableSets);
        int[] tableIds = new int[allTables.size()];
        int i = 0;
        for (String tableName : allTables.keySet()) {
            Table table = tables.get(tableName);
            if (table == null) {
                String msg = String.format("@UpdateCore was checking to see if table %s was empty, " +
                                           "presumably as part of a schema change, and it failed to find the table " +
                                           "in the current catalog context.", tableName);
                throw new SpecifiedException(ClientResponse.UNEXPECTED_FAILURE, msg);
            }
            tableIds[i++] = table.getRelativeIndex();
        }

        // get the table stats for these tables from the EE
        final VoltTable[] s1 =
                context.getSiteProcedureConnection().getStats(StatsSelector.TABLE,
                                                              tableIds,
                                                              false,
                                                              getTransactionTime().getTime());
        if ((s1 == null) || (s1.length == 0)) {
            String tableNames = StringUtils.join(tablesThatMustBeEmpty, ", ");
            String msg = String.format("@UpdateCore was checking to see if tables (%s) were empty ," +
                                       "presumably as part of a schema change, but failed to get the row counts " +
                                       "from the native storage engine.", tableNames);
            throw new SpecifiedException(ClientResponse.UNEXPECTED_FAILURE, msg);
        }
        VoltTable stats = s1[0];

        // find all empty tables and mark that they are empty.
        while (stats.advanceRow()) {
            long tupleCount = stats.getLong("TUPLE_COUNT");
            String tableName = stats.getString("TABLE_NAME");
            boolean isEmpty = true;
            if (tupleCount > 0) {
                isEmpty = false;
            }
            allTables.put(tableName.toUpperCase(), isEmpty);
        }

        // Reexamine the sets of sets and see if any of them has
        // one empty element.  If not, then add the respective
        // error message to the output message
        String msg = "Unable to make requested schema change:\n";
        boolean allOk = true;
        for (int idx = 0; idx < allTableSets.size(); idx += 1) {
            List<String> tableNames = allTableSets.get(idx);
            boolean allNonEmpty = true;
            for (String tableName : tableNames) {
                Boolean oneEmpty = allTables.get(tableName);
                if (oneEmpty != null && oneEmpty) {
                    allNonEmpty = false;
                    break;
                }
            }
            if (allNonEmpty) {
                String errMsg = reasonsForEmptyTables[idx];
                msg += errMsg + "\n";
                allOk = false;
            }
        }
        if ( ! allOk) {
            throw new SpecifiedException(ClientResponse.GRACEFUL_FAILURE, msg);
        }
    }

    /**
     * Take a list of list of table names and collapse into a map
     * which maps all table names to false.  We will set the correct
     * values later on.  We just want to get the structure right now.
     * Note that tables may be named multiple time in the lists of
     * lists of tables.  Everything gets mapped to false, so we don't
     * care.
     *
     * @param allTableSets
     * @return
     */
    private Map<String, Boolean> collapseSets(List<List<String>> allTableSets) {
        Map<String, Boolean> answer = new TreeMap<>();
        for (List<String> tables : allTableSets) {
            for (String table : tables) {
                answer.put(table, false);
            }
        }
        return answer;
    }

    /**
     * Decode sets of names encoded as by concatenation with plus signs
     * into lists of lists of strings.  Preserve the order, since we need
     * it to match to error messages later on.
     *
     * @param tablesThatMustBeEmpty
     * @return The decoded lists.
     */
    private List<List<String>> decodeTables(String[] tablesThatMustBeEmpty) {
        List<List<String>> answer = new ArrayList<>();
        for (String tableSet : tablesThatMustBeEmpty) {
            String tableNames[] = tableSet.split("\\+");
            answer.add(Arrays.asList(tableNames));
        }
        return answer;
    }

    public static class JavaClassForTest {
        public Class<?> forName(String name, boolean initialize, ClassLoader jarfileLoader) throws ClassNotFoundException {
            return CatalogContext.classForProcedureOrUDF(name, jarfileLoader);
        }
    }

    public final static HashMap<Integer, String> m_versionMap = new HashMap<>();
    static {
        m_versionMap.put(45, "Java 1.1");
        m_versionMap.put(46, "Java 1.2");
        m_versionMap.put(47, "Java 1.3");
        m_versionMap.put(48, "Java 1.4");
        m_versionMap.put(49, "Java 5");
        m_versionMap.put(50, "Java 6");
        m_versionMap.put(51, "Java 7");
        m_versionMap.put(52, "Java 8");
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context)
    {
        if (fragmentId == SysProcFragmentId.PF_updateCatalogPrecheckAndSync) {
            String[] tablesThatMustBeEmpty = (String[]) params.getParam(0);
            String[] reasonsForEmptyTables = (String[]) params.getParam(1);

            try {
                checkForNonEmptyTables(tablesThatMustBeEmpty, reasonsForEmptyTables, context);
            } catch (Exception ex) {
                log.info("checking non-empty tables failed: " + ex.getMessage() +
                         ", cleaning up temp catalog jar file");
                VoltDB.instance().cleanUpTempCatalogJar();
                throw ex;
            }

            // Send out fragments to do the initial round-trip to synchronize
            // all the cluster sites on the start of catalog update, we'll do
            // the actual work on the second round-trip below

            // Don't actually care about the returned table, just need to send something back to the MPI
            DependencyPair success = new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateCatalogPrecheckAndSync,
                    new VoltTable(new ColumnInfo[] { new ColumnInfo("UNUSED", VoltType.BIGINT) } ));

            if (log.isDebugEnabled()) {
                log.debug("Site " + CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()) +
                        " completed data precheck.");
            }
            return success;
        }
        else if (fragmentId == SysProcFragmentId.PF_updateCatalogPrecheckAndSyncAggregate) {
            // Don't actually care about the returned table, just need to send something
            // back to the MPI scoreboard
            log.info("Site " + CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()) +
                    " acknowledged data and catalog prechecks.");
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateCatalogPrecheckAndSyncAggregate,
                    new VoltTable(new ColumnInfo[] { new ColumnInfo("UNUSED", VoltType.BIGINT) } ));
        }
        else if (fragmentId == SysProcFragmentId.PF_updateCatalog) {
            String catalogDiffCommands = (String)params.toArray()[0];
            String commands = CompressionService.decodeBase64AndDecompress(catalogDiffCommands);
            int expectedCatalogVersion = (Integer)params.toArray()[1];
            boolean requiresSnapshotIsolation = ((Byte) params.toArray()[2]) != 0;
            boolean requireCatalogDiffCmdsApplyToEE = ((Byte) params.toArray()[3]) != 0;
            boolean hasSchemaChange = ((Byte) params.toArray()[4]) != 0;
            boolean requiresNewExportGeneration = ((Byte) params.toArray()[5]) != 0;
            long genId = (Long) params.toArray()[6];
            boolean hasSecurityUserChange = ((Byte) params.toArray()[7]) != 0;

            boolean isForReplay = m_runner.getTxnState().isForReplay();

            // if this is a new catalog, do the work to update
            if (context.getCatalogVersion() == expectedCatalogVersion) {

                // update the global catalog if we get there first
                CatalogContext catalogContext =
                        VoltDB.instance().catalogUpdate(
                                commands,
                                expectedCatalogVersion,
                                genId,
                                isForReplay,
                                requireCatalogDiffCmdsApplyToEE,
                                hasSchemaChange,
                                requiresNewExportGeneration,
                                hasSecurityUserChange);

                // If the cluster is in master role only (not replica or XDCR), reset trackers.
                // The producer would have been turned off by the code above already.
                if (VoltDB.instance().getReplicationRole() == ReplicationRole.NONE &&
                    !VoltDB.instance().getReplicationActive()) {
                    context.resetDrAppliedTracker();
                }

                // update the local catalog.  Safe to do this thanks to the check to get into here.
                long txnId = m_runner.getTxnState().txnId; // txnId used to generate DR event
                long uniqueId = m_runner.getUniqueId(); // unique id used to generate DR event
                long spHandle = m_runner.getTxnState().getNotice().getSpHandle();
                context.updateCatalog(commands, catalogContext,
                        requiresSnapshotIsolation, txnId, uniqueId, spHandle,
                        isForReplay,
                        requireCatalogDiffCmdsApplyToEE, requiresNewExportGeneration);
            }
            // if seen before by this code, then check to see if this is a restart
            else if (context.getCatalogVersion() == (expectedCatalogVersion + 1)) {
                log.info(String.format("Site %s will NOT apply an assumed restarted and identical catalog update.",
                            CoreUtils.hsIdToString(m_site.getCorrespondingSiteId())));
            }
            else {
                VoltDB.crashLocalVoltDB("Invalid catalog update.  Expected version: " + expectedCatalogVersion +
                        ", current version: " + context.getCatalogVersion(), false, null);
            }

            VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            result.addRow(VoltSystemProcedure.STATUS_OK);
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateCatalog, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_updateCatalogAggregate) {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_updateCatalog));
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateCatalogAggregate, result);
        }
        else {
            VoltDB.crashLocalVoltDB(
                    "Received unrecognized plan fragment id " + fragmentId + " in UpdateApplicationCatalog",
                    false,
                    null);
        }
        throw new RuntimeException("Should not reach this code");
    }

    private final void performCatalogVerifyWork(
            String[] tablesThatMustBeEmpty,
            String[] reasonsForEmptyTables)
    {
        // Do a null round of work to sync up all the sites.  Avoids the possibility that
        // skew between nodes and/or partitions could result in cases where a catalog update
        // affects global state before transactions expecting the old catalog run
        createAndExecuteSysProcPlan(SysProcFragmentId.PF_updateCatalogPrecheckAndSync,
                SysProcFragmentId.PF_updateCatalogPrecheckAndSyncAggregate, tablesThatMustBeEmpty,
                reasonsForEmptyTables);
    }

    private final VoltTable[] performCatalogUpdateWork(
            String catalogDiffCommands,
            int expectedCatalogVersion,
            byte requiresSnapshotIsolation,
            byte requireCatalogDiffCmdsApplyToEE,
            byte hasSchemaChange,
            byte requiresNewExportGeneration,
            long genId,
            byte hasSecurityUserChange)
    {
        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_updateCatalog,
                SysProcFragmentId.PF_updateCatalogAggregate, catalogDiffCommands, expectedCatalogVersion,
                requiresSnapshotIsolation, requireCatalogDiffCmdsApplyToEE, hasSchemaChange,
                requiresNewExportGeneration, genId, hasSecurityUserChange);
    }

    /**
     * Parameters to run are provided internally and do not map to the
     * user's input.
     * @return Standard STATUS table.
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx,
                           String catalogDiffCommands,
                           int expectedCatalogVersion,
                           long genId,
                           byte[] catalogBytes,
                           byte[] catalogHash,
                           byte[] deploymentBytes,
                           byte[] deploymentHash,
                           byte worksWithElastic,
                           String[] tablesThatMustBeEmpty,
                           String[] reasonsForEmptyTables,
                           byte requiresSnapshotIsolation,
                           byte requireCatalogDiffCmdsApplyToEE,
                           byte hasSchemaChange,
                           byte requiresNewExportGeneration,
                           byte hasSecurityUserChange)
                                   throws Exception
    {
        assert(tablesThatMustBeEmpty != null);
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        long start, duration = 0;

        if (worksWithElastic == 0 && VoltZK.zkNodeExists(zk, VoltZK.elasticJoinInProgress)) {
            throw new VoltAbortException("Can't do a catalog update while an elastic join is active. Please retry catalog update later.");
        }
        final CatalogContext context = VoltDB.instance().getCatalogContext();
        if (context.catalogVersion == expectedCatalogVersion) {
            if (context.checkMismatchedPreparedCatalog(catalogHash, deploymentHash)) {
                throw new VoltAbortException("Concurrent catalog update detected, abort the current one");
            }
        } else {
            if (context.catalogVersion == (expectedCatalogVersion + 1) &&
                Arrays.equals(context.getCatalogHash(), catalogHash) &&
                Arrays.equals(context.getDeploymentHash(), deploymentHash)) {
                log.info("Restarting catalog update");
            } else {
                // impossible to happen since we only allow catalog update sequentially
                String errMsg = "Invalid catalog update.  Catalog or deployment change was planned " +
                        "against one version of the cluster configuration but that version was " +
                        "no longer live when attempting to apply the change.  This is likely " +
                        "the result of multiple concurrent attempts to change the cluster " +
                        "configuration.  Please make such changes synchronously from a single " +
                        "connection to the cluster.";
                log.warn(errMsg);
                throw new VoltAbortException(errMsg);
            }
        }

        try {
            CatalogUtil.updateCatalogToZK(zk, expectedCatalogVersion + 1, genId,
                    catalogBytes, catalogHash, deploymentBytes);
        } catch (KeeperException | InterruptedException e) {
            log.error("error writing catalog bytes on ZK during @UpdateCore");
            throw e;
        }

        // log the start of UpdateCore
        log.info("New catalog update from: " + VoltDB.instance().getCatalogContext().getCatalogLogString());
        log.info("To: catalog hash: " + Encoder.hexEncode(catalogHash).substring(0, 10) +
                ", deployment hash: " + Encoder.hexEncode(deploymentHash).substring(0, 10));

        start = System.nanoTime();

        try {
            performCatalogVerifyWork(
                    tablesThatMustBeEmpty,
                    reasonsForEmptyTables);
        }
        catch (VoltAbortException vae) {
            log.info("Catalog verification failed: " + vae.getMessage());
            // revert the catalog node on ZK
            try {
                // read the current catalog bytes
                byte[] data = zk.getData(VoltZK.catalogbytesPrevious, false, null);
                assert(data != null);
                // write to the previous catalog bytes place holder
                zk.setData(VoltZK.catalogbytes, data, -1);
            } catch (KeeperException | InterruptedException e) {
                log.error("error read/write catalog bytes on zookeeper: " + e.getMessage());
                throw e;
            }
            throw vae;
        }

        performCatalogUpdateWork(
                catalogDiffCommands,
                expectedCatalogVersion,
                requiresSnapshotIsolation,
                requireCatalogDiffCmdsApplyToEE,
                hasSchemaChange,
                requiresNewExportGeneration,
                genId,
                hasSecurityUserChange);

        duration = System.nanoTime() - start;

        VoltDB.instance().getCatalogContext().m_lastUpdateCoreDuration = duration;
        log.info("Catalog update block time (milliseconds): " + duration * 1.0 / 1000 / 1000);

        // This is when the UpdateApplicationCatalog really ends in the blocking path
        log.info(String.format("Globally updating the current application catalog and deployment " +
                               "(new hashes %s, %s).",
                               Encoder.hexEncode(catalogHash).substring(0, 10),
                               Encoder.hexEncode(deploymentHash).substring(0, 10)));

        VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        result.addRow(VoltSystemProcedure.STATUS_OK);
        return (new VoltTable[] {result});
    }
}
