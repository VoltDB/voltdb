/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.ZKUtil;
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
import org.voltdb.dtxn.UndoAction;
import org.voltdb.exceptions.SpecifiedException;
import org.voltdb.plannerv2.VoltSchemaPlus;
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
    private static VoltLogger log = new VoltLogger("HOST");
    // Map from producer cluster ID to list of table names. Only used when applying a catalog
    private static volatile Map<Byte, String[]> s_replicableTables;

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{
            SysProcFragmentId.PF_updateCatalogPrecheckAndSync,
            SysProcFragmentId.PF_updateCatalogPrecheckAndSyncAggregate,
            SysProcFragmentId.PF_updateCatalog,
            SysProcFragmentId.PF_updateCatalogAggregate};
    }

    @Override
    public long[] getAllowableSysprocFragIdsInTaskLog() {
        return new long[]{
            SysProcFragmentId.PF_updateCatalogPrecheckAndSync,
            SysProcFragmentId.PF_updateCatalog};
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

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context)
    {
        if (fragmentId == SysProcFragmentId.PF_updateCatalogPrecheckAndSync) {
            String[] tablesThatMustBeEmpty = (String[]) params.getParam(0);
            String[] reasonsForEmptyTables = (String[]) params.getParam(1);
            int nextCatalogVersion = (int) params.getParam(2);
            ColumnInfo[] column = new ColumnInfo[2];
            column[0] = new ColumnInfo("NEXT_VERSION", VoltType.INTEGER);
            column[1] = new ColumnInfo("MESSAGE", VoltType.STRING);
            VoltTable result = new VoltTable(column);
            final int numLocalSites = context.getLocalActiveSitesCount();
            if (numLocalSites == 0) {
                result.addRow(nextCatalogVersion, String.format("All sites on host %d have been de-commissioned.", context.getHostId()));
                return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateCatalogPrecheckAndSync, result);
            }

            try {
                checkForNonEmptyTables(tablesThatMustBeEmpty, reasonsForEmptyTables, context);
            } catch (Exception ex) {
                log.info("checking non-empty tables failed: " + ex.getMessage() +
                         ", cleaning up temp catalog jar file");
                VoltDB.instance().cleanUpTempCatalogJar();
                result.addRow(nextCatalogVersion, ex.getMessage());
                return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateCatalogPrecheckAndSync, result);
            }

            // Note: this call can block up to a fixed timeout waiting for data source
            // to complete closing.
            VoltDB.getExportManager().waitOnClosingSources();

            // Send out fragments to do the initial round-trip to synchronize
            // all the cluster sites on the start of catalog update, we'll do
            // the actual work on the second round-trip below

            if (log.isDebugEnabled()) {
                log.debug("Site " + CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()) +
                        " completed data precheck.");
            }
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateCatalogPrecheckAndSync, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_updateCatalogPrecheckAndSyncAggregate) {
            log.info("Site " + CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()) +
                    " acknowledged data and catalog prechecks.");
            List<VoltTable> tables = dependencies.get(SysProcFragmentId.PF_updateCatalogPrecheckAndSync);
            for (VoltTable t : tables) {
                if (t.advanceRow()) {
                    // Update failed. Remove the staged catalog after all the sites get chance to work on it (ENG-19821)
                    ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
                    final int nextVersion = (int) t.getLong("NEXT_VERSION");
                    try {
                        ZKUtil.deleteRecursively(zk, ZKUtil.joinZKPath(VoltZK.catalogbytes, Integer.toString(nextVersion)));
                    } catch (Exception e) {
                        log.error("error deleting staged catalog.", e);
                    }
                    log.info("@UpdateCore aborted for catalog version " + nextVersion);
                    throw new SpecifiedException(ClientResponse.GRACEFUL_FAILURE, t.getString("MESSAGE"));
                }
            }
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_updateCatalogPrecheckAndSyncAggregate,
                    new VoltTable(new ColumnInfo[] { new ColumnInfo("UNUSED", VoltType.BIGINT) } ));
        }
        else if (fragmentId == SysProcFragmentId.PF_updateCatalog) {
            Object[] paramsList = params.toArray();
            String catalogDiffCommands = (String)paramsList[0];
            String commands = CompressionService.decodeBase64AndDecompress(catalogDiffCommands);
            int expectedCatalogVersion = (Integer)paramsList[1];
            int nextCatalogVersion = (Integer)paramsList[2];
            boolean requiresSnapshotIsolation = ((Byte) paramsList[3]) != 0;
            boolean requireCatalogDiffCmdsApplyToEE = ((Byte) paramsList[4]) != 0;
            boolean hasSchemaChange = ((Byte) paramsList[5]) != 0;
            boolean requiresNewExportGeneration = ((Byte) paramsList[6]) != 0;
            long genId = (Long) paramsList[7];
            boolean hasSecurityUserChange = ((Byte) paramsList[8]) != 0;

            boolean isForReplay = m_runner.getTxnState().isForReplay();

            // if this is a new catalog, do the work to update
            if (context.getCatalogVersion() == expectedCatalogVersion) {

                // Bring the DR and Export buffer update to date.
                context.getSiteProcedureConnection().quiesce();

                if (context.isLowestSiteId()) {
                    registerUndoAction(new UndoAction() {
                        @Override
                        public void undo() {
                            s_replicableTables = null;
                        }

                        @Override
                        public void release() {
                            s_replicableTables = null;
                        }
                    });
                }

                // update the global catalog if we get there first
                CatalogContext catalogContext =
                        VoltDB.instance().catalogUpdate(
                                commands,
                                expectedCatalogVersion,
                                nextCatalogVersion,
                                genId,
                                isForReplay,
                                requireCatalogDiffCmdsApplyToEE,
                                hasSchemaChange,
                                requiresNewExportGeneration,
                                hasSecurityUserChange,
                                r -> s_replicableTables = r);

                // If the cluster is in master role only (not replica or XDCR), reset trackers.
                // The producer would have been turned off by the code above already.
                if (VoltDB.instance().getReplicationRole() == ReplicationRole.NONE &&
                    !VoltDB.instance().getReplicationActive()) {
                    // We are not in XDCR so clear all
                    context.resetAllDrAppliedTracker();
                }

                // update the local catalog.  Safe to do this thanks to the check to get into here.
                long txnId = m_runner.getTxnState().txnId; // txnId used to generate DR event
                long uniqueId = m_runner.getUniqueId(); // unique id used to generate DR event
                long spHandle = m_runner.getTxnState().getNotice().getSpHandle();
                context.updateCatalog(commands, catalogContext,
                        requiresSnapshotIsolation, txnId, uniqueId, spHandle,
                        isForReplay,
                        requireCatalogDiffCmdsApplyToEE, requiresNewExportGeneration, s_replicableTables);
            }
            // if seen before by this code, then check to see if this is a restart
            else if (context.getCatalogVersion() == nextCatalogVersion) {
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
            String[] reasonsForEmptyTables,
            int nextCatalogVersion)
    {
        // Do a null round of work to sync up all the sites.  Avoids the possibility that
        // skew between nodes and/or partitions could result in cases where a catalog update
        // affects global state before transactions expecting the old catalog run
        createAndExecuteSysProcPlan(SysProcFragmentId.PF_updateCatalogPrecheckAndSync,
                SysProcFragmentId.PF_updateCatalogPrecheckAndSyncAggregate, tablesThatMustBeEmpty,
                reasonsForEmptyTables, nextCatalogVersion);
    }

    private final VoltTable[] performCatalogUpdateWork(
            String catalogDiffCommands,
            int expectedCatalogVersion,
            int nextCatalogVersion,
            byte requiresSnapshotIsolation,
            byte requireCatalogDiffCmdsApplyToEE,
            byte hasSchemaChange,
            byte requiresNewExportGeneration,
            long genId,
            byte hasSecurityUserChange)
    {
        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_updateCatalog,
                SysProcFragmentId.PF_updateCatalogAggregate, catalogDiffCommands, expectedCatalogVersion,
                nextCatalogVersion, requiresSnapshotIsolation, requireCatalogDiffCmdsApplyToEE, hasSchemaChange,
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
                           int nextCatalogVersion,
                           long genId,
                           byte[] catalogHash,
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

        if (VoltZK.zkNodeExists(zk, VoltZK.elasticOperationInProgress) ||
                VoltZK.zkNodeExists(zk, VoltZK.rejoinInProgress)) {
            throw new VoltAbortException("Can't do a catalog update while an elastic join or rejoin is active. Please retry catalog update later.");
        }
        if (requiresSnapshotIsolation == 1 && VoltZK.hasHostsSnapshotting(zk)) {
            throw new VoltAbortException("Snapshot in progress. Please retry catalog update later.");
        }
        final CatalogContext context = VoltDB.instance().getCatalogContext();
        if (context.catalogVersion == expectedCatalogVersion) {
            if (context.checkMismatchedPreparedCatalog(catalogHash, deploymentHash)) {
                throw new VoltAbortException("Concurrent catalog update detected, abort the current one");
            }
        } else {
            if (context.catalogVersion == nextCatalogVersion &&
                Arrays.equals(context.getCatalogHash(), catalogHash) &&
                Arrays.equals(context.getDeploymentHash(), deploymentHash)) {
                log.info("Restarting catalog update");
                // Catalog may have been published, reset the status to PENDING if COMPLETE
                CatalogUtil.unPublishCatalog(zk, nextCatalogVersion);
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

        // log the start of UpdateCore
        log.info("New catalog update from: " + VoltDB.instance().getCatalogContext().getCatalogLogString());
        log.info("To: catalog hash: " + Encoder.hexEncode(catalogHash).substring(0, 10) +
                ", deployment hash: " + Encoder.hexEncode(deploymentHash).substring(0, 10) +
                ", version: " + nextCatalogVersion);

        start = System.nanoTime();

        try {
            performCatalogVerifyWork(
                    tablesThatMustBeEmpty,
                    reasonsForEmptyTables,
                    nextCatalogVersion);
        }
        catch (VoltAbortException vae) {
            log.info("Catalog verification failed: " + vae.getMessage());
            throw vae;
        }

        try {
            CatalogUtil.publishCatalog(zk, nextCatalogVersion);
        } catch (KeeperException | InterruptedException e) {
            log.error("error writing catalog bytes on ZK during @UpdateCore");
            throw e;
        }

        performCatalogUpdateWork(
                catalogDiffCommands,
                expectedCatalogVersion,
                nextCatalogVersion,
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
        if (AdHocNTBase.USING_CALCITE) {
            CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
            catalogContext.setSchemaPlus(VoltSchemaPlus.from(catalogContext.getDatabase()));
        }
        return (new VoltTable[] {result});
    }
}
