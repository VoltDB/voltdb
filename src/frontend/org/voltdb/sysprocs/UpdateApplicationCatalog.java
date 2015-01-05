/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
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
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.exceptions.SpecifiedException;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.CatalogAndIds;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.InMemoryJarfile.JarLoader;
import org.voltdb.utils.VoltTableUtil;

import com.google_voltpatches.common.base.Throwables;

@ProcInfo(singlePartition = false)
public class UpdateApplicationCatalog extends VoltSystemProcedure {

    VoltLogger log = new VoltLogger("HOST");

    private static final int DEP_updateCatalogSync = (int)
            SysProcFragmentId.PF_updateCatalogPrecheckAndSync | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_updateCatalogSyncAggregate = (int)
            SysProcFragmentId.PF_updateCatalogPrecheckAndSyncAggregate;
    private static final int DEP_updateCatalog = (int)
            SysProcFragmentId.PF_updateCatalog | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_updateCatalogAggregate = (int)
            SysProcFragmentId.PF_updateCatalogAggregate;

    @Override
    public void init() {
        registerPlanFragment(SysProcFragmentId.PF_updateCatalogPrecheckAndSync);
        registerPlanFragment(SysProcFragmentId.PF_updateCatalogPrecheckAndSyncAggregate);
        registerPlanFragment(SysProcFragmentId.PF_updateCatalog);
        registerPlanFragment(SysProcFragmentId.PF_updateCatalogAggregate);
    }

    /**
     * Use EE stats to get the row counts for all tables in this partition.
     * Check the provided list of tables that need to be empty against actual
     * row counts. If any of them aren't empty, stop the catalog update and
     * return the pre-provided error message that corresponds to the non-empty
     * tables.
     *
     * @param tablesThatMustBeEmpty List of table names that must be empty.
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
        int[] tableIds = new int[tablesThatMustBeEmpty.length];
        int i = 0;
        for (String tableName : tablesThatMustBeEmpty) {
            Table table = tables.get(tableName);
            if (table == null) {
                String msg = String.format("@UpdateApplicationCatalog was checking to see if table %s was empty, " +
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
            String msg = String.format("@UpdateApplicationCatalog was checking to see if tables (%s) were empty ," +
                                       "presumably as part of a schema change, but failed to get the row counts " +
                                       "from the native storage engine.", tableNames);
            throw new SpecifiedException(ClientResponse.UNEXPECTED_FAILURE, msg);
        }
        VoltTable stats = s1[0];
        SortedSet<String> nonEmptyTables = new TreeSet<String>();

        // find all empty tables
        while (stats.advanceRow()) {
            long tupleCount = stats.getLong("TUPLE_COUNT");
            String tableName = stats.getString("TABLE_NAME");
            if (tupleCount > 0) {
                nonEmptyTables.add(tableName);
            }
        }

        // return an error containing the names of all non-empty tables
        // via the propagated reasons why each needs to be empty
        if (!nonEmptyTables.isEmpty()) {
            String msg = "Unable to make requested schema change:\n";
            for (i = 0; i < tablesThatMustBeEmpty.length; ++i) {
                if (nonEmptyTables.contains(tablesThatMustBeEmpty[i])) {
                    msg += reasonsForEmptyTables[i] + "\n";
                }
            }
            throw new SpecifiedException(ClientResponse.GRACEFUL_FAILURE, msg);
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
            checkForNonEmptyTables(tablesThatMustBeEmpty, reasonsForEmptyTables, context);

            // Send out fragments to do the initial round-trip to synchronize
            // all the cluster sites on the start of catalog update, we'll do
            // the actual work on the *next* round-trip below
            // Don't actually care about the returned table, just need to send something
            // back to the MPI scoreboard

            // We know the ZK bytes are okay because the run() method wrote them before sending
            // out fragments
            CatalogAndIds catalogStuff = null;
            try {
                catalogStuff = CatalogUtil.getCatalogFromZK(VoltDB.instance().getHostMessenger().getZK());
                InMemoryJarfile testjar = new InMemoryJarfile(catalogStuff.catalogBytes);
                JarLoader testjarloader = testjar.getLoader();
                for (String classname : testjarloader.getClassNames()) {
                    try {
                        Class.forName(classname, true, testjarloader);
                    }
                    // LinkageError catches most of the various class loading errors we'd
                    // care about here.
                    catch (LinkageError | ClassNotFoundException e) {
                        String cause = e.getMessage();
                        if (cause == null && e.getCause() != null) {
                            cause = e.getCause().getMessage();
                        }
                        String msg = "Error loading class: " + classname + " from catalog: " +
                            e.getClass().getCanonicalName() + ", " + cause;
                        log.warn(msg);
                        throw new VoltAbortException(e);
                    }
                }
            } catch (Exception e) {
                Throwables.propagate(e);
            }

            return new DependencyPair(DEP_updateCatalogSync,
                    new VoltTable(new ColumnInfo[] { new ColumnInfo("UNUSED", VoltType.BIGINT) } ));
        }
        else if (fragmentId == SysProcFragmentId.PF_updateCatalogPrecheckAndSyncAggregate) {
            // Don't actually care about the returned table, just need to send something
            // back to the MPI scoreboard
            return new DependencyPair(DEP_updateCatalogSyncAggregate,
                    new VoltTable(new ColumnInfo[] { new ColumnInfo("UNUSED", VoltType.BIGINT) } ));
        }
        else if (fragmentId == SysProcFragmentId.PF_updateCatalog) {
            String catalogDiffCommands = (String)params.toArray()[0];
            String commands = Encoder.decodeBase64AndDecompress(catalogDiffCommands);
            int expectedCatalogVersion = (Integer)params.toArray()[1];
            boolean requiresSnapshotIsolation = ((Byte) params.toArray()[2]) != 0;

            CatalogAndIds catalogStuff = null;
            try {
                catalogStuff = CatalogUtil.getCatalogFromZK(VoltDB.instance().getHostMessenger().getZK());
            } catch (Exception e) {
                Throwables.propagate(e);
            }

            String replayInfo = m_runner.getTxnState().isForReplay() ? " (FOR REPLAY)" : "";

            // if this is a new catalog, do the work to update
            if (context.getCatalogVersion() == expectedCatalogVersion) {

                // update the global catalog if we get there first
                @SuppressWarnings("deprecation")
                Pair<CatalogContext, CatalogSpecificPlanner> p =
                VoltDB.instance().catalogUpdate(
                        commands,
                        catalogStuff.catalogBytes,
                        catalogStuff.getCatalogHash(),
                        expectedCatalogVersion,
                        getVoltPrivateRealTransactionIdDontUseMe(),
                        getUniqueId(),
                        catalogStuff.deploymentBytes,
                        catalogStuff.getDeploymentHash());

                // update the local catalog.  Safe to do this thanks to the check to get into here.
                context.updateCatalog(commands, p.getFirst(), p.getSecond(), requiresSnapshotIsolation);

                log.debug(String.format("Site %s completed catalog update with catalog hash %s, deployment hash %s%s.",
                        CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()),
                        Encoder.hexEncode(catalogStuff.getCatalogHash()).substring(0, 10),
                        Encoder.hexEncode(catalogStuff.getDeploymentHash()).substring(0, 10),
                        replayInfo));
            }
            // if seen before by this code, then check to see if this is a restart
            else if ((context.getCatalogVersion() == (expectedCatalogVersion + 1) &&
                     (Arrays.equals(context.getCatalogHash(), catalogStuff.getCatalogHash()) &&
                      Arrays.equals(context.getDeploymentHash(), catalogStuff.getDeploymentHash()))))
            {
                log.info(String.format("Site %s will NOT apply an assumed restarted and identical catalog update with catalog hash %s and deployment hash %s.",
                            CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()),
                            Encoder.hexEncode(catalogStuff.getCatalogHash()),
                            Encoder.hexEncode(catalogStuff.getDeploymentHash())));
            }
            else {
                VoltDB.crashLocalVoltDB("Invalid catalog update.  Expected version: " + expectedCatalogVersion +
                        ", current version: " + context.getCatalogVersion(), false, null);
            }

            VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            result.addRow(VoltSystemProcedure.STATUS_OK);
            return new DependencyPair(DEP_updateCatalog, result);
        } else if (fragmentId == SysProcFragmentId.PF_updateCatalogAggregate) {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_updateCatalog));
            return new DependencyPair(DEP_updateCatalogAggregate, result);
        } else {
            VoltDB.crashLocalVoltDB(
                    "Received unrecognized plan fragment id " + fragmentId + " in UpdateApplicationCatalog",
                    false,
                    null);
        }
        throw new RuntimeException("Should not reach this code");
    }

    private final void performCatalogVerifyWork(
            String catalogDiffCommands,
            int expectedCatalogVersion,
            String[] tablesThatMustBeEmpty,
            String[] reasonsForEmptyTables,
            byte requiresSnapshotIsolation)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // Do a null round of work to sync up all the sites.  Avoids the possibility that
        // skew between nodes and/or partitions could result in cases where a catalog update
        // affects global state before transactions expecting the old catalog run

        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_updateCatalogPrecheckAndSync;
        pfs[0].outputDepId = DEP_updateCatalogSync;
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.fromArrayNoCopy(tablesThatMustBeEmpty, reasonsForEmptyTables);

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_updateCatalogPrecheckAndSyncAggregate;
        pfs[1].outputDepId = DEP_updateCatalogSyncAggregate;
        pfs[1].inputDepIds  = new int[] { DEP_updateCatalogSync };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        executeSysProcPlanFragments(pfs, DEP_updateCatalogSyncAggregate);
    }

    private final VoltTable[] performCatalogUpdateWork(
            String catalogDiffCommands,
            int expectedCatalogVersion,
            byte requiresSnapshotIsolation)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // Now do the real work
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_updateCatalog;
        pfs[0].outputDepId = DEP_updateCatalog;
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.fromArrayNoCopy(
                catalogDiffCommands, expectedCatalogVersion, requiresSnapshotIsolation);

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_updateCatalogAggregate;
        pfs[1].outputDepId = DEP_updateCatalogAggregate;
        pfs[1].inputDepIds  = new int[] { DEP_updateCatalog };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();


        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_updateCatalogAggregate);
        return results;
    }

    /**
     * Parameters to run are provided internally and do not map to the
     * user's input.
     * @param ctx
     * @param catalogDiffCommands
     * @param catalogURL
     * @param expectedCatalogVersion
     * @return Standard STATUS table.
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx,
                           String catalogDiffCommands,
                           byte[] catalogHash,
                           byte[] catalogBytes,
                           int expectedCatalogVersion,
                           String deploymentString,
                           String[] tablesThatMustBeEmpty,
                           String[] reasonsForEmptyTables,
                           byte requiresSnapshotIsolation,
                           byte worksWithElastic,
                           byte[] deploymentHash)
                                   throws Exception
    {
        assert(tablesThatMustBeEmpty != null);

        /*
         * Validate that no elastic join is in progress, blocking this catalog update.
         * If this update works with elastic then do the update anyways
         */
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        if (worksWithElastic == 0 &&
            !zk.getChildren(VoltZK.catalogUpdateBlockers, false).isEmpty()) {
            throw new VoltAbortException("Can't do a catalog update while an elastic join or rejoin is active");
        }

        // Pull the current catalog and deployment version and hash info.  Validate that we're either:
        // (a) starting a new, valid catalog or deployment update
        // (b) restarting a valid catalog or deployment update
        // otherwise, we can bomb out early.  This should guarantee that we only
        // ever write valid catalog and deployment state to ZK.
        CatalogAndIds catalogStuff = CatalogUtil.getCatalogFromZK(zk);
        // New update?
        if (catalogStuff.version == expectedCatalogVersion) {
            log.debug("New catalog update from: " + catalogStuff.toString());
            log.debug("To: catalog hash: " + Encoder.hexEncode(catalogHash).substring(0, 10) +
                    ", deployment hash: " + Encoder.hexEncode(deploymentHash).substring(0, 10));
        }
        // restart?
        else {
            if (catalogStuff.version == (expectedCatalogVersion + 1) &&
                (Arrays.equals(catalogStuff.getCatalogHash(), catalogHash) &&
                 Arrays.equals(catalogStuff.getDeploymentHash(), deploymentHash)))
            {
                log.debug("Restarting catalog update: " + catalogStuff.toString());
            }
            else {
                String errmsg = "Invalid catalog update.  Catalog or deployment change was planned " +
                        "against one version of the cluster configuration but that version was " +
                        "no longer live when attempting to apply the change.  This is likely " +
                        "the result of multiple concurrent attempts to change the cluster " +
                        "configuration.  Please make such changes synchronously from a single " +
                        "connection to the cluster.";
                log.warn(errmsg);
                throw new VoltAbortException(errmsg);
            }
        }

        byte[] deploymentBytes = deploymentString.getBytes("UTF-8");
        // update the global version. only one site per node will accomplish this.
        // others will see there is no work to do and gracefully continue.
        // then update data at the local site.
        CatalogUtil.updateCatalogToZK(
                zk,
                expectedCatalogVersion + 1,
                getVoltPrivateRealTransactionIdDontUseMe(),
                getUniqueId(),
                catalogBytes,
                deploymentBytes);

        try {
            performCatalogVerifyWork(
                    catalogDiffCommands,
                    expectedCatalogVersion,
                    tablesThatMustBeEmpty,
                    reasonsForEmptyTables,
                    requiresSnapshotIsolation);
        }
        catch (VoltAbortException vae) {
            // If there is a cluster failure before this point, we will re-run
            // the transaction with the same input args and the new state,
            // which we will recognize as a restart and do the right thing.
            log.debug("Catalog update cannot be applied.  Rolling back ZK state");
            CatalogUtil.updateCatalogToZK(
                    zk,
                    catalogStuff.version,
                    catalogStuff.txnId,
                    catalogStuff.uniqueId,
                    catalogStuff.catalogBytes,
                    catalogStuff.deploymentBytes);

            // hopefully this will throw a SpecifiedException if the fragment threw one
            throw vae;
            // If there is a cluster failure after this point, we will re-run
            // the transaction with the same input args and the old state,
            // which will look like a new UAC transaction.  If there is no
            // cluster failure, we leave the ZK state consistent with the
            // catalog state which we entered here with.
        }

        performCatalogUpdateWork(
                catalogDiffCommands,
                expectedCatalogVersion,
                requiresSnapshotIsolation);

        VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        result.addRow(VoltSystemProcedure.STATUS_OK);
        return (new VoltTable[] {result});
    }
}
