/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSaveAPI;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TheHashinator;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.sysprocs.saverestore.HashinatorSnapshotData;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.VoltTableUtil;

import com.google.common.primitives.Longs;

@ProcInfo(singlePartition = false)
public class SnapshotSave extends VoltSystemProcedure
{
    private static final VoltLogger TRACE_LOG = new VoltLogger(SnapshotSave.class.getName());
    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    private static final int DEP_saveTest = (int)
        SysProcFragmentId.PF_saveTest | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_saveTestResults = (int)
        SysProcFragmentId.PF_saveTestResults;
    private static final int DEP_createSnapshotTargets = (int)
        SysProcFragmentId.PF_createSnapshotTargets | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_createSnapshotTargetsResults = (int)
        SysProcFragmentId.PF_createSnapshotTargetsResults;
    private static final int DEP_snapshotSaveQuiesce = (int)
        SysProcFragmentId.PF_snapshotSaveQuiesce | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_snapshotSaveQuiesceResults = (int)
        SysProcFragmentId.PF_snapshotSaveQuiesceResults;

    public static final ColumnInfo nodeResultsColumns[] =
        new ColumnInfo[] {
            new ColumnInfo(CNAME_HOST_ID, CTYPE_ID),
            new ColumnInfo("HOSTNAME", VoltType.STRING),
            new ColumnInfo("TABLE", VoltType.STRING),
            new ColumnInfo("RESULT", VoltType.STRING),
            new ColumnInfo("ERR_MSG", VoltType.STRING)
    };

    public static final ColumnInfo partitionResultsColumns[] =
        new ColumnInfo[] {
                          new ColumnInfo(CNAME_HOST_ID, CTYPE_ID),
                          new ColumnInfo("HOSTNAME", VoltType.STRING),
                          new ColumnInfo(CNAME_SITE_ID, CTYPE_ID),
                          new ColumnInfo("RESULT", VoltType.STRING),
                          new ColumnInfo("ERR_MSG", VoltType.STRING)
    };

    public static final VoltTable constructNodeResultsTable()
    {
        return new VoltTable(nodeResultsColumns);
    }

    public static final VoltTable constructPartitionResultsTable()
    {
        return new VoltTable(partitionResultsColumns);
    }


    @Override
    public void init()
    {
        registerPlanFragment(SysProcFragmentId.PF_saveTest);
        registerPlanFragment(SysProcFragmentId.PF_saveTestResults);
        registerPlanFragment(SysProcFragmentId.PF_createSnapshotTargets);
        registerPlanFragment(SysProcFragmentId.PF_createSnapshotTargetsResults);
        registerPlanFragment(SysProcFragmentId.PF_snapshotSaveQuiesce);
        registerPlanFragment(SysProcFragmentId.PF_snapshotSaveQuiesceResults);
    }

    @Override
    public DependencyPair
    executePlanFragment(Map<Integer, List<VoltTable>> dependencies,
                        long fragmentId,
                        ParameterSet params,
                        SystemProcedureExecutionContext context)
    {
        String hostname = CoreUtils.getHostnameOrAddress();
        if (fragmentId == SysProcFragmentId.PF_saveTest)
        {
            assert(params.toArray()[0] != null);
            assert(params.toArray()[1] != null);
            assert(params.toArray()[2] != null);
            String file_path = (String) params.toArray()[0];
            String file_nonce = (String) params.toArray()[1];
            SnapshotFormat format =
                    SnapshotFormat.getEnumIgnoreCase((String) params.toArray()[2]);
            String data = (String) params.toArray()[3];
            return saveTest(file_path, file_nonce, format, data, context, hostname);
        }
        else if (fragmentId == SysProcFragmentId.PF_saveTestResults)
        {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_saveTest));
            return new DependencyPair( DEP_saveTestResults, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_createSnapshotTargets)
        {
            assert(params.toArray()[0] != null);
            assert(params.toArray()[1] != null);
            assert(params.toArray()[2] != null);
            assert(params.toArray()[3] != null);
            assert(params.toArray()[4] != null);
            assert(params.toArray()[5] != null);
            assert(params.toArray()[6] != null);
            assert(params.toArray()[9] != null);
            final String file_path = (String) params.toArray()[0];
            final String file_nonce = (String) params.toArray()[1];
            final long txnId = (Long)params.toArray()[2];
            long perPartitionTxnIds[] = (long[])params.toArray()[3];
            byte block = (Byte)params.toArray()[4];
            SnapshotFormat format =
                    SnapshotFormat.getEnumIgnoreCase((String) params.toArray()[5]);

            /*
             * Filter out the partitions that are active in the cluster
             * and include values for all partitions that aren't part of the current cluster.
             * These transaction ids are used to track partitions that have come and gone
             * so that the ids can resume without duplicates if the partitions are brought back.
             */
            List<Long> perPartitionTransactionIdsToKeep = new ArrayList<Long>();
            for (long txnid : perPartitionTxnIds) {
                int partitionId = TxnEgo.getPartitionId(txnid);
                if (partitionId >= context.getNumberOfPartitions() && partitionId != MpInitiator.MP_INIT_PID) {
                    perPartitionTransactionIdsToKeep.add(txnid);
                }
            }

            String data = (String) params.toArray()[6];
            HashinatorSnapshotData hashinatorData =
                    new HashinatorSnapshotData((byte[]) params.toArray()[7], (Long) params.toArray()[8]);
            final long timestamp = (Long)params.toArray()[9];
            SnapshotSaveAPI saveAPI = new SnapshotSaveAPI();
            VoltTable result = saveAPI.startSnapshotting(file_path, file_nonce,
                                                         format, block, txnId,
                                                         context.getLastCommittedSpHandle(),
                                                         Longs.toArray(perPartitionTransactionIdsToKeep),
                                                         data, context, hostname, hashinatorData, timestamp);
            return new DependencyPair(SnapshotSave.DEP_createSnapshotTargets, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_createSnapshotTargetsResults)
        {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_createSnapshotTargets));
            return new DependencyPair(DEP_createSnapshotTargetsResults, result);
        } else if (fragmentId == SysProcFragmentId.PF_snapshotSaveQuiesce) {
            // tell each site to quiesce
            context.getSiteProcedureConnection().quiesce();
            VoltTable results = new VoltTable(new ColumnInfo("id", VoltType.BIGINT));
            results.addRow(context.getSiteId());
            return new DependencyPair(DEP_snapshotSaveQuiesce, results);
        }
        else if (fragmentId == SysProcFragmentId.PF_snapshotSaveQuiesceResults) {
            VoltTable dummy = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            dummy.addRow(VoltSystemProcedure.STATUS_OK);
            return new DependencyPair(DEP_snapshotSaveQuiesceResults, dummy);
        }
        assert (false);
        return null;
    }

    private DependencyPair saveTest(String file_path, String file_nonce,
                                    SnapshotFormat format,
                                    String data,
                                    SystemProcedureExecutionContext context,
                                    String hostname) {
        {
            VoltTable result = constructNodeResultsTable();
            // Choose the lowest site ID on this host to do the file scan
            // All other sites should just return empty results tables.
            if (context.isLowestSiteId())
            {
                TRACE_LOG.trace("Checking feasibility of save with path and nonce: "
                                + file_path + ", " + file_nonce);
                final int numSitesSnapshotting = SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.size();
                if (numSitesSnapshotting > 0) {
                    SNAP_LOG.debug("Snapshot in progress, " +
                            numSitesSnapshotting +
                            " sites are still snapshotting");
                    result.addRow(
                                  context.getHostId(),
                                  hostname,
                                  "",
                                  "FAILURE",
                    "SNAPSHOT IN PROGRESS");
                    return new DependencyPair( DEP_saveTest, result);
                }

                for (Table table : SnapshotUtil.getTablesToSave(context.getDatabase()))
                {
                    String file_valid = "SUCCESS";
                    String err_msg = "";
                    if (format.isFileBased()) {
                        File saveFilePath =
                                SnapshotUtil.constructFileForTable(
                                                                   table,
                                                                   file_path,
                                                                   file_nonce,
                                                                   format,
                                                                   context.getHostId());
                        TRACE_LOG.trace("Host ID " + context.getHostId() +
                                        " table: " + table.getTypeName() +
                                        " to path: " + saveFilePath);
                        if (saveFilePath.exists())
                        {
                            file_valid = "FAILURE";
                            err_msg = "SAVE FILE ALREADY EXISTS: " + saveFilePath;
                        }
                        else if (!saveFilePath.getParentFile().canWrite())
                        {
                            file_valid = "FAILURE";
                            err_msg = "FILE LOCATION UNWRITABLE: " + saveFilePath;
                        }
                        else
                        {
                            try
                            {
                                /*
                                 * Sanity check that the file can be created
                                 * and then delete it so empty files aren't
                                 * orphaned if another part of the snapshot
                                 * test fails.
                                 */
                                if (saveFilePath.createNewFile()) {
                                    saveFilePath.delete();
                                }
                            }
                            catch (IOException ex)
                            {
                                file_valid = "FAILURE";
                                err_msg = "FILE CREATION OF " + saveFilePath +
                                        "RESULTED IN IOException: " + ex.getMessage();
                            }
                        }
                    }

                    result.addRow(context.getHostId(),
                                  hostname,
                                  table.getTypeName(),
                                  file_valid,
                                  err_msg);
                }
            }
            return new DependencyPair(DEP_saveTest, result);
        }
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, String command) throws Exception
    {
        final long startTime = System.currentTimeMillis();

        JSONObject jsObj = new JSONObject(command);
        final boolean block = jsObj.optBoolean("block", false);
        final String async = !block ? "Asynchronously" : "Synchronously";
        final String path = jsObj.getString("path");
        final String nonce = jsObj.getString("nonce");
        String formatStr = jsObj.optString("format", SnapshotFormat.NATIVE.toString());
        final SnapshotFormat format = SnapshotFormat.getEnumIgnoreCase(formatStr);
        final String data = jsObj.optString("data");

        JSONArray perPartitionTransactionIdsArray = jsObj.optJSONArray("perPartitionTxnIds");
        if (perPartitionTransactionIdsArray == null) {
            /*
             * Not going to make this fatal because I don't want people to
             * be blocked from getting their data out via snapshots.
             */
            SNAP_LOG.error(
                    "Failed to retrieve per partition transaction ids array from SnapshotDaemon." +
                    "This shouldn't happen and it prevents the snapshot from including transaction ids " +
                    "for partitions that are no longer active in the cluster. Those ids are necessary " +
                    "to prevent those partitions from generating duplicate unique ids when they are brought back.");
            perPartitionTransactionIdsArray = new JSONArray();
        }
        long perPartitionTxnIds[] = new long[perPartitionTransactionIdsArray.length()];
        for (int ii = 0; ii < perPartitionTxnIds.length; ii++) {
            perPartitionTxnIds[ii] = perPartitionTransactionIdsArray.getLong(ii);
        }

        if (format == SnapshotFormat.STREAM) {
            SNAP_LOG.info(async + " streaming database, ID: " + nonce + " at " + startTime);
        } else {
            SNAP_LOG.info(async + " saving database to path: " + path + ", ID: " + nonce + " at " + startTime);
        }

        ColumnInfo[] error_result_columns = new ColumnInfo[2];
        int ii = 0;
        error_result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
        error_result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
        if (format.isFileBased() && (path == null || path.equals(""))) {
            VoltTable results[] = new VoltTable[] { new VoltTable(error_result_columns) };
            results[0].addRow("FAILURE", "Provided path was null or the empty string");
            return results;
        }

        if (nonce == null || nonce.equals("")) {
            VoltTable results[] = new VoltTable[] { new VoltTable(error_result_columns) };
            results[0].addRow("FAILURE", "Provided nonce was null or the empty string");
            return results;
        }

        if (nonce.contains("-") || nonce.contains(",")) {
            VoltTable results[] = new VoltTable[] { new VoltTable(error_result_columns) };
            results[0].addRow("FAILURE", "Provided nonce " + nonce + " contains a prohibited character (- or ,)");
            return results;
        }

        // See if we think the save will succeed
        VoltTable[] results;
        results = performSaveFeasibilityWork(path, nonce, format, data);

        // Test feasibility results for fail
        while (results[0].advanceRow())
        {
            if (results[0].getString("RESULT").equals("FAILURE"))
            {
                // Something lost, bomb out and just return the whole
                // table of results to the client for analysis
                results[0].resetRowPosition();
                return results;
            }
        }

        performQuiesce();

        HashinatorSnapshotData serializationData;
        try {
            serializationData = TheHashinator.serializeConfiguredHashinator();
        }
        catch (IOException e) {
            VoltTable errorResults[] = new VoltTable[] { new VoltTable(error_result_columns) };
            errorResults[0].addRow("FAILURE", "I/O exception accessing hashinator config.");
            return errorResults;
        }

        results = performSnapshotCreationWork(path, nonce, ctx.getCurrentTxnId(), perPartitionTxnIds,
                                              (byte)(block ? 1 : 0), format, data,
                                              serializationData);
        SnapshotSaveAPI.logParticipatingHostCount(ctx.getCurrentTxnId());

        try {
            JSONStringer stringer = new JSONStringer();
            stringer.object();
            stringer.key("txnId").value(ctx.getCurrentTxnId());
            stringer.endObject();
            setAppStatusString(stringer.toString());
        } catch (Exception e) {
            SNAP_LOG.warn(e);
        }

        final long finishTime = System.currentTimeMillis();
        final long duration = finishTime - startTime;
        SNAP_LOG.info("Snapshot initiation took " + duration + " milliseconds");
        return results;
    }

    private final VoltTable[] performSaveFeasibilityWork(String filePath,
                                                         String fileNonce,
                                                         SnapshotFormat format,
                                                         String data)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution site to confirm the likely
        // success of writing tables to disk
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_saveTest;
        pfs[0].outputDepId = DEP_saveTest;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.fromArrayNoCopy(filePath, fileNonce, format.name(), data);

        // This fragment aggregates the save-to-disk sanity check results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_saveTestResults;
        pfs[1].outputDepId = DEP_saveTestResults;
        pfs[1].inputDepIds = new int[] { DEP_saveTest };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_saveTestResults);
        return results;
    }

    private final VoltTable[] performSnapshotCreationWork(String filePath,
            String fileNonce,
            long txnId,
            long perPartitionTxnIds[],
            byte block,
            SnapshotFormat format,
            String data,
            HashinatorSnapshotData hashinatorData)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution node to create the files
        // that will be written to during the snapshot
        byte[] hashinatorBytes = (hashinatorData != null ? hashinatorData.m_serData : null);
        long hashinatorVersion = (hashinatorData != null ? hashinatorData.m_version : 0);
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_createSnapshotTargets;
        pfs[0].outputDepId = DEP_createSnapshotTargets;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.fromArrayNoCopy(
                filePath, fileNonce, txnId, perPartitionTxnIds, block, format.name(), data,
                hashinatorBytes, hashinatorVersion,
                System.currentTimeMillis());

        // This fragment aggregates the results of creating those files
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_createSnapshotTargetsResults;
        pfs[1].outputDepId = DEP_createSnapshotTargetsResults;
        pfs[1].inputDepIds = new int[] { DEP_createSnapshotTargets };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_createSnapshotTargetsResults);
        return results;
    }

    private final VoltTable[] performQuiesce()
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution site flush export
        // data to disk with a sync
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_snapshotSaveQuiesce;
        pfs[0].outputDepId = DEP_snapshotSaveQuiesce;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.emptyParameterSet();

        // This fragment aggregates the quiesce results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_snapshotSaveQuiesceResults;
        pfs[1].outputDepId = DEP_snapshotSaveQuiesceResults;
        pfs[1].inputDepIds = new int[] { DEP_snapshotSaveQuiesce };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_snapshotSaveQuiesceResults);
        return results;
    }
}
