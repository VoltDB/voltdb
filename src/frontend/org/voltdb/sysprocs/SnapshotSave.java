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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.ZKUtil;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.RealVoltDB;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSaveAPI;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.client.ClientResponse;
import org.voltdb.exceptions.SpecifiedException;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.sysprocs.saverestore.HashinatorSnapshotData;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.VoltTableUtil;

import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.primitives.Longs;

public class SnapshotSave extends VoltSystemProcedure
{
    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    @Override
    public long[] getPlanFragmentIds()
    {
        return new long[]{
            SysProcFragmentId.PF_createSnapshotTargets,
            SysProcFragmentId.PF_createSnapshotTargetsResults
        };
    }

    @Override
    public DependencyPair
    executePlanFragment(Map<Integer, List<VoltTable>> dependencies,
                        long fragmentId,
                        ParameterSet params,
                        SystemProcedureExecutionContext context)
    {
        String hostname = CoreUtils.getHostnameOrAddress();
        if (fragmentId == SysProcFragmentId.PF_createSnapshotTargets)
        {
            final int numLocalSites = context.getLocalActiveSitesCount();
            if (numLocalSites == 0) {
                throw new SpecifiedException(ClientResponse.GRACEFUL_FAILURE,
                        String.format("All sites on host %d have been de-commissioned.", context.getHostId()));
            }
            // Those plan fragments are created in performSnapshotCreationWork()
            VoltTable result = SnapshotUtil.constructNodeResultsTable();

            if (SnapshotSiteProcessor.isSnapshotInProgress()) {
                // This should never happen. The SnapshotDaemon should never call
                // @SnapshotSave unless it's certain that there is no snapshot in
                // progress.
                SNAP_LOG.error("@SnapshotSave is called while another snapshot is still in progress");
                result.addRow(context.getHostId(), hostname, null, "FAILURE", "SNAPSHOT IN PROGRESS");
                return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_createSnapshotTargets, result);
            }

            // Tell each site to quiesce - bring the Export and DR system to a steady state with
            // no pending committed data. It's asynchronous, SnapshotSave sysproc doesn't wait
            // for Export and DR system to have *seen* the pending committed data.
            // NativeSnapshotWritePlan.createDeferredSetup() will force the wait then do the fsync
            context.getSiteProcedureConnection().quiesce();

            Object[] paramsArray = params.toArray();
            assert paramsArray.length == 10;
            assert (paramsArray[0] != null);
            assert (paramsArray[1] != null);
            assert (paramsArray[2] != null);
            assert (paramsArray[3] != null);
            assert (paramsArray[4] != null);
            assert (paramsArray[5] != null);
            assert (paramsArray[8] != null);
            assert (paramsArray[9] != null);
            final String file_path = (String) paramsArray[0];
            final String pathType = (String) paramsArray[9];
            final String file_nonce = (String) paramsArray[1];
            long perPartitionTxnIds[] = (long[]) paramsArray[2];
            byte block = (Byte) paramsArray[3];
            SnapshotFormat format =
                    SnapshotFormat.getEnumIgnoreCase((String) paramsArray[4]);

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

            String data = (String) paramsArray[5];
            HashinatorSnapshotData hashinatorData = new HashinatorSnapshotData((byte[]) paramsArray[6],
                    (Long) paramsArray[7]);

            final long timestamp = (Long) paramsArray[8];
            SnapshotSaveAPI saveAPI = new SnapshotSaveAPI();
            result = saveAPI.startSnapshotting(file_path, pathType, file_nonce,
                                               format, block, m_runner.getTxnState().txnId,
                                               context.getSpHandleForSnapshotDigest(),
                                               Longs.toArray(perPartitionTransactionIdsToKeep),
                                               data, context, hostname, hashinatorData, timestamp);

            // The MPI uses the result table to figure out which nodes are participating in the snapshot.
            // If this node is not doing useful work for the stream snapshot, return a single line
            // with hostID and success so that the MPI has the correct participant count.
            if (result.getRowCount() == 0) {
                result.addRow(context.getHostId(), hostname, "", "SUCCESS", "");
            }
            if (format == SnapshotFormat.STREAM) {
                result.resetRowPosition();
                result.advanceRow();
                String success = result.getString("RESULT");
                if (success.equals("SUCCESS")) {
                    ((RealVoltDB)VoltDB.instance()).updateReplicaForJoin(context.getSiteId(), m_runner.getTxnState());
                }
            }
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_createSnapshotTargets, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_createSnapshotTargetsResults)
        {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_createSnapshotTargets));
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_createSnapshotTargetsResults, result);
        }
        assert (false);
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, String command) throws Exception
    {
        // TRAIL [SnapSave:1] 1 [MPI] Check parameters and perform SnapshotCreationWork.
        final long startTime = System.currentTimeMillis();
        final long txnId = m_runner.getTxnState().txnId;

        JSONObject jsObj = new JSONObject(command);
        final boolean block = jsObj.optBoolean(SnapshotUtil.JSON_BLOCK, false);
        final String async = !block ? "Asynchronously" : "Synchronously";
        SnapshotPathType stype = SnapshotPathType.valueOf(jsObj.getString(SnapshotUtil.JSON_PATH_TYPE));
        String path = SnapshotUtil.getRealPath(stype, jsObj.getString(SnapshotUtil.JSON_PATH));
        final String nonce = jsObj.getString(SnapshotUtil.JSON_NONCE);
        String formatStr = jsObj.optString(SnapshotUtil.JSON_FORMAT, SnapshotFormat.NATIVE.toString());
        final SnapshotFormat format = SnapshotFormat.getEnumIgnoreCase(formatStr);
        final String data = jsObj.optString(SnapshotUtil.JSON_DATA);
        boolean terminus = false;
        if (jsObj.has(SnapshotUtil.JSON_TERMINUS)) {
            terminus = jsObj.getLong(SnapshotUtil.JSON_TERMINUS) != 0;
        }

        String truncReqId = "";
        if (data != null && !data.isEmpty()) {
            final JSONObject jsData = new JSONObject(data);
            if (jsData.has(SnapshotUtil.JSON_TRUNCATION_REQUEST_ID)) {
                truncReqId = jsData.getString(SnapshotUtil.JSON_TRUNCATION_REQUEST_ID);
            }
        }

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

        ColumnInfo[] error_result_columns = new ColumnInfo[] {
                new ColumnInfo("RESULT", VoltType.STRING),
                new ColumnInfo("ERR_MSG", VoltType.STRING)
        };

        if (SnapshotSiteProcessor.isSnapshotInProgress()) {
            VoltTable results[] = new VoltTable[] { new VoltTable(error_result_columns) };
            results[0].addRow("FAILURE", "SNAPSHOT IN PROGRESS");
            return results;
        }

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

        HashinatorSnapshotData serializationData = null;
        try {
            serializationData = TheHashinator.serializeConfiguredHashinator();
        }
        catch (IOException e) {
            VoltTable errorResults[] = new VoltTable[] { new VoltTable(error_result_columns) };
            errorResults[0].addRow("FAILURE", "I/O exception accessing hashinator config.");
            return errorResults;
        }

        final ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        VoltTable[] results;
        final ZKUtil.StringCallback createCallback;
        try {
            // ensure the snapshot barrier is not blocked by replica decommissioning
            String errorMessage = VoltZK.createActionBlocker(zk, VoltZK.snapshotSetupInProgress, CreateMode.EPHEMERAL,
                    SNAP_LOG, "Set up snapshot");
            if (errorMessage != null) {
                results = new VoltTable[] { new VoltTable(error_result_columns) };
                results[0].addRow("FAILURE", errorMessage);
                return results;
            }

            boolean isTruncation = (stype == SnapshotPathType.SNAP_CL && !truncReqId.isEmpty());
            // Asynchronously create the completion node
            createCallback = SnapshotSaveAPI.createSnapshotCompletionNode(path, stype.toString(), nonce, txnId,
                    isTruncation, terminus, truncReqId);

            // For snapshot targets creation, see executePlanFragment() in this file.
            results = performSnapshotCreationWork(path, stype.toString(), nonce, perPartitionTxnIds,
                    (byte) (block ? 1 : 0), format, data, serializationData);
        } finally {
            VoltZK.removeActionBlocker(zk, VoltZK.snapshotSetupInProgress, SNAP_LOG);
        }

        Set<Integer> participantHostIds = Sets.newHashSet();
        for (VoltTable result : results) {
            result.resetRowPosition();
            while (result.advanceRow()) {
                participantHostIds.add((int) result.getLong(VoltSystemProcedure.CNAME_HOST_ID));
            }
            result.resetRowPosition();
        }

        asyncLogHostCountToZK(txnId, createCallback, participantHostIds.size());

        try {
            JSONStringer stringer = new JSONStringer();
            stringer.object();
            stringer.key("txnId").value(txnId);
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

    private static void asyncLogHostCountToZK(final long txnId,
                                              final ZKUtil.StringCallback createCallback,
                                              final int participantCount)
    {
        VoltDB.instance().submitSnapshotIOWork(new Callable<Object>() {
            @Override
            public Object call() throws Exception
            {

                try {
                    createCallback.get();
                } catch (KeeperException.NodeExistsException e) {
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Unexpected exception logging snapshot completion to ZK", true, e);
                }

                SnapshotSaveAPI.logParticipatingHostCount(txnId, participantCount);

                return null;
            }
        });
    }

    private final VoltTable[] performSnapshotCreationWork(String filePath, String pathType,
            String fileNonce,
            long perPartitionTxnIds[],
            byte block,
            SnapshotFormat format,
            String data,
            HashinatorSnapshotData hashinatorData)
    {
        // TRAIL [SnapSave:2] 2 [MPI] Build & send create snapshot targets requests to all SP sites.
        // This fragment causes each execution node to create the files
        // that will be written to during the snapshot
        byte[] hashinatorBytes = (hashinatorData != null ? hashinatorData.m_serData : null);
        long hashinatorVersion = (hashinatorData != null ? hashinatorData.m_version : 0);
        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_createSnapshotTargets,  SysProcFragmentId.PF_createSnapshotTargetsResults,
                filePath, fileNonce, perPartitionTxnIds, block, format.name(), data,
                hashinatorBytes, hashinatorVersion, System.currentTimeMillis(), pathType);
    }
}
