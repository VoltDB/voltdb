/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import org.apache.zookeeper_voltpatches.KeeperException;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.ZKUtil;
import org.voltdb.DependencyPair;
import org.voltdb.DeprecatedProcedureAPIAccess;
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
import org.voltdb.dtxn.DtxnConstants;
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

    private static final int DEP_createSnapshotTargets = (int)
        SysProcFragmentId.PF_createSnapshotTargets | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_createSnapshotTargetsResults = (int)
        SysProcFragmentId.PF_createSnapshotTargetsResults;

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
            VoltTable result = SnapshotUtil.constructNodeResultsTable();

            if (SnapshotSiteProcessor.isSnapshotInProgress()) {
                // This should never happen. The SnapshotDaemon should never call
                // @SnapshotSave unless it's certain that there is no snapshot in
                // progress.
                SNAP_LOG.error("@SnapshotSave is called while another snapshot is still in progress");
                result.addRow(context.getHostId(), hostname, null, "FAILURE", "SNAPSHOT IN PROGRESS");
                return new DependencyPair.TableDependencyPair(SnapshotSave.DEP_createSnapshotTargets, result);
            }

            // tell each site to quiesce
            context.getSiteProcedureConnection().quiesce();

            assert(params.toArray()[0] != null);
            assert(params.toArray()[1] != null);
            assert(params.toArray()[2] != null);
            assert(params.toArray()[3] != null);
            assert(params.toArray()[4] != null);
            assert(params.toArray()[5] != null);
            assert(params.toArray()[6] != null);
            assert(params.toArray()[9] != null);
            assert(params.toArray()[10] != null);
            final String file_path = (String) params.toArray()[0];
            final String pathType = (String) params.toArray()[10];
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
            HashinatorSnapshotData hashinatorData = new HashinatorSnapshotData((byte[]) params.toArray()[7], (Long) params.toArray()[8]);

            final long timestamp = (Long)params.toArray()[9];
            SnapshotSaveAPI saveAPI = new SnapshotSaveAPI();
            result = saveAPI.startSnapshotting(file_path, pathType, file_nonce,
                                                         format, block, txnId,
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
                    ((RealVoltDB)VoltDB.instance()).updateReplicaForJoin(context.getSiteId(), txnId);
                }
            }
            return new DependencyPair.TableDependencyPair(SnapshotSave.DEP_createSnapshotTargets, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_createSnapshotTargetsResults)
        {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_createSnapshotTargets));
            return new DependencyPair.TableDependencyPair(DEP_createSnapshotTargetsResults, result);
        }
        assert (false);
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, String command) throws Exception
    {
        final long startTime = System.currentTimeMillis();
        @SuppressWarnings("deprecation")
        final long txnId = DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this);

        JSONObject jsObj = new JSONObject(command);
        final boolean block = jsObj.optBoolean(SnapshotUtil.JSON_BLOCK, false);
        final String async = !block ? "Asynchronously" : "Synchronously";
        SnapshotPathType stype = SnapshotPathType.valueOf(jsObj.getString(SnapshotUtil.JSON_PATH_TYPE));
        String path = SnapshotUtil.getRealPath(stype, jsObj.getString(SnapshotUtil.JSON_PATH));
        final String nonce = jsObj.getString(SnapshotUtil.JSON_NONCE);
        String formatStr = jsObj.optString(SnapshotUtil.JSON_FORMAT, SnapshotFormat.NATIVE.toString());
        final SnapshotFormat format = SnapshotFormat.getEnumIgnoreCase(formatStr);
        final String data = jsObj.optString(SnapshotUtil.JSON_DATA);

        String truncReqId = "";
        if (data != null && !data.isEmpty()) {
            final JSONObject jsData = new JSONObject(data);
            if (jsData.has("truncReqId")) {
                truncReqId = jsData.getString("truncReqId");
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

        boolean isTruncation = (stype == SnapshotPathType.SNAP_CL && !truncReqId.isEmpty());
        // Asynchronously create the completion node
        final ZKUtil.StringCallback createCallback =
                SnapshotSaveAPI.createSnapshotCompletionNode(path, stype.toString(), nonce, txnId,
                        isTruncation, truncReqId);

        VoltTable[] results = performSnapshotCreationWork(path, stype.toString(), nonce, txnId, perPartitionTxnIds,
                                              (byte)(block ? 1 : 0), format, data,
                                              serializationData);

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
                System.currentTimeMillis(), pathType);

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
}
