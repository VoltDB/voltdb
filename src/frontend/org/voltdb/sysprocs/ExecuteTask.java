/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.voltdb.DRConsumerDrIdTracker;
import org.voltdb.DRLogSegmentId;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TupleStreamStateInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.jni.ExecutionEngine.TaskType;
import org.voltdb.utils.VoltTableUtil;

public class ExecuteTask extends VoltSystemProcedure {

    private static final int DEP_executeTask = (int) SysProcFragmentId.PF_executeTask | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_executeTaskAggregate = (int) SysProcFragmentId.PF_executeTaskAggregate;


    static VoltTable createDRTupleStreamStateResultTable()
    {
        return new VoltTable(new VoltTable.ColumnInfo(CNAME_HOST_ID, CTYPE_ID),
                             new VoltTable.ColumnInfo(CNAME_PARTITION_ID, CTYPE_ID),
                             new VoltTable.ColumnInfo("REPLICATED", VoltType.TINYINT),
                             new VoltTable.ColumnInfo("SEQUENCE_NUMBER", VoltType.BIGINT),
                             new VoltTable.ColumnInfo("SP_UNIQUEID", VoltType.BIGINT),
                             new VoltTable.ColumnInfo("MP_UNIUQEID", VoltType.BIGINT),
                             new VoltTable.ColumnInfo("DR_VERSION", VoltType.INTEGER));
    }

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{SysProcFragmentId.PF_executeTask, SysProcFragmentId.PF_executeTaskAggregate};
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        if (fragmentId == SysProcFragmentId.PF_executeTask) {
            assert(params.toArray()[0] != null);
            byte[] payload = (byte [])params.toArray()[0];
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            int taskId = buffer.getInt();
            TaskType taskType = TaskType.values()[taskId];
            VoltTable result = null;
            switch (taskType) {
            // @VALIDATE_PARTITIONING is an existing system stored procedure, don't bother to provide another implementation here.
            case GET_DR_TUPLESTREAM_STATE:
            {
                TupleStreamStateInfo stateInfo = context.getSiteProcedureConnection().getDRTupleStreamStateInfo();
                result = createDRTupleStreamStateResultTable();
                result.addRow(context.getHostId(), context.getPartitionId(), 0,
                        stateInfo.partitionInfo.drId, stateInfo.partitionInfo.spUniqueId, stateInfo.partitionInfo.mpUniqueId,
                        stateInfo.drVersion);
                if (stateInfo.containsReplicatedStreamInfo) {
                    result.addRow(context.getHostId(), context.getPartitionId(), 1,
                            stateInfo.replicatedInfo.drId, stateInfo.replicatedInfo.spUniqueId, stateInfo.replicatedInfo.mpUniqueId,
                            stateInfo.drVersion);
                }
                break;
            }
            case SET_DR_SEQUENCE_NUMBERS:
            {
                result = new VoltTable(STATUS_SCHEMA);
                result.addRow(STATUS_OK);
                long partitionSequenceNumber = buffer.getLong();
                long mpSequenceNumber = buffer.getLong();
                context.getSiteProcedureConnection().setDRSequenceNumbers(partitionSequenceNumber, mpSequenceNumber);
                break;
            }
            case SET_DR_PROTOCOL_VERSION:
            {
                result = new VoltTable(STATUS_SCHEMA);
                result.addRow(STATUS_OK);
                int drVersion = buffer.getInt();
                int createStartStream = buffer.getInt();
                if (createStartStream > 0) {
                    long uniqueId = m_runner.getUniqueId();
                    long spHandle = m_runner.getTxnState().getNotice().getSpHandle();
                    context.getSiteProcedureConnection().setDRProtocolVersion(drVersion, spHandle, uniqueId);
                } else {
                    context.getSiteProcedureConnection().setDRProtocolVersion(drVersion);
                }
                break;
            }
            case SET_DRID_TRACKER_START:
            {
                result = new VoltTable(STATUS_SCHEMA,
                        new ColumnInfo("LOCAL_UNIQUEID", VoltType.BIGINT));
                try {
                    byte[] paramBuf = new byte[buffer.remaining()];
                    buffer.get(paramBuf);
                    ByteArrayInputStream bais = new ByteArrayInputStream(paramBuf);
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    Map<Integer, DRLogSegmentId> lastAckedIds = (Map<Integer, DRLogSegmentId>)ois.readObject();
                    for (Entry<Integer, DRLogSegmentId> e : lastAckedIds.entrySet()) {
                        if (!DRLogSegmentId.isEmptyDRId(e.getValue().drId)) {
                            int producerPartitionId = e.getKey();
                            int producerClusterId = DRLogSegmentId.getClusterIdFromDRId(e.getValue().drId);
                            DRConsumerDrIdTracker tracker =
                                    DRConsumerDrIdTracker.createPartitionTracker(e.getValue().drId, e.getValue().spUniqueId, e.getValue().mpUniqueId, producerPartitionId);
                            context.appendApplyBinaryLogTxns(producerClusterId, producerPartitionId, -1L, tracker);
                        }
                    }
                    result.addRow(STATUS_OK, m_runner.getTxnState().uniqueId);

                } catch (Exception e) {
                    e.printStackTrace();
                    result.addRow("FAILURE");
                }
                break;
            }
            case RESET_DR_APPLIED_TRACKER:
            {
                result = new VoltTable(STATUS_SCHEMA);
                result.addRow(STATUS_OK);
                context.resetDrAppliedTracker();
                break;
            }
            case SET_MERGED_DRID_TRACKER:
            {
                result = new VoltTable(STATUS_SCHEMA);
                try {
                    byte[] paramBuf = new byte[buffer.remaining()];
                    buffer.get(paramBuf);
                    ByteArrayInputStream bais = new ByteArrayInputStream(paramBuf);
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    Map<Integer, Map<Integer, DRConsumerDrIdTracker>> clusterToPartitionMap =
                            (Map<Integer, Map<Integer, DRConsumerDrIdTracker>>)ois.readObject();
                    context.recoverWithDrAppliedTrackers(clusterToPartitionMap);
                    result.addRow(STATUS_OK);
                } catch (Exception e) {
                    e.printStackTrace();
                    result.addRow("FAILURE");
                }
                break;
            }
            case INIT_DRID_TRACKER:
            {
                result = new VoltTable(STATUS_SCHEMA);
                try {
                    byte[] paramBuf = new byte[buffer.remaining()];
                    buffer.get(paramBuf);
                    ByteArrayInputStream bais = new ByteArrayInputStream(paramBuf);
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    Map<Byte, Integer> clusterIdToPartitionCountMap = (Map<Byte, Integer>)ois.readObject();
                    context.initDRAppliedTracker(clusterIdToPartitionCountMap);
                    result.addRow(STATUS_OK);
                } catch (Exception e) {
                    e.printStackTrace();
                    result.addRow("FAILURE");
                }
                break;
            }
            default:
                throw new VoltAbortException("Unable to find the task associated with the given task id");
            }
            return new DependencyPair.TableDependencyPair(DEP_executeTask, result);
        } else if (fragmentId == SysProcFragmentId.PF_executeTaskAggregate) {
            VoltTable unionTable = VoltTableUtil.unionTables(dependencies.get(DEP_executeTask));
            return new DependencyPair.TableDependencyPair(DEP_executeTaskAggregate, unionTable);
        }
        assert false;
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, byte[] params) {
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];

        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_executeTask;
        pfs[0].inputDepIds = new int[]{};
        pfs[0].outputDepId = DEP_executeTask;
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.fromArrayNoCopy(new Object[] { params });

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_executeTaskAggregate;
        pfs[1].inputDepIds = new int[]{DEP_executeTask};
        pfs[1].outputDepId = DEP_executeTaskAggregate;
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        VoltTable[] results = executeSysProcPlanFragments(pfs, DEP_executeTaskAggregate);

        return results;
    }

}
