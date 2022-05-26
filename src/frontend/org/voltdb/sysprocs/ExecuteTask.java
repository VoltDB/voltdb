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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProducerDRGateway;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TupleStreamStateInfo;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.dr2.DRIDTrackerHelper;
import org.voltdb.jni.ExecutionEngine.TaskType;
import org.voltdb.utils.VoltTableUtil;

import io.netty.buffer.ByteBuf;

// ExecuteTask is now a restartable system procedure
// make sure each sub task is either idempotent or can rollback (e.g. generateDREvent)
public class ExecuteTask extends VoltSystemProcedure
{

    static final VoltLogger log = new VoltLogger("TM");

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
        return new long[] { SysProcFragmentId.PF_executeTask, SysProcFragmentId.PF_executeTaskAggregate };
    }

    @Override
    public long[] getAllowableSysprocFragIdsInTaskLog() {
        return new long[] { SysProcFragmentId.PF_executeTask};
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        if (fragmentId == SysProcFragmentId.PF_executeTask) {
            assert(params.toArray()[0] != null);
            byte[] payload = (byte[]) params.toArray()[0];
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
                if (stateInfo != null) {
                    result.addRow(context.getHostId(), context.getPartitionId(), 0, stateInfo.partitionInfo.drId,
                            stateInfo.partitionInfo.spUniqueId, stateInfo.partitionInfo.mpUniqueId,
                            stateInfo.drVersion);
                    if (stateInfo.containsReplicatedStreamInfo) {
                        result.addRow(context.getHostId(), context.getPartitionId(), 1, stateInfo.replicatedInfo.drId,
                                stateInfo.replicatedInfo.spUniqueId, stateInfo.replicatedInfo.mpUniqueId,
                                stateInfo.drVersion);
                    }
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
                long txnId = m_runner.getTxnState().txnId;
                long uniqueId = m_runner.getUniqueId();
                long spHandle = m_runner.getTxnState().getNotice().getSpHandle();
                context.getSiteProcedureConnection().setDRProtocolVersion(drVersion, txnId, spHandle, uniqueId);
                break;
            }
            case RESET_DR_APPLIED_TRACKER:
            {
                result = new VoltTable(STATUS_SCHEMA);
                int producerClusterId = -1;
                try {
                    producerClusterId = buffer.getInt();
                    if (producerClusterId == -1) {
                        context.resetAllDrAppliedTracker();
                    } else {
                        context.resetDrAppliedTracker(producerClusterId);
                    }
                    result.addRow(STATUS_OK);
                } catch (Exception e) {
                    log.error("Failed to reset DR tracker information for " + producerClusterId, e);
                    result.addRow(STATUS_FAILURE);
                }
                break;
            }
            case SET_MERGED_DRID_TRACKER:
            {
                result = new VoltTable(STATUS_SCHEMA);
                int clusterId = -1;
                try {
                    clusterId = buffer.getInt();
                    byte[] paramBuf = new byte[buffer.remaining()];
                    buffer.get(paramBuf);
                    DRIDTrackerHelper.setDRIDTrackerFromBytes(context, clusterId, paramBuf);
                    result.addRow(STATUS_OK);
                } catch (Exception e) {
                    log.error("Failed to set DR tracker information for " + clusterId, e);
                    result.addRow(STATUS_FAILURE);
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
                    @SuppressWarnings("unchecked")
                    Map<Byte, Integer> clusterIdToPartitionCountMap = (Map<Byte, Integer>) ois.readObject();
                    context.initDRAppliedTracker(clusterIdToPartitionCountMap);
                    result.addRow(STATUS_OK);
                } catch (Exception e) {
                    log.error("Failed to initialize DR tracker information", e);
                    result.addRow(STATUS_FAILURE);
                }
                break;
            }
            case ELASTIC_CHANGE:
            {
                result = new VoltTable(STATUS_SCHEMA);
                int oldPartitionCnt = buffer.getInt();
                int newPartitionCnt = buffer.getInt();
                ProducerDRGateway producer = VoltDB.instance().getNodeDRGateway();
                if (producer==null) {
                    result.addRow(STATUS_FAILURE);
                }  else {
                    if (context.isLowestSiteId()) {
                        // update the total partition count reported in query response by DRProducer.
                        // Do this even if the Producer is disabled or there are no conversations.
                        producer.elasticChangeUpdatesPartitionCount(newPartitionCnt);
                    }
                    if (producer.isActive()) {
                        // Only generate the event if we are generating binary log buffers
                        long txnId = m_runner.getTxnState().txnId;
                        long uniqueId = m_runner.getUniqueId();
                        long spHandle = m_runner.getTxnState().getNotice().getSpHandle();
                        context.getSiteProcedureConnection().generateElasticChangeEvents(oldPartitionCnt,
                                newPartitionCnt, txnId, spHandle, uniqueId);
                    }
                    result.addRow(STATUS_OK);
                }
                break;
            }
            case JAVA_GET_DRID_TRACKER:
                Map<Integer, Map<Integer, DRSiteDrIdTracker>> drIdTrackers = context.getDrAppliedTrackers();
                Pair<Long, Long> lastConsumerUniqueIds = context.getDrLastAppliedUniqueIds();
                ByteBuf data = DRIDTrackerHelper.serializeClustersTrackers(lastConsumerUniqueIds, drIdTrackers, buffer);
                result = VoltTableUtil.splitLargeBuffer(context.getPartitionId(), data);
                break;
            default:
                throw new VoltAbortException("Unable to find the task associated with the given task id");
            }
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_executeTask, result);
        } else if (fragmentId == SysProcFragmentId.PF_executeTaskAggregate) {
            VoltTable unionTable = VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_executeTask));
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_executeTaskAggregate, unionTable);
        }
        assert false;
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, byte[] params) {
        if (log.isDebugEnabled()) {
            log.debug("Called ExecuteTask on MPI with param size of " + params.length);
        }
        if (params.length == 0) {
            // This is a way for forcing the MPI to execute a runnable without doing anything
            VoltTable result = new VoltTable(STATUS_SCHEMA);
            result.addRow(STATUS_OK);
            return new VoltTable[] { result };
        }

        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_executeTask, SysProcFragmentId.PF_executeTaskAggregate,
                params);
    }

}
