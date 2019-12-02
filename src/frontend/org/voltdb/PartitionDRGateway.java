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

package org.voltdb;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.iv2.SpScheduler.DurableUniqueIdListener;
import org.voltdb.iv2.TransactionCommitInterest;
import org.voltdb.jni.ExecutionEngine.EventType;

import com.google_voltpatches.common.collect.ImmutableMap;
import org.voltdb.utils.MiscUtils;

/**
 * Stub class that provides a gateway to the DRProducer when
 * DR is enabled. If no DR, then it acts as a noop stub.
 *
 */
public class PartitionDRGateway implements DurableUniqueIdListener, TransactionCommitInterest {

    public enum DRRecordType {
        INSERT, DELETE, UPDATE, BEGIN_TXN, END_TXN, TRUNCATE_TABLE, DELETE_BY_INDEX, UPDATE_BY_INDEX, HASH_DELIMITER;
    }

    // Keep sync with EE DRTxnPartitionHashFlag at types.h
    public enum DRTxnPartitionHashFlag {
        PLACEHOLDER,
        REPLICATED,
        SINGLE,
        MULTI,
        SPECIAL
    }

    public static enum DRRowType {
        EXISTING_ROW,
        EXPECTED_ROW,
        NEW_ROW
    }

    public static enum DRConflictResolutionFlag {
        ACCEPT_CHANGE,
        CONVERGENT
    }

    // Keep sync with EE DRConflictType at types.h
    public static enum DRConflictType {
        NO_CONFLICT,
        CONSTRAINT_VIOLATION,
        EXPECTED_ROW_MISSING,
        EXPECTED_ROW_TIMESTAMP_MISMATCH
    }

    public static ImmutableMap<Integer, PartitionDRGateway> m_partitionDRGateways = ImmutableMap.of();
    public static final DRConflictManager m_conflictManager;
    static {
        if (MiscUtils.isPro()) {
            DRConflictManager tmpObj = null;
            try {
                Class<?> klass = Class.forName("org.voltdb.dr2.DRConflictManagerImpl");
                Constructor<?> constructor = klass.getConstructor();
                tmpObj = (DRConflictManager) constructor.newInstance();
            } catch (Exception e) {}
            m_conflictManager = tmpObj;
        } else {
            m_conflictManager = null;
        }
    }

    /**
     * Load the full subclass if it should, otherwise load the
     * noop stub.
     * @param partitionId partition id
     * @param overflowDir
     * @return Instance of PartitionDRGateway
     */
    public static PartitionDRGateway getInstance(int partitionId,
                                                 ProducerDRGateway producerGateway,
                                                 StartAction startAction)
    {
        // if this is a primary cluster in a DR-enabled scenario
        // try to load the real version of this class
        PartitionDRGateway pdrg = null;
        if (producerGateway != null) {
            pdrg = tryToLoadProVersion();
        }
        if (pdrg == null) {
            pdrg = new PartitionDRGateway();
        }

        // init the instance and return
        try {
            pdrg.init(partitionId, producerGateway, startAction);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), false, e);
        }

        // Regarding apparent lack of thread safety: this is called serially
        // while looping over the SPIs during database initialization
        assert !m_partitionDRGateways.containsKey(partitionId);
        ImmutableMap.Builder<Integer, PartitionDRGateway> builder = ImmutableMap.builder();
        builder.putAll(m_partitionDRGateways);
        builder.put(partitionId, pdrg);
        m_partitionDRGateways = builder.build();

        return pdrg;
    }

    private static PartitionDRGateway tryToLoadProVersion()
    {
        try {
            Class<?> pdrgiClass = null;
            pdrgiClass = Class.forName("org.voltdb.dr2.PartitionDRGatewayImpl");
            Constructor<?> constructor = pdrgiClass.getConstructor();
            Object obj = constructor.newInstance();
            return (PartitionDRGateway) obj;
        } catch (Exception e) {
        }
        return null;
    }

    // empty methods for community edition
    protected void init(int partitionId,
                        ProducerDRGateway producerGateway,
                        StartAction startAction) throws IOException, ExecutionException, InterruptedException
    {}
    public void onSuccessfulProcedureCall(StoredProcedureInvocation spi) {}
    public void onSuccessfulMPCall(StoredProcedureInvocation spi) {}
    public long onBinaryDR(long lastCommittedSpHandle, int partitionId, long startSequenceNumber, long lastSequenceNumber,
                           long lastSpUniqueId, long lastMpUniqueId, EventType eventType, ByteBuffer buf) {
        final BBContainer cont = DBBPool.wrapBB(buf);
        DBBPool.registerUnsafeMemory(cont.address());
        cont.discard();
        return -1;
    }

    @Override
    public void lastUniqueIdsMadeDurable(long spUniqueId, long mpUniqueId) {}

    public static long pushDRBuffer(
            int partitionId,
            long lastCommittedSpHandle,
            long startSequenceNumber,
            long lastSequenceNumber,
            long lastSpUniqueId,
            long lastMpUniqueId,
            int eventType,
            ByteBuffer buf) {
        final PartitionDRGateway pdrg = m_partitionDRGateways.get(partitionId);
        if (pdrg == null) {
            return -1;
        }
        return pdrg.onBinaryDR(lastCommittedSpHandle, partitionId, startSequenceNumber, lastSequenceNumber,
                lastSpUniqueId, lastMpUniqueId, EventType.values()[eventType], buf);
    }

    public void forceAllDRNodeBuffersToDisk(final boolean nofsync) {}

    public static int reportDRConflict(int partitionId, int remoteClusterId, long remoteTimestamp, String tableName, int action,
                                       int deleteConflict, ByteBuffer existingMetaTableForDelete, ByteBuffer existingTupleTableForDelete,
                                       ByteBuffer expectedMetaTableForDelete, ByteBuffer expectedTupleTableForDelete,
                                       int insertConflict, ByteBuffer existingMetaTableForInsert, ByteBuffer existingTupleTableForInsert,
                                       ByteBuffer newMetaTableForInsert, ByteBuffer newTupleTableForInsert) {
        assert m_conflictManager != null : "Community edition should not have any conflicts";
        return m_conflictManager.resolveConflict(partitionId,
                                                 remoteClusterId,
                                                 remoteTimestamp,
                                                 tableName,
                                                 DRRecordType.values()[action],
                                                 DRConflictType.values()[deleteConflict],
                                                 existingMetaTableForDelete, existingTupleTableForDelete,
                                                 expectedMetaTableForDelete, expectedTupleTableForDelete,
                                                 DRConflictType.values()[insertConflict],
                                                 existingMetaTableForInsert, existingTupleTableForInsert,
                                                 newMetaTableForInsert, newTupleTableForInsert);
    }

    @Override
    public void transactionCommitted(long spHandle) {}
}
