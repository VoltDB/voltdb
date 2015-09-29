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

package org.voltdb;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.iv2.SpScheduler.DurableUniqueIdListener;
import org.voltdb.licensetool.LicenseApi;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Stub class that provides a gateway to the InvocationBufferServer when
 * DR is enabled. If no DR, then it acts as a noop stub.
 *
 */
public class PartitionDRGateway implements DurableUniqueIdListener {

    public enum DRRecordType {
        INSERT, DELETE, UPDATE, BEGIN_TXN, END_TXN, TRUNCATE_TABLE, DELETE_BY_INDEX, UPDATE_BY_INDEX;
    }

    public static enum DRRowType {
        EXISTING_ROW,
        EXPECTED_ROW,
        NEW_ROW
    }

    public static enum DRRowDecision {
        CONFLICT_KEEP_ROW,
        CONFLICT_DELETE_ROW;
    }

    // Keep sync with EE DRConflictType at types.h
    public static enum DRConflictType {
        CONFLICT_NEW_ROW_UNIQUE_CONSTRIANT_VIOLATION,
        CONFLICT_NEW_ROW_UNIQUE_CONSTRAINT_ON_PK_UPDATE,
        CONFLICT_EXPECTED_ROW_MISSING,
        CONFLICT_EXPECTED_ROW_MISSING_ON_PK_UPDATE,
        CONFLICT_EXPECTED_ROW_TIMESTAMP_MISMATCH,
        CONFLICT_EXPECTED_ROW_TIMESTAMP_AND_NEW_ROW_CONSTRAINT,
        CONFLICT_EXPECTED_ROW_MISSING_AND_NEW_ROW_CONSTRAINT,
        CONFLICT_EXPECTED_ROW_MISSING_AND_NEW_ROW_CONSTRAINT_ON_PK,
    }

    // Keep sync with EE DRConflictType at types.h
    public static enum DRResolutionType {
        CONFLICT_DO_NOTHING,         // Use existing rows for Constraint or TimeStamp; Ignore New if Missing Row
        CONFLICT_APPLY_NEW,          // Delete all existing and apply the new row
        CONFLICT_DELETE_EXISTING,    // Delete some existing rows and do not apply the new row
        CONFLICT_APPLY_GENERATED,    // Ignore the new row and use the generated instead (possibly delete existing rows)
        BREAK_REPLICATION;
    }

    public static String DR_ROW_TYPE_COLUMN_NAME = "@DR_ROW_TYPE";
    public static String DR_LOG_ACTION_COLUMN_NAME = "@DR_ACTION_TYPE";
    public static String DR_CONFLICT_COLUMN_NAME = "@DR_CONFLICT_TYPE";
    public static String DR_ROW_DECISION_COLUMN_NAME = "@DR_ROW_DECISION";
    public static String DR_CLUSTER_ID_COLUMN_NAME = "@DR_CLUSTER_ID";
    public static String DR_TIMESTAMP_COLUMN_NAME = "@DR_TIMESTAMP";

    public static ImmutableMap<Integer, PartitionDRGateway> m_partitionDRGateways = ImmutableMap.of();

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
        final VoltDBInterface vdb = VoltDB.instance();
        LicenseApi api = vdb.getLicenseApi();
        final boolean licensedToDR = api.isDrReplicationAllowed();

        // if this is a primary cluster in a DR-enabled scenario
        // try to load the real version of this class
        PartitionDRGateway pdrg = null;
        if (licensedToDR && producerGateway != null) {
            pdrg = tryToLoadProVersion();
        }
        if (pdrg == null) {
            pdrg = new PartitionDRGateway();
        }

        // init the instance and return
        try {
            pdrg.init(partitionId, producerGateway, startAction);
        } catch (IOException e) {
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
                        StartAction startAction) throws IOException {}
    public void onSuccessfulProcedureCall(long txnId, long uniqueId, int hash,
                                          StoredProcedureInvocation spi,
                                          ClientResponseImpl response) {}
    public void onSuccessfulMPCall(long spHandle, long txnId, long uniqueId, int hash,
                                   StoredProcedureInvocation spi,
                                   ClientResponseImpl response) {}
    public long onBinaryDR(int partitionId, long startSequenceNumber, long lastSequenceNumber,
            long lastSpUniqueId, long lastMpUniqueId, ByteBuffer buf) {
        final BBContainer cont = DBBPool.wrapBB(buf);
        DBBPool.registerUnsafeMemory(cont.address());
        cont.discard();
        return -1;
    }

    @Override
    public void lastUniqueIdsMadeDurable(long spUniqueId, long mpUniqueId) {}

    public int processDRConflict(int partitionId, long remoteSequenceNumber, DRConflictType drConflictType,
                                 DRRecordType action, String tableName, ByteBuffer existingTable,
                                 ByteBuffer expectedTable, ByteBuffer newTable, ByteBuffer output) {
        return 0;
    }

    public static long pushDRBuffer(
            int partitionId,
            long startSequenceNumber,
            long lastSequenceNumber,
            long lastSpUniqueId,
            long lastMpUniqueId,
            ByteBuffer buf) {
        final PartitionDRGateway pdrg = m_partitionDRGateways.get(partitionId);
        if (pdrg == null) {
            VoltDB.crashLocalVoltDB("No PRDG when there should be", true, null);
        }
        return pdrg.onBinaryDR(partitionId, startSequenceNumber, lastSequenceNumber, lastSpUniqueId, lastMpUniqueId, buf);
    }

    public void forceAllDRNodeBuffersToDisk(final boolean nofsync) {}

    public static int reportDRConflict(int partitionId, long remoteSequenceNumber, int drConflictType, int action,
                                       String tableName, ByteBuffer existingTable, ByteBuffer expectedTable,
                                       ByteBuffer newTable, ByteBuffer output) {
        final PartitionDRGateway pdrg = m_partitionDRGateways.get(partitionId);
        if (pdrg == null) {
            VoltDB.crashLocalVoltDB("No PRDG when there should be", true, null);
        }
        return pdrg.processDRConflict(partitionId, remoteSequenceNumber, DRConflictType.values()[drConflictType],
                DRRecordType.values()[action], tableName, existingTable, expectedTable, newTable, output);
    }
}
