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

package org.voltdb.export;

import java.io.IOException;

import org.voltcore.logging.VoltLogger;
import org.voltdb.SimpleClientResponseAdapter;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.utils.MiscUtils;

public class MigrateRowsDeleter {

    private static final VoltLogger logger = new VoltLogger("EXPORT");

    final String m_tableName;
    final StoredProcedureInvocation m_deleterSPI = new StoredProcedureInvocation();
    final int m_batchSize;
    final int m_partitionId;
    int m_partitionKey = Integer.MIN_VALUE;

    static int getHashinatorPartitionKey(int partitionId) {
        VoltTable partitionKeys = TheHashinator.getPartitionKeys(VoltType.INTEGER);
        while (partitionKeys.advanceRow()) {
            if (partitionId == partitionKeys.getLong("PARTITION_ID")) {
                return (int)(partitionKeys.getLong("PARTITION_KEY"));
            }
        }
        return Integer.MIN_VALUE;
    }

    class DeleterCB implements SimpleClientResponseAdapter.Callback {
        final long m_deletableTxnId;
        DeleterCB(long deletableTxnId) {
            m_deletableTxnId = deletableTxnId;
        }

        @Override
        public void handleResponse(ClientResponse clientResponse) {
            byte responseStatus = clientResponse.getStatus();
            if (responseStatus == ClientResponse.TXN_MISPARTITIONED) {
                // The partition key we use to send deletes to the correct partition has changed. This does
                // not mean that the delete would fail just that the number we use to route has moved so
                // recalculate and send again
                if (logger.isTraceEnabled()) {
                    logger.trace("MigrateRowsDeleter received mispartitioned response on partition " + m_partitionId);
                }
                m_partitionKey = getHashinatorPartitionKey(m_partitionId);
                StoredProcedureInvocation retryDeleteSPI = new StoredProcedureInvocation();
                if (m_partitionId == MpInitiator.MP_INIT_PID) {
                    m_deleterSPI.setProcName("@MigrateRowsAcked_MP");
                } else {
                    m_deleterSPI.setProcName("@MigrateRowsAcked_SP");
                }
                retryDeleteSPI.setParams(m_partitionKey, m_tableName, m_deletableTxnId, m_batchSize);
                try {
                    retryDeleteSPI = MiscUtils.roundTripForCL(retryDeleteSPI);
                    ExportManager.instance().invokeMigrateRowsDelete(retryDeleteSPI, m_partitionId, new DeleterCB(m_deletableTxnId));
                }
                catch (IOException e) {}
            }
            else
            if (responseStatus != ClientResponse.SUCCESS) {
                logger.warn("Errors while deleting migrated rows. status:" + clientResponse.getStatus());
            }
        }
    };

    public MigrateRowsDeleter(String table, int partitionId, int batchSize) {
        m_tableName = table;
        m_partitionId = partitionId;
        m_batchSize = batchSize;
        if (partitionId == MpInitiator.MP_INIT_PID) {
            m_deleterSPI.setProcName("@MigrateRowsAcked_MP");
        } else {
            m_deleterSPI.setProcName("@MigrateRowsAcked_SP");
        }
        m_partitionKey = getHashinatorPartitionKey(partitionId);
        if (m_partitionKey == Integer.MIN_VALUE) {
            logger.warn(String.format("The partition key for table %s on partition %d could not be found", m_tableName, m_partitionId));
        }
    }

    public void delete(long deletableTxnId) {

        if (m_partitionKey == Integer.MIN_VALUE) {
            return;
        }
        try {
            m_deleterSPI.setParams(m_partitionKey, m_tableName, deletableTxnId, m_batchSize);
            try {
                StoredProcedureInvocation serializedSPI = MiscUtils.roundTripForCL(m_deleterSPI);
                ExportManager.instance().invokeMigrateRowsDelete(serializedSPI, m_partitionId, new DeleterCB(deletableTxnId));
            }
            catch (IOException e) {}
        } catch (Exception e) {
            logger.error("Error deleting migrated rows", e);
        } catch (Error e) {
            VoltDB.crashLocalVoltDB("Error deleting migrated rows", true, e);
        }
    }
}