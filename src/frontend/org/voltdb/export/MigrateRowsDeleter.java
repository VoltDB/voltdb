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

package org.voltdb.export;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

/**
 * Class allowing invoking the migrating rows deletion logic.
 * <p>
 * This implementation is THREAD-SAFE.
 *
 */
public class MigrateRowsDeleter {

    private static final VoltLogger logger = new VoltLogger("EXPORT");
    final String m_tableName;
    final int m_partitionId;

    public MigrateRowsDeleter(String table, int partitionId) {
        m_tableName = table;
        m_partitionId = partitionId;
    }

    public void delete(long deletableTxnId) {
        try {
            final ProcedureCallback cb = new ProcedureCallback() {
                @Override
                public void clientCallback(ClientResponse resp) throws Exception {
                    if (resp.getStatus() != ClientResponse.SUCCESS) {
                        logger.warn(String.format("Fail to execute migrate delete on table: %s,status: %s",
                                m_tableName, resp.getStatusString()));
                    }
                }
            };
            VoltDB.getExportManager().invokeMigrateRowsDelete(m_partitionId, m_tableName, deletableTxnId, cb);
        } catch (Exception e) {
            logger.error("Error deleting migrated rows", e);
        } catch (Error e) {
            VoltDB.crashLocalVoltDB("Error deleting migrated rows", true, e);
        }
    }
}
