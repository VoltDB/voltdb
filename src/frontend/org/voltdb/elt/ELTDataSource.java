/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.elt;

import org.voltdb.VoltDB;
import org.voltdb.elt.processors.RawProcessor;
import org.voltdb.messaging.MessagingException;

/**
 *  Allows an ELTDataProcessor to access underlying table queues
 */
public class ELTDataSource {

    private final String m_database;
    private final String m_tableName;
    private final int m_tableId;
    private final int m_siteId;
    private final int m_partitionId;

    /**
     * Create a new data source.
     * @param db
     * @param tableName
     * @param partitionId
     * @param siteId
     * @param tableId
     */
    public ELTDataSource(String db, String tableName, int partitionId, int siteId, int tableId) {
        m_database = db;
        m_tableName = tableName;
        m_tableId = tableId;
        m_partitionId = partitionId;
        m_siteId = siteId;
    }

    /**
     * Obtain next block of data from source
     * @return next ByteBuffer or null if source is empty.
     * @throws MessagingException
     */
    public void poll(RawProcessor.ELTInternalMessage m) throws MessagingException {
        VoltDB.instance().getHostMessenger().send(m_siteId, 0, m);
    }

    public String getDatabase() {
        return m_database;
    }

    public String getTableName() {
        return m_tableName;
    }

    public int getTableId() {
        return m_tableId;
    }

    public int getSiteId() {
        return m_siteId;
    }

    public int getPartitionId() {
        return m_partitionId;
    }
}
