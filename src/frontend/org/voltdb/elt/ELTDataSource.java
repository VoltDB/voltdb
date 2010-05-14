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

import java.util.ArrayList;

import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.elt.processors.RawProcessor;
import org.voltdb.messaging.MessagingException;
import org.voltdb.utils.CatalogUtil;

/**
 *  Allows an ELTDataProcessor to access underlying table queues
 */
public class ELTDataSource {

    private final String m_database;
    private final String m_tableName;
    private final byte m_isReplicated;
    private final int m_tableId;
    private final int m_siteId;
    private final int m_partitionId;
    public final ArrayList<String> m_columnNames = new ArrayList<String>();
    public final ArrayList<Integer> m_columnTypes = new ArrayList<Integer>();

    /**
     * Create a new data source.
     * @param db
     * @param tableName
     * @param isReplicated
     * @param partitionId
     * @param siteId
     * @param tableId
     * @param catalogMap
     */
    public ELTDataSource(String db, String tableName,
                         boolean isReplicated,
                         int partitionId, int siteId, int tableId,
                         CatalogMap<Column> catalogMap)
    {
        m_database = db;
        m_tableName = tableName;
        // coerce true == 1, false == 0 for wire format
        m_isReplicated = (byte)(isReplicated ? 1 : 0);
        m_tableId = tableId;
        m_partitionId = partitionId;
        m_siteId = siteId;

        // Add the ELT meta-data columns to the schema first
        // Transaction ID
        m_columnNames.add("VOLT_TRANSACTION_ID");
        m_columnTypes.add((Integer)((int)VoltType.BIGINT.getValue()));
        // timestamp
        m_columnNames.add("VOLT_ELT_TIMESTAMP");
        m_columnTypes.add((Integer)((int)VoltType.BIGINT.getValue()));
        // sequence #
        m_columnNames.add("VOLT_ELT_SEQUENCE_NUMBER");
        m_columnTypes.add((Integer)((int)VoltType.BIGINT.getValue()));
        // partition ID
        m_columnNames.add("VOLT_PARTITION_ID");
        m_columnTypes.add((Integer)((int)VoltType.BIGINT.getValue()));
        // site ID
        m_columnNames.add("VOLT_SITE_ID");
        m_columnTypes.add((Integer)((int)VoltType.BIGINT.getValue()));
        // INSERT or DELETE (ELT OPERATION TYPE?)
        m_columnNames.add("VOLT_ELT_OPERATION");
        m_columnTypes.add((Integer)((int)VoltType.TINYINT.getValue()));

        for (Column c : CatalogUtil.getSortedCatalogItems(catalogMap, "index")) {
            m_columnNames.add(c.getName());
            m_columnTypes.add(c.getType());
        }
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

    public byte getIsReplicated() {
        return m_isReplicated;
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
