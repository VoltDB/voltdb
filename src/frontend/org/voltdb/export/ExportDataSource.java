/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb.export;

import java.io.IOException;
import java.util.ArrayList;

import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.export.processors.RawProcessor;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.MessagingException;
import org.voltdb.utils.CatalogUtil;

/**
 *  Allows an ExportDataProcessor to access underlying table queues
 */
public class ExportDataSource implements Comparable<ExportDataSource> {

    private final String m_database;
    private final String m_tableName;
    private final byte m_isReplicated;
    private final long m_tableId;
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
    public ExportDataSource(String db, String tableName,
                         boolean isReplicated,
                         int partitionId, int siteId, long tableId,
                         CatalogMap<Column> catalogMap)
    {
        m_database = db;
        m_tableName = tableName;

        /*
         * coerce true == 1, false == 0 for wire format
         */
        m_isReplicated = (byte)(isReplicated ? 1 : 0);

        /*
         * This is not the catalog relativeIndex(). This ID incorporates
         * a catalog version and a table id so that it is constant across
         * catalog updates that add or drop tables.
         */
        m_tableId = tableId;
        m_partitionId = partitionId;
        m_siteId = siteId;

        // Add the Export meta-data columns to the schema followed by the
        // catalog columns for this table.
        m_columnNames.add("VOLT_TRANSACTION_ID");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));

        m_columnNames.add("VOLT_EXPORT_TIMESTAMP");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));

        m_columnNames.add("VOLT_EXPORT_SEQUENCE_NUMBER");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));

        m_columnNames.add("VOLT_PARTITION_ID");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));

        m_columnNames.add("VOLT_SITE_ID");
        m_columnTypes.add(((int)VoltType.BIGINT.getValue()));

        m_columnNames.add("VOLT_EXPORT_OPERATION");
        m_columnTypes.add(((int)VoltType.TINYINT.getValue()));

        for (Column c : CatalogUtil.getSortedCatalogItems(catalogMap, "index")) {
            m_columnNames.add(c.getName());
            m_columnTypes.add(c.getType());
        }
    }

    /**
     * Obtain next block of data from source
     * @throws MessagingException
     */
    public void exportAction(RawProcessor.ExportInternalMessage m) throws MessagingException {
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

    public long getTableId() {
        return m_tableId;
    }

    public int getSiteId() {
        return m_siteId;
    }

    public int getPartitionId() {
        return m_partitionId;
    }

    public void writeAdvertisementTo(FastSerializer fs) throws IOException {
        fs.writeByte(getIsReplicated());
        fs.writeInt(getPartitionId());
        fs.writeLong(getTableId());
        fs.writeString(getTableName());
        fs.writeInt(m_columnNames.size());
        for (int ii=0; ii < m_columnNames.size(); ++ii) {
            fs.writeString(m_columnNames.get(ii));
            fs.writeInt(m_columnTypes.get(ii));
        }
    }

    /**
     * Compare two ExportDataSources for equivalence. This currently does not
     * compare column names, but it should once column add/drop is allowed.
     * This comparison is performed to decide if a datasource in a new catalog
     * needs to be passed to a proccessor.
     */
    @Override
    public int compareTo(ExportDataSource o) {
        int result;

        result = m_database.compareTo(o.m_database);
        if (result != 0) {
            return result;
        }

        result = m_tableName.compareTo(o.m_tableName);
        if (result != 0) {
            return result;
        }

        result = (m_siteId - o.m_siteId);
        if (result != 0) {
            return result;
        }

       result = (m_partitionId - o.m_partitionId);
       if (result != 0) {
           return result;
       }

       // does not verify replicated / unreplicated.
       // does not verify column names / schema
       return 0;
    }

    /**
     * Make sure equal objects compareTo as 0.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ExportDataSource))
            return false;

        return compareTo((ExportDataSource)o) == 0;
    }

}
