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

package org.voltdb.export;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.VoltType;

/**
 * The Export data source metadata
 */
public class AdvertisedDataSource
{
    final public int partitionId;
    final public String signature;
    final public String tableName;
    //Set to other than partition column in case of kafka.
    private String m_partitionColumnName = "";
    final public long m_generation;
    final public long systemStartTimestamp;
    final public ArrayList<String> columnNames = new ArrayList<String>();
    final public ArrayList<VoltType> columnTypes = new ArrayList<VoltType>();
    final public List<Integer> columnLengths = new ArrayList<Integer>();
    final public ExportFormat exportFormat;

    /*
     * Enumeration defining what format the blocks of export data are in.
     * Updated for 4.4 to use smaller values for integers and a binary variable size
     * representation for decimals so that the format would be more efficient and
     * shareable with other features
     */
    public enum ExportFormat {
        ORIGINAL, FOURDOTFOUR;
    }

    @Override
    public int hashCode() {
        return (((int)m_generation) + ((int)(m_generation >> 32))) + partitionId + signature.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AdvertisedDataSource) {
            AdvertisedDataSource other = (AdvertisedDataSource)o;
            if (other.m_generation == m_generation &&
                    other.signature.equals(signature) &&
                    other.partitionId == partitionId) {
                return true;
                    }
        }
        return false;
    }

    public AdvertisedDataSource(int p_id, String t_signature, String t_name,
            String partitionColumnName,
            long systemStartTimestamp,
            long generation,
            ArrayList<String> names,
            ArrayList<VoltType> types,
            List<Integer> lengths,
            ExportFormat exportFormat)
    {
        partitionId = p_id;
        signature = t_signature;
        tableName = t_name;
        m_partitionColumnName = partitionColumnName;
        m_generation = generation;
        this.systemStartTimestamp = systemStartTimestamp;

        // null checks are for happy-making test time
        if (names != null)
            columnNames.addAll(names);
        if (types != null)
            columnTypes.addAll(types);
        if (lengths != null) {
            columnLengths.addAll(lengths);
        }
        this.exportFormat = exportFormat;
    }

    public VoltType columnType(int index) {
        return columnTypes.get(index);
    }

    public String columnName(int index) {
        return columnNames.get(index);
    }

    public Integer columnLength(int index) {
        return columnLengths.get(index);
    }

    //This is for setting column other than partition column of table.
    //Kafka uses any arbitrary column for using its value for kafka key
    public void setPartitionColumnName(String partitionColumnName) {
        m_partitionColumnName = partitionColumnName;
    }

    public String getPartitionColumnName() {
        return m_partitionColumnName;
    }

    @Override
    public String toString() {
        return "Generation: " + m_generation + " Table: " + tableName
                + " partition " + partitionId + " signature " + signature
                + " partitionColumn " + m_partitionColumnName;
    }
}
