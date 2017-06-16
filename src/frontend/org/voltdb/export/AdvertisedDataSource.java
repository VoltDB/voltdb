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
    final public String tableName;
    //Set to other than partition column in case of kafka.
    private String m_partitionColumnName = "";
    final public long m_generation;
    final public long systemStartTimestamp;
    final public List<String> columnNames;
    final public List<VoltType> columnTypes;
    final public List<Integer> columnLengths;
    final public ExportFormat exportFormat;

    /*
     * Enumeration defining what format the blocks of export data are in.
     * Updated for 4.4 to use smaller values for integers and a binary variable size
     * representation for decimals so that the format would be more efficient and
     * shareable with other features
     */
    public enum ExportFormat {
        ORIGINAL, FOURDOTFOUR, SEVENDOTX;
    }

    @Override
    public int hashCode() {
        return (((int)m_generation) + ((int)(m_generation >> 32))) + partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AdvertisedDataSource) {
            AdvertisedDataSource other = (AdvertisedDataSource)o;
            if (other.m_generation == m_generation &&
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
            List<String> names,
            List<VoltType> types,
            List<Integer> lengths,
            ExportFormat exportFormat)
    {
        columnNames = new ArrayList<String>();
        columnTypes = new ArrayList<VoltType>();
        columnLengths = new ArrayList<Integer>();
        partitionId = p_id;
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

    //This imethod is used where stream data is used to build ADS we dont unnecessary copy values and format is forced to be SEVENDOTX
    //Once format is removed this will be simple and can be merged with above constructor.
    public AdvertisedDataSource(int p_id, String t_signature, String t_name,
            String partitionColumnName,
            long systemStartTimestamp,
            long generation,
            List<String> names,
            List<VoltType> types,
            List<Integer> lengths)
    {
        partitionId = p_id;
        tableName = t_name;
        m_partitionColumnName = partitionColumnName;
        m_generation = generation;
        this.systemStartTimestamp = systemStartTimestamp;

        columnNames = names;
        columnTypes = types;
        columnLengths = lengths;
        exportFormat = ExportFormat.SEVENDOTX;
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
                + " partition " + partitionId
                + " partitionColumn " + m_partitionColumnName;
    }
}
