/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
    final public long m_generation;
    final public long systemStartTimestamp;
    final public ArrayList<String> columnNames = new ArrayList<String>();
    final public ArrayList<VoltType> columnTypes = new ArrayList<VoltType>();
    final public List<Integer> columnLengths = new ArrayList<Integer>();

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
            long systemStartTimestamp,
            long generation,
            ArrayList<String> names,
            ArrayList<VoltType> types,
            List<Integer> lengths)
    {
        partitionId = p_id;
        signature = t_signature;
        tableName = t_name;
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

    @Override
    public String toString() {
        return "Generation: " + m_generation + " Table: " + tableName + " partition " + partitionId + " signature " + signature;
    }
}
