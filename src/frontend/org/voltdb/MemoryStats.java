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

package org.voltdb;

import java.util.ArrayList;
import java.util.Iterator;

import org.voltdb.VoltTable.ColumnInfo;

public class MemoryStats extends StatsSource {
    public MemoryStats(String name) {
        super(name, false);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new Iterator<Object>() {
            boolean returnRow = true;

            @Override
            public boolean hasNext() {
                return returnRow;
            }

            @Override
            public Object next() {
                if (returnRow) {
                    returnRow = false;
                    return new Object();
                } else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new VoltTable.ColumnInfo("RSS", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("JAVAUSED", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("JAVAUNUSED", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("TUPLEDATA", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("TUPLEALLOCATED", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("INDEXMEMORY", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("STRINGMEMORY", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("TUPLECOUNT", VoltType.BIGINT));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        VoltTable t = StatsAgent.getMemoryStatsTable();
        t.advanceRow();

        rowValues[columnNameToIndex.get("RSS")] = t.getLong("RSS");
        rowValues[columnNameToIndex.get("JAVAUSED")] = t.getLong("JAVAUSED");
        rowValues[columnNameToIndex.get("JAVAUNUSED")] = t.getLong("JAVAUNUSED");
        rowValues[columnNameToIndex.get("TUPLEDATA")] = t.getLong("TUPLEDATA");
        rowValues[columnNameToIndex.get("TUPLEALLOCATED")] = t.getLong("TUPLEALLOCATED");
        rowValues[columnNameToIndex.get("INDEXMEMORY")] = t.getLong("INDEXMEMORY");
        rowValues[columnNameToIndex.get("STRINGMEMORY")] = t.getLong("STRINGMEMORY");
        rowValues[columnNameToIndex.get("TUPLECOUNT")] = t.getLong("TUPLECOUNT");
        super.updateStatsRow(rowKey, rowValues);
    }

}
