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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.SystemStatsCollector;

public class MemoryStats extends StatsSource {
    static class PartitionMemRow {
        long tupleCount = 0;
        int tupleDataMem = 0;
        int tupleAllocatedMem = 0;
        int indexMem = 0;
        int stringMem = 0;
        long pooledMem = 0;
    }
    Map<Long, PartitionMemRow> m_memoryStats = new TreeMap<Long, PartitionMemRow>();

    public MemoryStats() {
        super(false);
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
        columns.add(new VoltTable.ColumnInfo("POOLEDMEMORY", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("PHYSICALMEMORY", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("JAVAMAXHEAP", VoltType.INTEGER));
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        // sum up all of the site statistics
        PartitionMemRow totals = new PartitionMemRow();
        for (PartitionMemRow pmr : m_memoryStats.values()) {
            totals.tupleCount += pmr.tupleCount;
            totals.tupleDataMem += pmr.tupleDataMem;
            totals.tupleAllocatedMem += pmr.tupleAllocatedMem;
            totals.indexMem += pmr.indexMem;
            totals.stringMem += pmr.stringMem;
            totals.pooledMem += pmr.pooledMem;
        }

        // get system statistics
        int rss = 0; int javaused = 0; int javaunused = 0;
        SystemStatsCollector.Datum d = SystemStatsCollector.getRecentSample();
        if (d != null) {
            rss = (int) (d.rss / 1024);
            double javausedFloat = d.javausedheapmem + d.javausedsysmem;
            javaused = (int) (javausedFloat / 1024);
            javaunused = (int) ((d.javatotalheapmem + d.javatotalsysmem - javausedFloat) / 1024);
        }

        rowValues[columnNameToIndex.get("RSS")] = rss;
        rowValues[columnNameToIndex.get("JAVAUSED")] = javaused;
        rowValues[columnNameToIndex.get("JAVAUNUSED")] = javaunused;
        rowValues[columnNameToIndex.get("TUPLEDATA")] = totals.tupleDataMem;
        rowValues[columnNameToIndex.get("TUPLEALLOCATED")] = totals.tupleAllocatedMem;
        rowValues[columnNameToIndex.get("INDEXMEMORY")] = totals.indexMem;
        rowValues[columnNameToIndex.get("STRINGMEMORY")] = totals.stringMem;
        rowValues[columnNameToIndex.get("TUPLECOUNT")] = totals.tupleCount;
        rowValues[columnNameToIndex.get("POOLEDMEMORY")] = totals.pooledMem / 1024;
        //in kb to make math simpler with other mem values.
        rowValues[columnNameToIndex.get("PHYSICALMEMORY")] = PlatformProperties.getPlatformProperties().ramInMegabytes * 1024;
        rowValues[columnNameToIndex.get("JAVAMAXHEAP")] = Runtime.getRuntime().maxMemory() / 1024;
        super.updateStatsRow(rowKey, rowValues);
    }

    public synchronized void eeUpdateMemStats(long siteId,
                                              long tupleCount,
                                              int tupleDataMem,
                                              int tupleAllocatedMem,
                                              int indexMem,
                                              int stringMem,
                                              long pooledMemory) {
        PartitionMemRow pmr = new PartitionMemRow();
        pmr.tupleCount = tupleCount;
        pmr.tupleDataMem = tupleDataMem;
        pmr.tupleAllocatedMem = tupleAllocatedMem;
        pmr.indexMem = indexMem;
        pmr.stringMem = stringMem;
        pmr.pooledMem = pooledMemory;
        m_memoryStats.put(siteId, pmr);
    }
}
