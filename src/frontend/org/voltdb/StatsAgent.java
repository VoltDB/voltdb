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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.utils.SystemStatsCollector;

/**
 * Agent responsible for collecting stats on this host.
 *
 */
public class StatsAgent {
    private final HashMap<SysProcSelector, HashMap<Integer, ArrayList<StatsSource>>> registeredStatsSources =
        new HashMap<SysProcSelector, HashMap<Integer, ArrayList<StatsSource>>>();

    private final HashSet<SysProcSelector> handledSelectors = new HashSet<SysProcSelector>();

    private static int m_hostId = -1;
    private static String m_hostName = null;

    public StatsAgent() {
        SysProcSelector selectors[] = SysProcSelector.values();
        for (int ii = 0; ii < selectors.length; ii++) {
            registeredStatsSources.put(selectors[ii], new HashMap<Integer, ArrayList<StatsSource>>());
        }
        handledSelectors.add(SysProcSelector.PROCEDURE);
    }

    public synchronized void registerStatsSource(SysProcSelector selector, int catalogId, StatsSource source) {
        assert selector != null;
        assert source != null;
        final HashMap<Integer, ArrayList<StatsSource>> catalogIdToStatsSources = registeredStatsSources.get(selector);
        assert catalogIdToStatsSources != null;
        ArrayList<StatsSource> statsSources = catalogIdToStatsSources.get(catalogId);
        if (statsSources == null) {
            statsSources = new ArrayList<StatsSource>();
            catalogIdToStatsSources.put(catalogId, statsSources);
        }
        statsSources.add(source);
    }

    public synchronized VoltTable getStats(
            final SysProcSelector selector,
            final ArrayList<Integer> catalogIds,
            final boolean interval,
            final Long now) {
        assert selector != null;
        assert catalogIds != null;
        assert catalogIds.size() > 0;
        final HashMap<Integer, ArrayList<StatsSource>> catalogIdToStatsSources = registeredStatsSources.get(selector);
        assert catalogIdToStatsSources != null;

        assert catalogIdToStatsSources.get(catalogIds.get(0)) != null;
        ArrayList<StatsSource> statsSources = catalogIdToStatsSources.get(catalogIds.get(0));
        assert statsSources != null && statsSources.size() > 0;

        /*
         * Some sources like TableStats use VoltTable to keep track of
         * statistics. We need to use the table schema the VoltTable has in this
         * case.
         */
        VoltTable.ColumnInfo columns[] = null;
        if (!statsSources.get(0).isEEStats())
            columns = statsSources.get(0).getColumnSchema().toArray(new VoltTable.ColumnInfo[0]);
        else {
            final VoltTable table = statsSources.get(0).getStatsTable();
            if (table == null)
                return null;
            columns = new VoltTable.ColumnInfo[table.getColumnCount()];
            for (int i = 0; i < columns.length; i++)
                columns[i] = new VoltTable.ColumnInfo(table.getColumnName(i),
                                                      table.getColumnType(i));
        }
        final VoltTable resultTable = new VoltTable(columns);

        for (Integer catalogId : catalogIds) {
            statsSources = catalogIdToStatsSources.get(catalogId);
            assert statsSources != null;
            for (final StatsSource ss : statsSources) {
                assert ss != null;

                /*
                 * Some sources like TableStats use VoltTable to keep track of
                 * statistics
                 */
                if (ss.isEEStats()) {
                    final VoltTable table = ss.getStatsTable();
                    // this table can be null during recovery, at least
                    if (table != null) {
                        while (table.advanceRow()) {
                            resultTable.add(table);
                        }
                        table.resetRowPosition();
                    }
                } else {
                    Object statsRows[][] = ss.getStatsRows(interval, now);
                    for (Object[] row : statsRows) {
                        resultTable.addRow(row);
                    }
                }
            }
        }
        return resultTable;
    }

    /////////////////////////////////////////////
    // STATIC STUFF FOR ROLLUP GOES HERE

    static class PartitionMemRow {
        long tupleCount = 0;
        int tupleDataMem = 0;
        int tupleAllocatedMem = 0;
        int indexMem = 0;
        int stringMem = 0;
        long pooledMem = 0;
    }
    static Map<Integer, PartitionMemRow> m_memoryStats = new TreeMap<Integer, PartitionMemRow>();

    static synchronized void eeUpdateMemStats(int siteId,
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

    public static VoltTable getEmptyNodeMemStatsTable() {
        // create a table with the right schema
        VoltTable t = new VoltTable(new ColumnInfo[] {
                new ColumnInfo("HOSTID", VoltType.INTEGER),
                new ColumnInfo("HOSTNAME", VoltType.STRING),
                new ColumnInfo("RSS", VoltType.INTEGER),
                new ColumnInfo("JAVAUSED", VoltType.INTEGER),
                new ColumnInfo("JAVAUNUSED", VoltType.INTEGER),
                new ColumnInfo("TUPLEDATA", VoltType.INTEGER),
                new ColumnInfo("TUPLEALLOCATED", VoltType.INTEGER),
                new ColumnInfo("INDEXMEMORY", VoltType.INTEGER),
                new ColumnInfo("STRINGMEMORY", VoltType.INTEGER),
                new ColumnInfo("TUPLECOUNT", VoltType.BIGINT),
                new ColumnInfo("POOLEDMEMORY", VoltType.BIGINT)
        });
        return t;
    }

    public static synchronized VoltTable getMemoryStatsTable() {
        // get the host id and hostname once
        if (m_hostId == -1) {
            m_hostId = VoltDB.instance().getHostMessenger().getHostId();
            // get the hostname, but fail gracefully
            m_hostName = "&lt;unknownhost&gt;";
            try {
                InetAddress addr = InetAddress.getLocalHost();
                m_hostName = addr.getHostName();
            }
            catch (Exception e) {
                try {
                    InetAddress addr = InetAddress.getLocalHost();
                    m_hostName = addr.getHostAddress();
                }
                catch (Exception e2) {}
            }
        }

        // create a table with the right schema
        VoltTable t = getEmptyNodeMemStatsTable();

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

        // create the row and return it
        t.addRow(m_hostId, m_hostName, rss, javaused, javaunused,
                 totals.tupleDataMem, totals.tupleAllocatedMem,
                 totals.indexMem, totals.stringMem, totals.tupleCount, totals.pooledMem / 1024);
        assert(t.getRowCount() == 1);
        return t;
    }

}
