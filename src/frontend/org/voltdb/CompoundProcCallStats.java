/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

/**
 * Statistics specific to compound procedures.
 * This class is instanced once per compound procedure,
 * based on the simple name of the procedure.
 */
public class CompoundProcCallStats extends StatsSource {

    private final String procName;
    private final Map<String,CallInfo> calledProcs;

    // Data collection, called from CompoundProcedureRunner

    private static class CallInfo {
        long calls;
    }

    public CompoundProcCallStats(String procName) {
        super(false);
        this.procName = procName;
        this.calledProcs = new HashMap<>();
    }

    public synchronized void trackCallTo(String procName) {
        CallInfo ci = calledProcs.computeIfAbsent(procName, k -> new CallInfo());
        ci.calls++;
    }

    // Statisics reporting, called from StatsAgent

    public enum CompoundProcColumn {
        PROCEDURE        (VoltType.STRING),
        CALLED_PROCEDURE (VoltType.STRING),
        INVOCATIONS      (VoltType.BIGINT);
        final VoltType m_type; // required by StatsSource
        CompoundProcColumn(VoltType type) { m_type = type; }
    }

    private static class RowInfo implements Comparable<RowInfo> {
        String name;
        long calls;
        RowInfo(String n, long c) { name = n; calls = c; }
        public int compareTo(RowInfo r) { return name.compareTo(r.name); }
    }

    @Override
    protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
        super.populateColumnSchema(columns, CompoundProcColumn.class);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        ArrayList<RowInfo> temp = new ArrayList<>();
        synchronized (this) { // make copy for thread safety
            for (Map.Entry<String,CallInfo> ent : calledProcs.entrySet()) {
                temp.add(new RowInfo(ent.getKey(), ent.getValue().calls));
            }
        }
        Collections.sort(temp);
        return new Iterator<Object>() {
            private Iterator<RowInfo> it = temp.iterator();
            public boolean hasNext() { return it.hasNext(); }
            public Object next() { return it.next(); }
        };
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object rowValues[]) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        rowValues[offset + CompoundProcColumn.PROCEDURE.ordinal()] = procName;
        RowInfo ri = (RowInfo)rowKey;
        rowValues[offset + CompoundProcColumn.CALLED_PROCEDURE.ordinal()] = ri.name;
        rowValues[offset + CompoundProcColumn.INVOCATIONS.ordinal()] = ri.calls;
        return offset + CompoundProcColumn.values().length;
    }

    @Override
    public String toString() {
        return procName;
    }

    // This kludgery ensures we always have at least one stats
    // source registered. The dummy entry automatically produces
    // no statistics, since it has not called any transactions.

    private static CompoundProcCallStats dummyStats;

    static void initStats(StatsAgent sa) {
        synchronized (CompoundProcCallStats.class) {
            if (dummyStats == null) {
                dummyStats = new CompoundProcCallStats("<<dummy>>");
            }
        }
        sa.registerStatsSource(StatsSelector.COMPOUNDPROCCALLS, -1, dummyStats);
    }
}
