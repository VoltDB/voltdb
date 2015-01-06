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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltcore.utils.Pair;

public class IOStats extends StatsSource {
    private Map<Long, Pair<String, long[]>> m_ioStats =
        new HashMap<Long, Pair<String,long[]>>();

    /**
     * A dummy iterator that wraps an Iterator<Long> and provides the
     * Iterator<Object>
     */
    private class DummyIterator implements Iterator<Object> {
        private final Iterator<Long> i;

        private DummyIterator(Iterator<Long> i) {
            this.i = i;
        }

        @Override
        public boolean hasNext() {
            return i.hasNext();
        }

        @Override
        public Object next() {
            return i.next();
        }

        @Override
        public void remove() {
            i.remove();
        }
    }

    public IOStats() {
        super(false);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo("CONNECTION_ID", VoltType.BIGINT));
        columns.add(new ColumnInfo("CONNECTION_HOSTNAME", VoltType.STRING));
        columns.add(new ColumnInfo("BYTES_READ", VoltType.BIGINT));
        columns.add(new ColumnInfo("MESSAGES_READ", VoltType.BIGINT));
        columns.add(new ColumnInfo("BYTES_WRITTEN", VoltType.BIGINT));
        columns.add(new ColumnInfo("MESSAGES_WRITTEN", VoltType.BIGINT));

    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        final Pair<String, long[]> info = m_ioStats.get(rowKey);
        final long[] counters = info.getSecond();

        rowValues[columnNameToIndex.get("CONNECTION_ID")] = rowKey;
        rowValues[columnNameToIndex.get("CONNECTION_HOSTNAME")] = info.getFirst();
        rowValues[columnNameToIndex.get("BYTES_READ")] = counters[0];
        rowValues[columnNameToIndex.get("MESSAGES_READ")] = counters[1];
        rowValues[columnNameToIndex.get("BYTES_WRITTEN")] = counters[2];
        rowValues[columnNameToIndex.get("MESSAGES_WRITTEN")] = counters[3];
        super.updateStatsRow(rowKey, rowValues);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        try {
            m_ioStats = VoltDB.instance().getHostMessenger().getIOStats(interval);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new DummyIterator(m_ioStats.keySet().iterator());
    }
}
