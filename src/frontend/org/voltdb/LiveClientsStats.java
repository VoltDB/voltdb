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

// This StatsSource is a bit of a hackjob, in that it exists only to make StatsAgent
// happy.  The updating is never used, and the code in the Statistics sysproc
// gets the client stats from the client interfaces and constructs the results
// tables directly.  This is identical to what IOStats also does.  As
// near as I can tell, this is because both the VoltNetwork and now the ClientInterface
// have no catalog ID with which to associate these stats, so they just sort of hang out there
// oddly.
public class LiveClientsStats extends StatsSource
{
    private Map<Long, Pair<String, long[]>> m_clientStats =
        new HashMap<Long, Pair<String,long[]>>();

    public static final ColumnInfo liveClientColumnInfo[] =
        new ColumnInfo[] {new ColumnInfo("CONNECTION_ID", VoltType.BIGINT),
                          new ColumnInfo("CLIENT_HOSTNAME", VoltType.STRING),
                          new ColumnInfo("ADMIN", VoltType.TINYINT),
                          new ColumnInfo("OUTSTANDING_REQUEST_BYTES", VoltType.BIGINT),
                          new ColumnInfo("OUTSTANDING_RESPONSE_MESSAGES", VoltType.BIGINT),
                          new ColumnInfo("OUTSTANDING_TRANSACTIONS", VoltType.BIGINT)
    };

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

    public LiveClientsStats() {
        super(false);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        for (ColumnInfo column : liveClientColumnInfo)
        {
            columns.add(column);
        }
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        final Pair<String, long[]> info = m_clientStats.get(rowKey);
        final long[] counters = info.getSecond();

        rowValues[columnNameToIndex.get("CONNECTION_ID")] = rowKey;
        rowValues[columnNameToIndex.get("CLIENT_HOSTNAME")] = info.getFirst();
        rowValues[columnNameToIndex.get("ADMIN")] = counters[0];
        rowValues[columnNameToIndex.get("OUTSTANDING_REQUEST_BYTES")] = counters[1];
        rowValues[columnNameToIndex.get("OUTSTANDING_RESPONSE_MESSAGES")] = counters[2];
        rowValues[columnNameToIndex.get("OUTSTANDING_TRANSACTIONS")] = counters[3];
        super.updateStatsRow(rowKey, rowValues);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval)
    {
        m_clientStats = new HashMap<Long, Pair<String,long[]>>();
        ClientInterface ci = VoltDB.instance().getClientInterface();
        if (ci != null) {
            m_clientStats.putAll(ci.getLiveClientStats());
        }
        return new DummyIterator(m_clientStats.keySet().iterator());
    }
}
