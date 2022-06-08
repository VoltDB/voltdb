/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
package org.voltdb.tasks.clockskew;

import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.net.HostAndPort;
import org.voltcore.logging.VoltLogger;
import org.voltdb.InternalConnectionStatsCollector;
import org.voltdb.StatsSource;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static org.voltdb.ClockSkewCollectorAgent.HOST_ID;
import static org.voltdb.ClockSkewCollectorAgent.NODE_CURRENT_TIME;

public class ClockSkewStats extends StatsSource implements InternalConnectionStatsCollector {
    private final Duration WARN_THRESHOLD = Duration.ofMillis(100);
    private final Duration CRITICAL_THRESHOLD = Duration.ofMillis(200);

    private final Clock clock;
    private final VoltLogger hostLog;

    final Map<Integer, Long> cachedSkews = new ConcurrentHashMap<>();

    public enum Skew {
        SKEW_TIME       (VoltType.INTEGER),
        REMOTE_HOST_ID  (VoltType.INTEGER),
        REMOTE_HOST_NAME  (VoltType.STRING);

        public final VoltType m_type;
        Skew(VoltType type) { m_type = type; }
    }

    /**
     * Initialize this source of statistical information with the specified
     * name. Populate the column schema by calling populateColumnSchema on the
     * derived class and use it to populate the columnNameToIndex map.
     *
     * This source does not represent statistics from EE
     * @param clock to get time from
     * @param hostLog logger for a node
     */
    public ClockSkewStats(Clock clock, VoltDBInterface voltDb, VoltLogger hostLog) {
        super(false, voltDb);
        this.clock = clock;
        this.hostLog = hostLog;
    }

    public void clear() {
        cachedSkews.clear();
    }

    @Override
    public void reportCompletion(String callerName, String procName, ClientResponse response) {
        final long localTime = clock.millis();

        int localHostId = voltDb.getMyHostId();

        if (response == null || response.getResults().length == 0) {
            return;
        }

        VoltTable result = response.getResults()[0];
        while (result.advanceRow()) {
            int remoteHostId = (int) result.get(HOST_ID, VoltType.INTEGER);
            if (localHostId != remoteHostId) {
                long remoteTime = (long) result.get(NODE_CURRENT_TIME, VoltType.BIGINT);
                long skew = Math.abs(localTime - remoteTime);

                traceResponse(localHostId, remoteHostId, skew);
                cachedSkews.put(remoteHostId, skew);
            }
        }
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return ImmutableSet.<Object>copyOf(cachedSkews.keySet()).iterator();
    }

    @Override
    protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
        super.populateColumnSchema(columns, Skew.class);
    }

    @Override
    protected synchronized int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);

        Long skew = cachedSkews.get(rowKey);
        String hostDisplayName = voltDb.getHostMessenger().getHostDisplayNameForHostId((Integer) rowKey);
        rowValues[offset + Skew.SKEW_TIME.ordinal()] = skew;
        rowValues[offset + Skew.REMOTE_HOST_ID.ordinal()] = rowKey;
        rowValues[offset + Skew.REMOTE_HOST_NAME.ordinal()] = hostDisplayName;
        return offset + Skew.values().length;
    }

    private void traceResponse(int localHostId, int remoteHostId, long skew) {
        if (hostLog.isTraceEnabled()) {
            String localHostName = voltDb.getHostMessenger().getHostname();
            String hostDisplayName = voltDb.getHostMessenger().getHostDisplayNameForHostId(remoteHostId);
            hostLog.trace(format("Collecting reply in node %d(%s) got skew result from %d(%s) equal to %d",
                                    localHostId, localHostName,
                                    remoteHostId, hostDisplayName,
                                    skew));
        }

        if (WARN_THRESHOLD.toMillis() <= skew) {
            hostLog.info(format("Clock skew between node %d and %d is %dms. Clock skew is collected on a best-effort basis - a large value may include anomalies, such as JVM GC or network latency.",
                    localHostId, remoteHostId, skew));
        }
    }
}
