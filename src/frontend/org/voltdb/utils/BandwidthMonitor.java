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

package org.voltdb.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.voltcore.logging.VoltLogger;

public class BandwidthMonitor {

    protected class Stats {
        public long totalBytesSent = 0;
        public long totalBytesReceived = 0;
        public long windowBytesSent = 0;
        public long windowBytesReceived = 0;
    }

    protected Stats m_globalStats = new Stats();
    protected Map<String, Stats> m_statsByHost = new HashMap<String, Stats>();

    protected final long m_startTS;
    protected long m_windowStartTS = 0;
    protected final long m_windowSizeInMS;
    private final VoltLogger m_logger;

    public BandwidthMonitor(int throughputDisplayPeriod, VoltLogger log) {
        // set up current window
        m_startTS = m_windowStartTS = System.currentTimeMillis();
        m_logger = log;
        m_windowSizeInMS = throughputDisplayPeriod * 1000;
    }

    public void logBytesTransfered(String hostName, long byteCountSent, long byteCountReceived) {
        long now = System.currentTimeMillis();
        Stats hostStats = m_statsByHost.get(hostName);
        if (hostStats == null) {
            hostStats = new Stats();
            m_statsByHost.put(hostName, hostStats);
        }

        // check if we need a new window
        if ((now - m_windowStartTS) >= m_windowSizeInMS) {
            logStatistics(now, true, m_logger);

            // reset all the windows for all the hosts
            for (Entry<String, Stats> e : m_statsByHost.entrySet()) {
                e.getValue().windowBytesReceived = 0;
                e.getValue().windowBytesSent = 0;
            }
            // reset the global windows
            m_globalStats.windowBytesReceived = 0;
            m_globalStats.windowBytesSent = 0;

            // set a new window start
            m_windowStartTS += m_windowSizeInMS;
        }

        // update stats for this host
        hostStats.totalBytesReceived += byteCountReceived;
        hostStats.totalBytesSent += byteCountSent;
        hostStats.windowBytesReceived += byteCountReceived;
        hostStats.windowBytesSent += byteCountSent;

        // update global stats
        m_globalStats.totalBytesReceived += byteCountReceived;
        m_globalStats.totalBytesSent += byteCountSent;
        m_globalStats.windowBytesReceived += byteCountReceived;
        m_globalStats.windowBytesSent += byteCountSent;
    }

    public void removeHost(String hostName) {
        m_statsByHost.remove(hostName);
    }

    private void logStatsLine(String formatStr, String hostname, long windowSize, long sent, long received) {
        m_logger.info(String.format(formatStr,
                windowSize / 1000.0,
                hostname,
                received / (windowSize / 1000.0) / (1024.0 * 1024.0),
                sent / (windowSize / 1000.0) / (1024.0 * 1024.0)));
    }

    public void logStatistics(long currentTS, boolean includeWindow, VoltLogger log) {
        // log last 10s is requested
        if (includeWindow) {
            logStatsLine("In the previous %.0f s: %-15s rate was %.4f MB/s sent and %.4f MB/s received",
                    "GLOBAL", m_windowSizeInMS, m_globalStats.windowBytesSent, m_globalStats.windowBytesReceived);
            for (Entry<String, Stats> e : m_statsByHost.entrySet()) {
                logStatsLine("In the previous %.0f s: %-15s rate was %.4f MB/s sent and %.4f MB/s received",
                        e.getKey(), m_windowSizeInMS, e.getValue().windowBytesSent, e.getValue().windowBytesReceived);
            }
        }
        // log stats since the beginning
        logStatsLine("Since startup (%.0f s): %-15s rate was %.4f MB/s sent and %.4f MB/s received",
                "GLOBAL", currentTS - m_startTS, m_globalStats.totalBytesSent, m_globalStats.totalBytesReceived);
        for (Entry<String, Stats> e : m_statsByHost.entrySet()) {
            logStatsLine("Since startup (%.0f s): %-15s rate was %.4f MB/s sent and %.4f MB/s received",
                    e.getKey(), currentTS - m_startTS, e.getValue().totalBytesSent, e.getValue().totalBytesReceived);
        }
    }
}
