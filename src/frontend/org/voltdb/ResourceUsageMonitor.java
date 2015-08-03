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

import org.voltcore.logging.VoltLogger;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.SystemStatsCollector.Datum;

/**
 * Used to periodically check if the server's resource utilization is above the configured limits
 * and pause the server.
 */
public class ResourceUsageMonitor implements Runnable
{
    public static final int DEFAULT_MONITORING_INTERVAL = 60;
    private static final VoltLogger m_logger = new VoltLogger("HOST");

    private long m_rssLimit;
    private int m_resourceCheckInterval;

    public ResourceUsageMonitor(SystemSettingsType systemSettings)
    {
        if (systemSettings == null) {
            return;
        }

        if (systemSettings.getMemorylimit() != null) {
            // configured value is in GB. Convert it to bytes
            double dblLimit = systemSettings.getMemorylimit().getSize().doubleValue()*1073741824;
            m_rssLimit = Double.valueOf(dblLimit).longValue();
        }

        if (systemSettings.getResourcecheckinterval() != null) {
            m_resourceCheckInterval = systemSettings.getResourcecheckinterval().getValue();
        } else { // Use default value, if user did not specify anything.
            m_resourceCheckInterval = DEFAULT_MONITORING_INTERVAL;
        }
    }

    public boolean hasResourceLimitsConfigured()
    {
        return (m_rssLimit > 0 && m_resourceCheckInterval > 0);
    }

    public int getResourceCheckInterval()
    {
        return m_resourceCheckInterval;
    }

    @Override
    public void run()
    {
        if (VoltDB.instance().getMode() != OperationMode.RUNNING) {
            return;
        }

        Datum datum = SystemStatsCollector.getRecentSample();
        if (datum == null) { // this will be null if stats has not run yet
            m_logger.warn("No stats are available from stats collector. Skipping resource check.");
            return;
        }

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("RSS=" + datum.rss + " Configured rss limit=" + m_rssLimit);
        }
        if (datum.rss >= m_rssLimit) {
            m_logger.warn(String.format("RSS %d is over configured limit value %d. Server will be paused.", datum.rss, m_rssLimit));
            VoltDB.instance().getClientInterface().getInternalConnectionHandler().callProcedure(0, "@Pause");
        }
    }
}
