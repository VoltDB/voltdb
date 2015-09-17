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
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.compiler.deploymentfile.ResourceMonitorType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.SystemStatsCollector.Datum;

/**
 * Used to periodically check if the server's resource utilization is above the configured limits
 * and pause the server.
 */
public class ResourceUsageMonitor implements Runnable, InternalConnectionContext
{
    private static final VoltLogger m_logger = new VoltLogger("HOST");

    private long m_rssLimit;
    private int m_resourceCheckInterval;
    private DiskResourceChecker m_diskLimitConfig;

    public ResourceUsageMonitor(SystemSettingsType systemSettings, PathsType pathsConfig)
    {
        if (systemSettings == null || systemSettings.getResourcemonitor() == null) {
            return;
        }

        ResourceMonitorType config = systemSettings.getResourcemonitor();
        m_resourceCheckInterval = config.getFrequency();

        if (config.getMemorylimit() != null) {
            // configured value is in GB. Convert it to bytes
            double dblLimit = config.getMemorylimit().getSize().doubleValue()*1073741824;
            m_rssLimit = Double.valueOf(dblLimit).longValue();
        }

        m_diskLimitConfig = new DiskResourceChecker(systemSettings, pathsConfig);
    }

    public boolean hasResourceLimitsConfigured()
    {
        return ((m_rssLimit > 0 || (m_diskLimitConfig!=null && m_diskLimitConfig.hasLimitsConfigured()))
                && m_resourceCheckInterval > 0);
    }

    public int getResourceCheckInterval()
    {
        return m_resourceCheckInterval;
    }

    public void logResourceLimitConfigurationInfo()
    {
        if (hasResourceLimitsConfigured()) {
            m_logger.info("Resource limit monitoring configured to run every " + m_resourceCheckInterval + " seconds");
            if (m_rssLimit > 0) {
                m_logger.info("RSS limit: " + m_rssLimit + " bytes");
            }
            if (m_diskLimitConfig!=null) {
                m_diskLimitConfig.logConfiguredLimits();
            }
        } else {
            m_logger.info("No resource usage limit monitoring configured");
        }
    }

    @Override
    public void run()
    {
        if (VoltDB.instance().getMode() != OperationMode.RUNNING) {
            return;
        }

        if (isOverMemoryLimit() || m_diskLimitConfig.isOverLimitConfiguration()) {
            m_logger.warn("Pausing the server");
            VoltDB.instance().getClientInterface().getInternalConnectionHandler().callProcedure(this, 0, "@Pause");
        }
    }

    private boolean isOverMemoryLimit()
    {
        if (m_rssLimit<=0) {
            return false;
        }

        Datum datum = SystemStatsCollector.getRecentSample();
        if (datum == null) { // this will be null if stats has not run yet
            m_logger.warn("No stats are available from stats collector. Skipping resource check.");
            return false;
        }

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("RSS=" + datum.rss + " Configured rss limit=" + m_rssLimit);
        }
        if (datum.rss >= m_rssLimit) {
            m_logger.warn(String.format("RSS %d is over configured limit value %d.", datum.rss, m_rssLimit));
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String getName()
    {
        return "ResourceUsageMonitor";
    }

    @Override
    public void setBackPressure(boolean hasBackPressure)
    {
        // nothing to do here.
    }
}
