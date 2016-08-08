/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import org.voltcore.utils.CoreUtils;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.deploymentfile.ResourceMonitorType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.SystemStatsCollector.Datum;

/**
 * Used to periodically check if the server's resource utilization is above the configured limits
 * and pause the server.
 */
public class ResourceUsageMonitor implements Runnable
{
    private static final VoltLogger m_logger = new VoltLogger("HOST");

    private String m_rssLimitStr;
    private long m_rssLimit;
    private int m_resourceCheckInterval;
    private DiskResourceChecker m_diskLimitConfig;

    public ResourceUsageMonitor(SystemSettingsType systemSettings)
    {
        if (systemSettings == null || systemSettings.getResourcemonitor() == null) {
            return;
        }

        ResourceMonitorType config = systemSettings.getResourcemonitor();
        m_resourceCheckInterval = config.getFrequency();

        if (config.getMemorylimit() != null) {
            m_rssLimitStr = config.getMemorylimit().getSize().trim();
            // configured value is in GB. Convert it to bytes
            double dblLimit = getMemoryLimitSize(m_rssLimitStr);
            m_rssLimit = Double.valueOf(dblLimit).longValue();
        }

        m_diskLimitConfig = new DiskResourceChecker(systemSettings);
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
                m_logger.info("RSS limit: "  + getRssLimitLogString());
            }
            if (m_diskLimitConfig!=null) {
                m_diskLimitConfig.logConfiguredLimits();
            }
        } else {
            m_logger.info("No resource usage limit monitoring configured");
        }
    }

    private String getRssLimitLogString()
    {
        String rssWithUnit = getValueWithUnit(m_rssLimit);
        return (m_rssLimitStr.endsWith("%") ?
                m_rssLimitStr + " (" +  rssWithUnit + ")" : rssWithUnit);
    }

    @Override
    public void run()
    {
        if (getClusterOperationMode() != OperationMode.RUNNING) {
            return;
        }

        if (isOverMemoryLimit() || m_diskLimitConfig.isOverLimitConfiguration()) {
            SyncCallback cb = new SyncCallback();
            if (getConnectionHadler().callProcedure(getInternalUser(), true, BatchTimeoutOverrideType.NO_TIMEOUT, cb, "@Pause")) {
                try {
                    cb.waitForResponse();
                } catch (InterruptedException e) {
                    m_logger.error("Interrupted while pausing cluster for resource overusage", e);
                }
                ClientResponseImpl r = ClientResponseImpl.class.cast(cb.getResponse());
                if (r.getStatus() != ClientResponse.SUCCESS) {
                    m_logger.error("Unable to pause cluster for resource overusage: " + r.getStatusString());
                }
            } else {
                m_logger.error("Unable to pause cluster for resource overusage: failed to invoke @Pause");
            }
        }
    }

    private OperationMode getClusterOperationMode()
    {
        return VoltDB.instance().getMode();
    }

    private InternalConnectionHandler getConnectionHadler()
    {
        return VoltDB.instance().getClientInterface().getInternalConnectionHandler();
    }

    private AuthUser getInternalUser()
    {
        return VoltDB.instance().getCatalogContext().authSystem.getInternalAdminUser();
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
            m_logger.error(String.format(
                    "Resource limit exceeded. RSS limit %s on %s. Setting database to read-only. " +
                    "Use \"voltadmin resume\" command once resource constraint is corrected.",
                    getRssLimitLogString(), CoreUtils.getHostnameOrAddress()));
            m_logger.error(String.format("Resource limit exceeded. Current RSS size %s.", getValueWithUnit(datum.rss)));
            return true;
        } else {
            return false;
        }
    }

    public static String getValueWithUnit(long value)
    {
        if (value >= 1073741824L) {
            return String.format("%.2f GB", (value/1073741824.0));
        } else if (value >= 1048576) {
            return String.format("%.2f MB", (value/1048576.0));
        } else {
            return value + " bytes";
        }
    }

    // package-private for junit
    double getMemoryLimitSize(String sizeStr)
    {
        if (sizeStr==null || sizeStr.length()==0) {
            return 0;
        }

        try {
            if (sizeStr.charAt(sizeStr.length()-1)=='%') { // size as a percentage of total available memory
                int perc = Integer.parseInt(sizeStr.substring(0, sizeStr.length()-1));
                if (perc<0 || perc > 99) {
                    throw new IllegalArgumentException("Invalid memory limit percentage: " + sizeStr);
                }
                return PlatformProperties.getPlatformProperties().ramInMegabytes*1048576L*perc/100.0;
            } else { // size in GB
                double size = Double.parseDouble(sizeStr)*1073741824L;
                if (size<0) {
                    throw new IllegalArgumentException("Invalid memory limit value: " + sizeStr);
                }
                return size;
            }
        } catch(NumberFormatException e) {
            throw new IllegalArgumentException("Invalid memory limit value " + sizeStr +
                    ". Memory limit must be configued as a percentage of total available memory or as GB value");
        }
    }
}
