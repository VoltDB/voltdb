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

package org.voltdb;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.DRRoleStats.DRRole;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.compiler.deploymentfile.ResourceMonitorType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.snmp.FaultFacility;
import org.voltdb.snmp.SnmpTrapSender;
import org.voltdb.snmp.ThresholdType;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.SystemStatsCollector.Datum;

/**
 * Used to periodically check if the server's resource utilization is above the configured limits
 * and pause the server.
 */
public class HealthMonitor implements Runnable, Promotable
{
    private static final VoltLogger m_logger = new VoltLogger("HOST");

    private String m_rssLimitStr;
    private long m_rssLimit;
    private int m_resourceCheckInterval;
    private DiskResourceChecker m_diskLimitConfig;
    private boolean m_snmpMemoryTrapSent = false;
    private SnmpTrapSender m_snmpTrapSender;
    private String m_snmpRssLimitStr;
    private long m_snmpRssLimit;
    private ThresholdType m_snmpRssCriteria;
    private Map<Byte,Boolean> m_snmpDRTrapSent = new HashMap<>();
    private boolean m_isLeader;

    public HealthMonitor(SystemSettingsType systemSettings, SnmpTrapSender snmpTrapSender)
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

        m_diskLimitConfig = new DiskResourceChecker(systemSettings.getResourcemonitor().getDisklimit(), snmpTrapSender);

        // for snmp trap
        m_snmpTrapSender = snmpTrapSender;
        if (config.getMemorylimit() != null) {
            m_snmpRssLimitStr = config.getMemorylimit().getAlert().trim();
            // configured value is in GB. Convert it to bytes
            double dblLimit = getMemoryLimitSize(m_snmpRssLimitStr);
            m_snmpRssLimit = Double.valueOf(dblLimit).longValue();
            m_snmpRssCriteria = m_snmpRssLimitStr.endsWith("%") ? ThresholdType.PERCENT : ThresholdType.LIMIT;
        }
    }

    public boolean hasResourceLimitsConfigured()
    {
        return ((m_rssLimit > 0 || m_snmpRssLimit > 0 || (m_diskLimitConfig!=null && m_diskLimitConfig.hasLimitsConfigured()))
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
                m_logger.info("RSS limit: "  + getRssLimitLogString(m_rssLimit, m_rssLimitStr));
            }
            if (MiscUtils.isPro() && m_snmpRssLimit > 0) {
                m_logger.info("RSS SNMP notification limit: "  + getRssLimitLogString(m_snmpRssLimit, m_snmpRssLimitStr));
            }
            if (m_diskLimitConfig!=null) {
                m_diskLimitConfig.logConfiguredLimits();
            }
        } else {
            m_logger.info("No resource usage limit monitoring configured");
        }
    }

    private String getRssLimitLogString(long rssLimit, String rssLimitStr)
    {
        String rssWithUnit = getValueWithUnit(rssLimit);
        return (rssLimitStr.endsWith("%") ?
                rssLimitStr + " (" +  rssWithUnit + ")" : rssWithUnit);
    }

    @Override
    public void run()
    {
        if (getClusterOperationMode() != OperationMode.RUNNING) {
            return;
        }

        // check DRRole stats if it's responsible
        if (m_isLeader) {
            checkDRRole();
        }

        if (isOverMemoryLimit() || m_diskLimitConfig.isOverLimitConfiguration()) {
            SyncCallback cb = new SyncCallback();
            if (getConnectionHadler().callProcedure(getInternalUser(), true, BatchTimeoutOverrideType.NO_TIMEOUT, cb, "@Pause")) {
                try {
                    cb.waitForResponse();
                } catch (InterruptedException e) {
                    m_logger.error("Interrupted while pausing cluster for resource overusage", e);
                    return;
                }
                ClientResponse r = cb.getResponse();
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

    private void checkDRRole() {
        SyncCallback cb = new SyncCallback();
        if (getConnectionHadler().callProcedure(getInternalUser(), false, BatchTimeoutOverrideType.NO_TIMEOUT, cb, "@Statistics", "DRROLE",0)) {
            try {
                cb.waitForResponse();
            } catch (InterruptedException e) {
                m_logger.error("Interrupted while retrieving cluster for DRROLE STATS", e);
                return;
            }
            ClientResponse r = cb.getResponse();
            if (r.getStatus() != ClientResponse.SUCCESS) { // timeout could happen if hostdown
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Unable to retrieve DRROLE STATS: " + r.getStatusString());
                }
                return;
            }
            VoltTable result = r.getResults()[0];
            while (result.advanceRow()) {
                DrRoleType drRole = DrRoleType.fromValue(result.getString(DRRole.ROLE.name()).toLowerCase());
                DRRoleStats.State state = DRRoleStats.State.valueOf(result.getString(DRRole.STATE.name()));
                byte remoteCluster = (byte) result.getLong(DRRole.REMOTE_CLUSTER_ID.name());
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("DRROLE stats: Role:" + drRole + " State:" + state + " Remote Cluster ID:" + remoteCluster);
                }
                if (drRole == DrRoleType.NONE) {
                    continue;
                }

                if (DRRoleStats.State.STOPPED == state) {
                    if (!m_snmpDRTrapSent.getOrDefault(remoteCluster, false)) {
                        m_snmpTrapSender.statistics(FaultFacility.DR, String.format("Database Replication ROLE: %s break with Remote Cluster %d.",
                                drRole, remoteCluster));
                        m_snmpDRTrapSent.put(remoteCluster, true);
                    }
                } else {
                    // reset
                    if (m_snmpDRTrapSent.getOrDefault(remoteCluster, false)) {
                        m_snmpDRTrapSent.put(remoteCluster, false);
                    }
                }
            }
        } else {
            m_logger.error("Unable to retrieve DRROLE STATS:: failed to invoke @Statistics DRROLE, 0.");
        }
    }


    private boolean isOverMemoryLimit()
    {
        if (m_rssLimit<=0 && m_snmpRssLimit<=0) {
            return false;
        }

        Datum datum = SystemStatsCollector.getRecentSample();
        if (datum == null) { // this will be null if stats has not run yet
            m_logger.warn("No stats are available from stats collector. Skipping resource check.");
            return false;
        }

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("RSS=" + datum.rss + " Configured rss limit=" + m_rssLimit +
                    " Configured SNMP rss limit=" + m_snmpRssLimit);
        }

        if (MiscUtils.isPro()) {
            if (m_snmpRssLimit > 0 && datum.rss >= m_snmpRssLimit) {
                if (!m_snmpMemoryTrapSent) {
                    m_snmpTrapSender.resource(m_snmpRssCriteria, FaultFacility.MEMORY, m_snmpRssLimit, datum.rss,
                            String.format("SNMP resource limit exceeded. RSS limit %s on %s. Current RSS size %s.",
                                    getRssLimitLogString(m_snmpRssLimit, m_snmpRssLimitStr),
                                    CoreUtils.getHostnameOrAddress(), getValueWithUnit(datum.rss)));
                    m_snmpMemoryTrapSent = true;
                }
            } else {
                if (m_snmpRssLimit > 0 && m_snmpMemoryTrapSent) {
                    m_snmpTrapSender.resourceClear(m_snmpRssCriteria, FaultFacility.MEMORY, m_snmpRssLimit, datum.rss,
                            String.format("SNMP resource limit cleared. RSS limit %s on %s. Current RSS size %s.",
                                    getRssLimitLogString(m_snmpRssLimit, m_snmpRssLimitStr),
                                    CoreUtils.getHostnameOrAddress(), getValueWithUnit(datum.rss)));
                    m_snmpMemoryTrapSent = false;
                }
            }
        }

        if (m_rssLimit > 0 && datum.rss >= m_rssLimit) {
            m_logger.error(String.format(
                    "Resource limit exceeded. RSS limit %s on %s. Setting database to read-only. " +
                    "Please contact your administrator to clean up space via the admin port. " +
                    "Use \"voltadmin resume\" command once resource constraint is corrected.",
                    getRssLimitLogString(m_rssLimit,m_rssLimitStr), CoreUtils.getHostnameOrAddress()));
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

    @Override
    public void acceptPromotion() throws InterruptedException, ExecutionException, KeeperException {
        m_isLeader = true;
    }
}
