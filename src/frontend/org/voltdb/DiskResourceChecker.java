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

import java.io.File;
import java.util.EnumSet;
import java.util.List;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.compiler.deploymentfile.DiskLimitType;
import org.voltdb.compiler.deploymentfile.FeatureNameType;
import org.voltdb.licensing.Licensing;
import org.voltdb.snmp.FaultFacility;
import org.voltdb.snmp.SnmpTrapSender;
import org.voltdb.snmp.ThresholdType;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Disk space monitoring related functionality of resource monitoring.
 */
public class DiskResourceChecker
{
    private static final VoltLogger m_logger = new VoltLogger("HOST");

    static FileCheckForTest s_testFileCheck; // used only for testing
    private ImmutableMap<FeatureNameType, FeatureDiskLimitConfig> m_configuredLimits;
    private SnmpTrapSender m_snmpTrapSender;
    private boolean m_snmpDiskTrapSent = false;

    public DiskResourceChecker(DiskLimitType diskLimit, SnmpTrapSender snmpTrapSender) {
        findDiskLimitConfiguration(diskLimit);
        m_snmpTrapSender = snmpTrapSender;
        m_snmpDiskTrapSent = false;
    }

    public DiskResourceChecker(DiskLimitType diskLimit)
    {
        this(diskLimit, null);
    }

    public boolean hasLimitsConfigured()
    {
        if (m_configuredLimits==null) {
            return false;
        }

        for (FeatureDiskLimitConfig config : m_configuredLimits.values())
        {
            if (config.m_diskSizeLimit > 0 || config.m_diskSizeLimitPerc > 0 ||
                    config.m_diskSizeLimitSnmp > 0 || config.m_diskSizeLimitPercSnmp > 0) {
                return true;
            }
        }

        return false;
    }

    public void logConfiguredLimits()
    {
        if (m_configuredLimits==null) {
            return;
        }

        for (FeatureDiskLimitConfig config : m_configuredLimits.values())
        {
            if (config.m_diskSizeLimit > 0 || config.m_diskSizeLimitPerc > 0) {
                m_logger.info(config.m_featureName.value() + " on " + config.m_path + " configured with size limit: " +
                        (config.m_diskSizeLimit > 0 ? config.m_diskSizeLimit + "GB" : config.m_diskSizeLimitPerc + "%"));
            }
            if ((MiscUtils.isPro()) && (config.m_diskSizeLimitSnmp > 0 || config.m_diskSizeLimitPercSnmp > 0)) {
                m_logger.info(config.m_featureName.value() + " on " + config.m_path + " configured with SNMP notification limit: " +
                        (config.m_diskSizeLimitSnmp > 0 ? config.m_diskSizeLimitSnmp + "GB" : config.m_diskSizeLimitPercSnmp + "%"));
            }
        }
    }

    public boolean isOverLimitConfiguration() {
        if (m_configuredLimits == null) {
            return false;
        }

        for (FeatureDiskLimitConfig config : m_configuredLimits.values()) {
            if (config.m_diskSizeLimitSnmp > 0 || config.m_diskSizeLimitPercSnmp > 0) {
                isDiskAvailable(config.m_path, config.m_diskSizeLimitPercSnmp,
                        config.m_diskSizeLimitSnmp,
                        config.m_featureName, true);
            }

            if (config.m_diskSizeLimit <= 0 && config.m_diskSizeLimitPerc <= 0) {
                continue;
            }
            if (!isDiskAvailable(config.m_path, config.m_diskSizeLimitPerc, config.m_diskSizeLimit,
                    config.m_featureName)) {
                m_logger.error("Disk is over configured limits for feature " + config.m_featureName);
                return true;
            }
        }
        return false;
    }

    private void findDiskLimitConfiguration(DiskLimitType diskLimit)
    {
        // By now we know that resource monitor is not null
        DiskLimitType diskLimitConfig = diskLimit;
        if (diskLimitConfig==null) {
            return;
        }

        List<DiskLimitType.Feature> features = diskLimitConfig.getFeature();
        if (features==null || features.isEmpty()) {
            return;
        }

        ImmutableMap.Builder<FeatureNameType, FeatureDiskLimitConfig> builder = new ImmutableMap.Builder<>();
        EnumSet<FeatureNameType> configuredFeatures = EnumSet.noneOf(FeatureNameType.class);
        if (features!=null && !features.isEmpty())
        {
            for (DiskLimitType.Feature feature : features) {
                configuredFeatures.add(feature.getName());
                if (!isSupportedFeature(feature.getName())) {
                    m_logger.warn("Ignoring unsupported feature " + feature.getName());
                    continue;
                }
                String size = feature.getSize();
                String snmpSize = feature.getAlert();
                FeatureDiskLimitConfig aConfig =
                        new FeatureDiskLimitConfig(feature.getName(), size, snmpSize);
                builder.put(feature.getName(), aConfig);
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Added disk usage limit configuration " + (size == null? "no limit ": size) +
                            "snmp alert configuration " + (snmpSize == null? "no limit ": snmpSize) +
                            " for feature " + feature.getName());
                }
            }
        }

        m_configuredLimits = builder.build();
    }

    private boolean isSupportedFeature(FeatureNameType featureName)
    {
        switch(featureName)
        {
        case COMMANDLOG:
        case COMMANDLOGSNAPSHOT:
            return VoltDB.instance().getLicensing().isFeatureAllowed("CMDLOG");
        case DROVERFLOW:
            return VoltDB.instance().getLicensing().isFeatureAllowed("DR");
        case SNAPSHOTS:
        case EXPORTOVERFLOW:
        case TOPICSDATA:
            return true;
        default:
            return false;
        }
    }

    private boolean isDiskAvailable(File filePath, int percThreshold, double sizeThreshold, FeatureNameType featureName) {
        return isDiskAvailable(filePath, percThreshold, sizeThreshold, featureName, false);
    }

    private boolean isDiskAvailable(File filePath, int percThreshold, double sizeThreshold, FeatureNameType featureName, boolean forSnmp)
    {
        boolean canWrite = (s_testFileCheck==null) ? filePath.canWrite() : s_testFileCheck.canWrite(filePath);
        ThresholdType snmpCriteria = percThreshold > 0? ThresholdType.PERCENT:ThresholdType.LIMIT;
        if (!canWrite) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    String.format("Invalid or readonly file path %s (%s).",filePath, featureName.value()));
            return false;
        }

        long total = (s_testFileCheck==null) ? filePath.getTotalSpace() : s_testFileCheck.getTotalSpace(filePath);
        long usable = (s_testFileCheck==null) ? filePath.getUsableSpace() : s_testFileCheck.getUsableSpace(filePath);
        long calculatedThreshold = Math.round(sizeThreshold*1073741824);
        if (percThreshold > 0) {
            calculatedThreshold = Math.round(total*percThreshold/100.0);
        }
        long usedSpace = total - usable;
        if (m_logger.isDebugEnabled()) {
            m_logger.debug(
                String.format("File system for path %s has total space=%d and used space=%d. percentage-threshold=%d, size-threshold=%fGB, calculated-threshold=%d",
                        filePath, total, usedSpace, percThreshold, sizeThreshold, calculatedThreshold));
        }

        if (usedSpace >= calculatedThreshold) {
            if (forSnmp) {
                if (MiscUtils.isPro() && !m_snmpDiskTrapSent) {
                    m_snmpTrapSender.resource(snmpCriteria, FaultFacility.DISK, calculatedThreshold, usedSpace,
                                              String.format(
                                                  "SNMP resource limit exceeded. Disk for path %s (%s) limit %s on %s. Current disk usage is %s.",
                                                  filePath, featureName.value(),
                                                  (percThreshold > 0 ? percThreshold + "%" : sizeThreshold + " GB"),
                                                  CoreUtils.getHostnameOrAddress(), HealthMonitor.getValueWithUnit(usedSpace)));
                    m_snmpDiskTrapSent = true;
                }
            } else {
                m_logger.error(String.format(
                    "Resource limit exceeded. Disk for path %s (%s) limit %s on %s. Setting database to read-only. "
                            + "Use \"voltadmin resume\" command once resource constraint is corrected.",
                    filePath, featureName.value(), (percThreshold > 0 ? percThreshold + "%" : sizeThreshold + " GB"),
                    CoreUtils.getHostnameOrAddress()));
                m_logger.error(String.format("Resource limit exceeded. Current disk usage for path %s (%s) is %s.",
                    filePath, featureName.value(), HealthMonitor.getValueWithUnit(usedSpace)));
            }
            return false;
        } else {
            if (forSnmp && m_snmpDiskTrapSent) {
                m_snmpTrapSender.resourceClear(snmpCriteria, FaultFacility.DISK, calculatedThreshold, usedSpace,
                        String.format(
                                "SNMP resource limit cleared. Disk for path %s (%s) limit %s on %s. Current disk usage is %s.",
                                filePath, featureName.value(),
                                (percThreshold > 0 ? percThreshold + "%" : sizeThreshold + " GB"),
                                CoreUtils.getHostnameOrAddress(), HealthMonitor.getValueWithUnit(usedSpace)));
                m_snmpDiskTrapSent = false;
            }
            return true;
        }
    }

    private static File getPathForFeature(FeatureNameType featureName)
    {
        switch(featureName) {
        case COMMANDLOG :
            return new File(VoltDB.instance().getCommandLogPath());
        case COMMANDLOGSNAPSHOT :
            return new File(VoltDB.instance().getCommandLogSnapshotPath());
        case DROVERFLOW:
            return new File(VoltDB.instance().getDROverflowPath());
        case EXPORTOVERFLOW:
            return VoltDB.instance().getExportOverflowPath();
        case SNAPSHOTS:
            return new File(VoltDB.instance().getSnapshotPath());
        case TOPICSDATA:
            return VoltDB.instance().getTopicsDataPath();
        default: // Not a valid feature or one that is supported for disk limit monitoring.
                 // Should not happen unless we forget to add a newly supported feature here.
            return null;
        }
    }

    // Utility class that holds all the parameters for a feature
    private static class FeatureDiskLimitConfig
    {
        final FeatureNameType m_featureName;
        final File m_path;
        final double m_diskSizeLimit;
        final int m_diskSizeLimitPerc;
        final double m_diskSizeLimitSnmp;
        final int m_diskSizeLimitPercSnmp;

        FeatureDiskLimitConfig(FeatureNameType featureName, String sizeConfig, String sizeConfigSnmp)
        {
            m_featureName = featureName;
            m_path = getPathForFeature(featureName);
            if (m_path==null) {
                throw new IllegalArgumentException(
                        featureName + " is not a valid feature or not one supported for disk limit monitoring");
            }

            if (sizeConfig==null || sizeConfig.trim().isEmpty()) {
                m_diskSizeLimit = 0;
                m_diskSizeLimitPerc = 0;
            } else {
                String str = sizeConfig.trim();
                try {
                    if (str.charAt(str.length()-1) == '%') {
                        m_diskSizeLimit = 0;
                        m_diskSizeLimitPerc = Integer.parseInt(str.substring(0, str.length()-1));
                        if (m_diskSizeLimitPerc > 99 || m_diskSizeLimitPerc < 0) {
                            throw new IllegalArgumentException(
                                    "Invalid percentage value " + sizeConfig + " configured for disk limit size for feature " + featureName);
                        }
                    } else {
                        m_diskSizeLimit = Double.parseDouble(str);
                        m_diskSizeLimitPerc = 0;
                    }
                } catch(NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "Invalid value " + sizeConfig + " configured for disk limit size for feature " + featureName);
                }
            }

            if (sizeConfigSnmp==null || sizeConfigSnmp.trim().isEmpty()) {
                m_diskSizeLimitSnmp = 0;
                m_diskSizeLimitPercSnmp = 0;
            } else {
                String str = sizeConfigSnmp.trim();
                try {
                    if (str.charAt(str.length()-1) == '%') {
                        m_diskSizeLimitSnmp = 0;
                        m_diskSizeLimitPercSnmp = Integer.parseInt(str.substring(0, str.length()-1));
                        if (m_diskSizeLimitPerc > 99 || m_diskSizeLimitPerc < 0) {
                            throw new IllegalArgumentException(
                                    "Invalid percentage value " + sizeConfig + " configured for disk limit size for feature " + featureName);
                        }
                    } else {
                        m_diskSizeLimitSnmp = Double.parseDouble(str);
                        m_diskSizeLimitPercSnmp = 0;
                    }
                } catch(NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "Invalid value " + sizeConfigSnmp + " configured for disk snmp alert limit size for feature " + featureName);
                }
            }
        }
    }
}
