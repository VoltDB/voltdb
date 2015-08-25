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

import java.io.File;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltdb.compiler.deploymentfile.DiskLimitType;
import org.voltdb.compiler.deploymentfile.FeatureNameType;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

/**
 * Disk space monitoring related functionality of resource monitoring.
 */
public class DiskResourceChecker
{
    private static final VoltLogger m_logger = new VoltLogger("HOST");

    static FileCheckForTest s_testFileCheck; // used only for testing
    private final Map<FeatureNameType, FeatureDiskLimitConfig> m_configuredLimits = new HashMap<>();

    public DiskResourceChecker(SystemSettingsType systemSettings, PathsType pathsConfig)
    {
        findDiskLimitConfiguration(systemSettings, pathsConfig);
    }

    public boolean hasLimitsConfigured()
    {
        for (FeatureDiskLimitConfig config : m_configuredLimits.values())
        {
            if (config.m_diskSizeLimit > 0 || config.m_diskSizeLimitPerc > 0) {
                return true;
            }
        }

        return false;
    }

    public void logConfiguredLimits()
    {
        for (FeatureDiskLimitConfig config : m_configuredLimits.values())
        {
            if (config.m_diskSizeLimit > 0 || config.m_diskSizeLimitPerc > 0) {
                m_logger.info(config.m_featureName.value() + " on " + config.m_path + " configured with size limit: " +
                        (config.m_diskSizeLimit > 0 ? config.m_diskSizeLimit + "GB" : config.m_diskSizeLimitPerc + "%"));
            }
        }
    }

    public boolean isOverLimitConfiguration()
    {
        for (FeatureDiskLimitConfig config : m_configuredLimits.values())
        {
            if (config.m_diskSizeLimit <= 0 && config.m_diskSizeLimitPerc <= 0) {
                continue;
            }
            if (!isDiskAvailable(config.m_path, config.m_diskSizeLimitPerc, config.m_diskSizeLimit)) {
                m_logger.warn("Disk is over configured limits for feature " + config.m_featureName);
                return true;
            }
        }

        return false;
    }

    private void findDiskLimitConfiguration(SystemSettingsType systemSettings, PathsType pathsConfig)
    {
        // By now we know that resource monitor is not null
        DiskLimitType diskLimitConfig = systemSettings.getResourcemonitor().getDisklimit();
        if (diskLimitConfig==null) {
            return;
        }

        String defaultSize = diskLimitConfig.getSize();
        List<DiskLimitType.Feature> features = diskLimitConfig.getFeature();
        //If defaultsize is configured, that should be set as the limit size for all features not explicitly configured
        if ( (features==null || features.isEmpty()) && (defaultSize == null || defaultSize.trim().isEmpty()) ) {
            return;
        }

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
                m_configuredLimits.put(feature.getName(),
                        new FeatureDiskLimitConfig(feature.getName(), pathsConfig, size));
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Added disk usage limit configuration " + size + " for feature " + feature.getName());
                }
            }
        }

        if (defaultSize !=null && !defaultSize.trim().isEmpty())
        {
            EnumSet<FeatureNameType> forDefault = EnumSet.complementOf(configuredFeatures);
            for (FeatureNameType featureName : forDefault)
            {
                if (!isSupportedFeature(featureName)) {
                    continue;
                }
                m_configuredLimits.put(featureName,
                        new FeatureDiskLimitConfig(featureName, pathsConfig, defaultSize));
                if (m_logger.isDebugEnabled()) {
                    m_logger.debug("Added disk usage limit configuration " + defaultSize + " for feature " + featureName);
                }
            }
        }
    }

    private boolean isSupportedFeature(FeatureNameType featureName)
    {
        LicenseApi licenseApi = VoltDB.instance().getLicenseApi();
        switch(featureName)
        {
        case COMMANDLOG:
        case COMMANDLOGSNAPSHOT:
        case SNAPSHOTS:
            return licenseApi.isCommandLoggingAllowed();
        case DROVERFLOW:
            return licenseApi.isDrReplicationAllowed();
        case EXPORTOVERFLOW:
            return MiscUtils.isPro();
        default: return false;
        }
    }

    private boolean isDiskAvailable(File filePath, int percThreshold, double sizeThreshold)
    {
        boolean canWrite = (s_testFileCheck==null) ? filePath.canWrite() : s_testFileCheck.canWrite(filePath);
        if (!canWrite) {
            m_logger.warn("Invalid or readonly file path " + filePath);
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
            m_logger.warn(String.format("Disk space usage on %s is >= the threshold value of %s. Total space=%d, Used space=%d",
                    filePath,
                    (percThreshold > 0 ? percThreshold+"%" : sizeThreshold+"GB"), total, usedSpace));
            return false;
        } else {
            return true;
        }
    }

    private static File getPathForFeature(FeatureNameType featureName, PathsType pathsConfig)
    {
        // pathsConfig will never be null or something is really wrong
        File voltDbRoot = CatalogUtil.getVoltDbRoot(pathsConfig);
        switch(featureName) {
        case COMMANDLOG :
            return CatalogUtil.getCommandLog(pathsConfig.getCommandlog(), voltDbRoot);
        case COMMANDLOGSNAPSHOT :
            return CatalogUtil.getCommandLogSnapshot(pathsConfig.getCommandlogsnapshot(), voltDbRoot);
        case DROVERFLOW:
            return CatalogUtil.getDROverflow(pathsConfig.getDroverflow(), voltDbRoot);
        case EXPORTOVERFLOW:
            return CatalogUtil.getExportOverflow(pathsConfig.getExportoverflow(), voltDbRoot);
        case SNAPSHOTS:
            return CatalogUtil.getSnapshot(pathsConfig.getSnapshots(), voltDbRoot);
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

        FeatureDiskLimitConfig(FeatureNameType featureName, PathsType pathsConfig, String sizeConfig)
        {
            m_featureName = featureName;
            m_path = getPathForFeature(featureName, pathsConfig);
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
                        if (m_diskSizeLimitPerc > 100 || m_diskSizeLimitPerc < 0) {
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
        }
    }
}
