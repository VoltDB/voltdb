/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.export;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.compiler.deploymentfile.PropertyType;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Throwables;

/**
 * Bridges the connection to an OLAP system and the buffers passed
 * between the OLAP connection and the execution engine. Each processor
 * implements ExportDataProcessor interface. The processors are passed one
 * or more ExportDataSources. The sources map, currently, 1:1 with Export
 * enabled tables. The ExportDataSource has poll() and ack() methods that
 * processors may use to pull and acknowledge as processed, EE Export data.
 * Data passed to processors is wrapped in ExportDataBlocks which in turn
 * wrap a BBContainer.
 *
 * Processors are loaded by reflection based on configuration in project.xml.
 */
public class StandaloneExportManager
{
    /**
     * the only supported processor class
     */
    public static final String PROCESSOR_CLASS =
            "org.voltdb.export.processors.StandaloneGuestProcessor";

    public static final String EXPORT_TO_TYPE = "__EXPORT_TO_TYPE__";

    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    private final AtomicReference<StandaloneExportGeneration> m_generation = new AtomicReference<>(null);

    /**
     * Connections OLAP loaders. Currently at most one loader allowed.
     * Supporting multiple loaders mainly involves reference counting
     * the EE data blocks and bookkeeping ACKs from processors.
     */
    AtomicReference<StandaloneExportDataProcessor> m_processor = new AtomicReference<StandaloneExportDataProcessor>();

    /** Obtain the global ExportManager via its instance() method */
    private static StandaloneExportManager m_self;

    private volatile Properties m_processorConfig = new Properties();

    public static long m_cdl;
    public static boolean shouldExit() {
        return m_cdl <= 0;
    }

    /**
     * Construct ExportManager using catalog.
     */
    // FIXME - this synchronizes on the ExportManager class, but everyone else synchronizes on the instance.
    public static synchronized void initialize(
            String overflow,
            String exportConnectorClassName,
            List<PropertyType> exportConfiguration)
    {
        StandaloneExportManager em = new StandaloneExportManager(exportConnectorClassName, exportConfiguration);

        m_cdl = em.createInitialExportProcessor(overflow);
        m_self = em;
    }

    /**
     * Get the global instance of the ExportManager.
     * @return The global single instance of the ExportManager.
     */
    public static StandaloneExportManager instance() {
        return m_self;
    }

    /**
     * Read the catalog to setup manager and loader(s)
     */
    private StandaloneExportManager(
            String exportConnectorClassName, List<PropertyType> exportConfiguration)
    {
        updateProcessorConfig(exportConnectorClassName, exportConfiguration);

        exportLog.info(String.format("Export is enabled and can overflow to %s.", "/tmp"));
    }

    private synchronized int createInitialExportProcessor(String overflow) {
        try {
            exportLog.info("Creating connector " + PROCESSOR_CLASS);
            final Class<?> loaderClass = Class.forName(PROCESSOR_CLASS);
            StandaloneExportDataProcessor newProcessor = (StandaloneExportDataProcessor) loaderClass.newInstance();
            newProcessor.addLogger(exportLog);
            newProcessor.setProcessorConfig(m_processorConfig);
            m_processor.set(newProcessor);

            initializePersistedGenerations(new File(overflow));

            StandaloneExportGeneration nextGeneration = m_generation.get();
            if (nextGeneration == null) {
                System.out.println("Nothing loaded. exiting");
                return 0;
            }
            newProcessor.setExportGeneration(nextGeneration);
            newProcessor.readyForData();

            int sz = nextGeneration.getDataSourceByPartition().values().size();
            for (Map<String, ExportDataSource> sources : nextGeneration.getDataSourceByPartition().values()) {
                for (final ExportDataSource source : sources.values()) {
                    try {
                        source.acceptMastership();
                    } catch (Exception e) {
                        exportLog.error("Unable to start exporting", e);
                    }
                }
            }
            return sz;
        }
        catch (final ClassNotFoundException e) {
            exportLog.l7dlog( Level.ERROR, LogKeys.export_ExportManager_NoLoaderExtensions.name(), e);
            Throwables.propagate(e);
        }
        catch (final Exception e) {
            Throwables.propagate(e);
        }
        return 0;
    }

    private void initializePersistedGenerations(File exportOverflowDirectory) throws IOException {

        File files[] = exportOverflowDirectory.listFiles();
        if (files == null) {
            //Clean export overflow no generations seen.
            return;
        }

        StandaloneExportGeneration generation = new StandaloneExportGeneration(exportOverflowDirectory);
        if (generation.initializeGenerationFromDisk()) {
            assert (m_generation.get() == null);
            m_generation.set(generation);
        } else {
            String list[] = exportOverflowDirectory.list();
            if (list != null && list.length == 0) {
                try {
                    VoltFile.recursivelyDelete(exportOverflowDirectory);
                } catch (IOException ioe) {
                }
            } else {
                exportLog.error("Invalid export generation in overflow directory " + exportOverflowDirectory
                        + " this will need to be manually cleaned up. number of files left: "
                        + (list != null ? list.length : 0));
            }
        }
    }

    public void updateProcessorConfig(String exportClassName, List<PropertyType> exportConfiguration) {
        Properties newConfig = new Properties();

        for (PropertyType prop : exportConfiguration) {
            newConfig.put(prop.getName(), prop.getValue());
        }
        newConfig.put(EXPORT_TO_TYPE, exportClassName);
        m_processorConfig = newConfig;
    }

    public void shutdown(final HostMessenger messenger) {
        StandaloneExportDataProcessor proc = m_processor.getAndSet(null);
        if (proc != null) {
            proc.shutdown();
        }
        StandaloneExportGeneration generation = m_generation.get();
        if (generation != null) {
            generation.close(messenger);
        }
    }

    public static long getQueuedExportBytes(int partitionId, String signature) {
        StandaloneExportManager instance = instance();
        try {
            StandaloneExportGeneration generation = instance.m_generation.get();
            if (generation != null) {
                return 0;
            }
            return generation.getQueuedExportBytes( partitionId, signature);
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error(e);
        }
        return 0;
    }
}
