/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.COWSortedMap;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Throwables;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.voltdb.compiler.deploymentfile.PropertyType;

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

    public static final String EXPORT_TO_TYPE = "__EXPORT_TO_TYPE__";

    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    // TODO remove this - there is only one generation!
    private final COWSortedMap<Long, StandaloneExportGeneration> m_generations
            = new COWSortedMap<Long, StandaloneExportGeneration>();

    /**
     * Connections OLAP loaders. Currently at most one loader allowed.
     * Supporting multiple loaders mainly involves reference counting
     * the EE data blocks and bookkeeping ACKs from processors.
     */
    AtomicReference<StandaloneExportDataProcessor> m_processor = new AtomicReference<StandaloneExportDataProcessor>();

    /** Obtain the global ExportManager via its instance() method */
    private static StandaloneExportManager m_self;

    private String m_loaderClass;

    private volatile Properties m_processorConfig = new Properties();

    public static AtomicBoolean m_exit = new AtomicBoolean(false);
    public static boolean shouldExit() {
        return m_exit.get();
    }

    /**
     * Construct ExportManager using catalog.
     */
    public static synchronized void initialize(
            String overflow, String exportConnectorClassName, List<PropertyType> exportConfiguration)
    {
        StandaloneExportManager em = new StandaloneExportManager(
                "org.voltdb.export.processors.StandaloneGuestProcessor", exportConnectorClassName, exportConfiguration);

        m_self = em;
        em.createInitialExportProcessor(overflow);
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
            String loaderClass, String exportConnectorClassName, List<PropertyType> exportConfiguration)
    {
        updateProcessorConfig(exportConnectorClassName, exportConfiguration);

        exportLog.info(String.format("Export is enabled and can overflow to %s.", "/tmp"));

        m_loaderClass = loaderClass;
    }

    private synchronized void createInitialExportProcessor(String overflow) {
        try {
            exportLog.info("Creating connector " + m_loaderClass);
            StandaloneExportDataProcessor newProcessor = null;
            final Class<?> loaderClass = Class.forName(m_loaderClass);
            newProcessor = (StandaloneExportDataProcessor) loaderClass.newInstance();
            newProcessor.addLogger(exportLog);
            newProcessor.setProcessorConfig(m_processorConfig);
            m_processor.set(newProcessor);

            File exportOverflowDirectory = new File(overflow);

            /*
             * If this is a catalog update providing an existing generation,
             * the persisted stuff has already been initialized
             */
            initializePersistedGenerations(exportOverflowDirectory);

            if (m_generations.isEmpty()) {
                System.out.println("Nothing loaded. exiting");
                return;
            }
            final StandaloneExportGeneration nextGeneration = m_generations.firstEntry().getValue();
            /*
             * For the newly constructed processor, provide it the oldest known generation
             */
            newProcessor.setExportGeneration(nextGeneration);
            newProcessor.readyForData();

            for (Map<String, ExportDataSource> sources : nextGeneration.getDataSourceByPartition().values()) {
                for (final ExportDataSource source : sources.values()) {
                    try {
                        source.acceptMastership();
                    } catch (Exception e) {
                        exportLog.error("Unable to start exporting", e);
                    }
                }
            }
        }
        catch (final ClassNotFoundException e) {
            exportLog.l7dlog( Level.ERROR, LogKeys.export_ExportManager_NoLoaderExtensions.name(), e);
            Throwables.propagate(e);
        }
        catch (final Exception e) {
            Throwables.propagate(e);
        }
    }

    private void initializePersistedGenerations(
            File exportOverflowDirectory) throws IOException {
        TreeSet<File> generationDirectories = new TreeSet<File>();
        for (File f : exportOverflowDirectory.listFiles()) {
            if (f.isDirectory()) {
                if (!f.canRead() || !f.canWrite() || !f.canExecute()) {
                    throw new RuntimeException("Can't one of read/write/execute directory " + f);
                }
                generationDirectories.add(f);
            }
        }

        //Only give the processor to the oldest generation
        for (File generationDirectory : generationDirectories) {
            StandaloneExportGeneration generation = new StandaloneExportGeneration(generationDirectory);

            if (generation.initializeGenerationFromDisk()) {
                // TODO remove m_generations
                m_generations.put(0L, generation);
            } else {
                String list[] = generationDirectory.list();
                if (list != null && list.length == 0) {
                    try {
                        VoltFile.recursivelyDelete(generationDirectory);
                    } catch (IOException ioe) {
                    }
                } else {
                    exportLog.error("Invalid export generation in overflow directory " + generationDirectory
                            + " this will need to be manually cleaned up. number of files left: "
                            + (list != null ? list.length : 0));
                }
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
        for (StandaloneExportGeneration generation : m_generations.values()) {
            generation.close(messenger);
        }
        m_generations.clear();
        m_loaderClass = null;
    }

    public static long getQueuedExportBytes(int partitionId, String signature) {
        StandaloneExportManager instance = instance();
        try {
            Map<Long, StandaloneExportGeneration> generations = instance.m_generations;
            if (generations.isEmpty()) {
                return 0;
            }

            long exportBytes = 0;
            for (StandaloneExportGeneration generation : generations.values()) {
                exportBytes += generation.getQueuedExportBytes( partitionId, signature);
            }

            return exportBytes;
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error(e);
        }
        return 0;
    }
}
