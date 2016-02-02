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

package org.voltdb.export;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.COWSortedMap;
import org.voltdb.VoltDB;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Preconditions;
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

    private final COWSortedMap<Long, StandaloneExportGeneration> m_generations
            = new COWSortedMap<Long, StandaloneExportGeneration>();
    /*
     * When a generation is drained store a the id so
     * we can tell if a buffer comes late
     */
    private final CopyOnWriteArrayList<Long> m_generationGhosts =
            new CopyOnWriteArrayList<Long>();

    private HostMessenger m_messenger;

    /**
     * Set of partition ids for which this export manager instance is master of
     */
    private final Set<Integer> m_masterOfPartitions = new HashSet<Integer>();

    /**
     * Thrown if the initial setup of the loader fails
     */
    public static class SetupException extends Exception {
        private static final long serialVersionUID = 1L;

        SetupException(final String msg) {
            super(msg);
        }

        SetupException(final Throwable cause) {
            super(cause);
        }
    }

    /**
     * Connections OLAP loaders. Currently at most one loader allowed.
     * Supporting multiple loaders mainly involves reference counting
     * the EE data blocks and bookkeeping ACKs from processors.
     */
    AtomicReference<StandaloneExportDataProcessor> m_processor = new AtomicReference<StandaloneExportDataProcessor>();

    /** Obtain the global ExportManager via its instance() method */
    private static StandaloneExportManager m_self;
    private final int m_hostId;

    private String m_loaderClass;

    private volatile Properties m_processorConfig = new Properties();

    /*
     * Issue a permit when a generation is drained so that when we are truncating if a generation
     * is completely truncated we can wait for the on generation drained task to finish.
     *
     * This eliminates a race with CL replay where it may do catalog updates and such while truncation
     * is still running on generation drained.
     */
    private final Semaphore m_onGenerationDrainedForTruncation = new Semaphore(0);

    public class GenerationDrainRunnable implements Runnable {

        private final StandaloneExportGeneration m_generation;

        public GenerationDrainRunnable(StandaloneExportGeneration generation) {
            m_generation = generation;
        }

        @Override
        public void run() {
            /*
             * Do all the work to switch to a new generation in the thread for the processor
             * of the old generation
             */
            StandaloneExportDataProcessor proc = m_processor.get();
            if (proc == null) {
                System.out.println("No export data processor found.");
                System.exit(1);
            }
            proc.queueWork(new Runnable() {
                @Override
                public void run() {
                    try {
                        rollToNextGeneration(m_generation);
                    } catch (RuntimeException e) {
                        exportLog.error("Error rolling to next export generation", e);
                    } catch (Exception e) {
                        exportLog.error("Error rolling to next export generation", e);
                    } finally {
                        m_onGenerationDrainedForTruncation.release();
                    }
                }

            });
        }

    }

    public static AtomicBoolean m_exit = new AtomicBoolean(false);
    public static boolean shouldExit() {
        return m_exit.get();
    }

    private void rollToNextGeneration(StandaloneExportGeneration drainedGeneration) throws Exception {
        StandaloneExportDataProcessor newProcessor = null;
        StandaloneExportDataProcessor oldProcessor = null;
        boolean doexit = false;
        synchronized (StandaloneExportManager.this) {
            boolean installNewProcessor = false;
            if (m_generations.containsKey(drainedGeneration.m_timestamp)) {
                m_generations.remove(drainedGeneration.m_timestamp);
                m_generationGhosts.add(drainedGeneration.m_timestamp);
                installNewProcessor = true;
                exportLog.info("Finished draining generation " + drainedGeneration.m_timestamp);
            } else {
                exportLog.warn("Finished draining a generation that is not known to export generations.");
            }

            try {
                if (m_loaderClass != null && !m_generations.isEmpty() && installNewProcessor) {
                    exportLog.info("Creating connector " + m_loaderClass);
                    final Class<?> loaderClass = Class.forName(m_loaderClass);
                    //Make it so
                    StandaloneExportGeneration nextGeneration = m_generations.firstEntry().getValue();
                    newProcessor = (StandaloneExportDataProcessor) loaderClass.newInstance();
                    newProcessor.addLogger(exportLog);
                    newProcessor.setExportGeneration(nextGeneration);
                    newProcessor.setProcessorConfig(m_processorConfig);
                    newProcessor.readyForData();

                    nextGeneration.kickOffLeaderElection();
                    oldProcessor = m_processor.getAndSet(newProcessor);
                } else {
                    //All drained
                    exportLog.info("Finished all generation after: " + drainedGeneration.m_timestamp);
                    doexit = true;
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Error creating next export processor", true, e);
            }
        }

        /*
         * The old processor should shutdown if we installed a new processor.
         */
        if (oldProcessor != null) {
            oldProcessor.shutdown();
        }
        if (doexit) {
            m_exit.set(true);
        }
    }

    /**
     * Construct ExportManager using catalog.
     * @param myHostId
     */
    public static synchronized void initialize(
            int myHostId, String overflow, String exportConnectorClassName, List<PropertyType> exportConfiguration)
            throws StandaloneExportManager.SetupException
    {
        StandaloneExportManager em = new StandaloneExportManager(myHostId,
                "org.voltdb.export.processors.StandaloneGuestProcessor", exportConnectorClassName, exportConfiguration);

        m_self = em;
        em.createInitialExportProcessor(overflow);
    }

    /**
     * Indicate to associated {@link StandaloneExportGeneration}s to become     * masters for the given partition id
     * @param partitionId
     */
    synchronized public void acceptMastership(int partitionId) {
        if (m_loaderClass == null) {
            return;
        }
        Preconditions.checkArgument(
                m_masterOfPartitions.add(partitionId),
                "can't acquire mastership twice for partition id: " + partitionId
                );
        exportLog.info("ExportManager accepting mastership for partition " + partitionId);
        /*
         * Only the first generation will have a processor which
         * makes it safe to accept mastership.
         */
        StandaloneExportGeneration gen = m_generations.firstEntry().getValue();
        if (gen != null) {
            gen.acceptMastershipTask(partitionId);
        } else {
            exportLog.info("Failed to run accept mastership tasks for partition: " + partitionId);
        }
    }

    /**
     * Get the global instance of the ExportManager.
     * @return The global single instance of the ExportManager.
     */
    public static StandaloneExportManager instance() {
        return m_self;
    }

    public static void setInstanceForTest(StandaloneExportManager self) {
        m_self = self;
    }

    protected StandaloneExportManager() {
        m_hostId = 0;
        m_messenger = null;
    }

    /**
     * Read the catalog to setup manager and loader(s)
     * @param siteTracker
     */
    private StandaloneExportManager(
            int myHostId, String loaderClass, String exportConnectorClassName, List<PropertyType> exportConfiguration)
            throws StandaloneExportManager.SetupException
    {
        m_hostId = myHostId;
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
            nextGeneration.kickOffLeaderElection();
            /*
             * If the oldest known generation was disk based,
             * and we are using server side export we need to kick off a leader election
             * to choose which server is going to export each partition
             */
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
            generation.setGenerationDrainRunnable(new GenerationDrainRunnable(generation));

            if (generation.initializeGenerationFromDisk(null, m_messenger)) {
                m_generations.put( generation.m_timestamp, generation);
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

    public void shutdown() {
        StandaloneExportDataProcessor proc = m_processor.getAndSet(null);
        if (proc != null) {
            proc.shutdown();
        }
        for (StandaloneExportGeneration generation : m_generations.values()) {
            generation.close();
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
