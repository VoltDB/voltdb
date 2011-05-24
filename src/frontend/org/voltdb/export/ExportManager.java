/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.export;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.Database;
import org.voltdb.logging.Level;
import org.voltdb.logging.VoltLogger;
import org.voltdb.network.InputHandler;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltFile;

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
public class ExportManager
{

    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    private final AtomicReference<TreeMap< Long, ExportGeneration>> m_generations =
        new AtomicReference<TreeMap<Long, ExportGeneration>>(new TreeMap<Long, ExportGeneration>());

    /**
     * Thrown if the initial setup of the loader fails
     */
    public static class SetupException extends Exception {
        private static final long serialVersionUID = 1L;
        private final String m_msg;
        SetupException(final String msg) {
            m_msg = msg;
        }
        @Override
        public String getMessage() {
            return m_msg;
        }
    }

    /**
     * Connections OLAP loaders. Currently at most one loader allowed.
     * Supporting multiple loaders mainly involves reference counting
     * the EE data blocks and bookkeeping ACKs from processors.
     */
    AtomicReference<ExportDataProcessor> m_processor = new AtomicReference<ExportDataProcessor>();

    /** Obtain the global ExportManager via its instance() method */
    private static ExportManager m_self;
    private final int m_hostId;

    private String m_loaderClass;

    private final Runnable m_onGenerationDrained = new Runnable() {
        @Override
        public void run() {
            /*
             * Do all the work to switch to a new generation in the thread for the processor
             * of the old generation
             */
            ExportDataProcessor proc = m_processor.get();
            if (proc == null) {
                VoltDB.crashVoltDB();
            }
            proc.queueWork(new Runnable() {
                @Override
                public void run() {
                    TreeMap<Long, ExportGeneration> generations =
                        new TreeMap<Long, ExportGeneration>(m_generations.get());
                    ExportGeneration generation = generations.firstEntry().getValue();
                    generations.remove(generations.firstEntry().getKey());
                    exportLog.info("Finished draining generation " + generation.m_timestamp);
                    m_generations.set(generations);

                    exportLog.info("Creating connector " + m_loaderClass);
                    ExportDataProcessor newProcessor = null;
                    try {
                        final Class<?> loaderClass = Class.forName(m_loaderClass);
                        newProcessor = (ExportDataProcessor)loaderClass.newInstance();
                        newProcessor.addLogger(exportLog);
                        newProcessor.setExportGeneration(generations.firstEntry().getValue());
                        newProcessor.readyForData();
                    } catch (ClassNotFoundException e) {} catch (InstantiationException e) {
                        exportLog.error(e);
                    } catch (IllegalAccessException e) {
                        exportLog.error(e);
                    }

                    m_processor.getAndSet(newProcessor).shutdown();
                    try {
                        generation.closeAndDelete();
                    } catch (IOException e) {
                        e.printStackTrace();
                        exportLog.error(e);
                    }
                }
            });
        }
    };

    /**
     * Construct ExportManager using catalog.
     * @param myHostId
     * @param catalogContext
     * @throws ExportManager.SetupException
     */
    public static synchronized void initialize(int myHostId, CatalogContext catalogContext, boolean isRejoin)
    throws ExportManager.SetupException
    {
        /*
         * If a node is rejoining it is because it crashed. Export overflow isn't crash safe so it isn't possible
         * to recover the data. Delete it instead.
         */
        if (isRejoin) {
            deleteExportOverflowData(catalogContext);
        }
        ExportManager tmp = new ExportManager(myHostId, catalogContext);
        m_self = tmp;
    }

    private static void deleteExportOverflowData(CatalogContext context) {
        File exportOverflowDirectory = new File(context.cluster.getExportoverflow());
        exportLog.info("Deleting export overflow data from " + exportOverflowDirectory);
        if (!exportOverflowDirectory.exists()) {
            return;
        }
        File files[] = exportOverflowDirectory.listFiles();
        if (files != null) {
            for (File f : files) {
                try {
                    VoltFile.recursivelyDelete(f);
                } catch (IOException e) {
                    exportLog.fatal(e);
                    VoltDB.crashVoltDB();
                }
            }
        }
    }
    /**
     * Get the global instance of the ExportManager.
     * @return The global single instance of the ExportManager.
     */
    public static ExportManager instance() {
        assert (m_self != null);
        return m_self;
    }

    public static void setInstanceForTest(ExportManager self) {
        m_self = self;
    }

    protected ExportManager() {
        m_hostId = 0;
    }

    /**
     * Read the catalog to setup manager and loader(s)
     * @param siteTracker
     */
    private ExportManager(int myHostId, CatalogContext catalogContext)
    throws ExportManager.SetupException
    {
        m_hostId = myHostId;

        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        final Connector conn= db.getConnectors().get("0");

        if (conn == null) {
            exportLog.info("System is not using any export functionality.");
            return;
        }

        if (conn.getEnabled() == false) {
            exportLog.info("Export is disabled by user configuration.");
            return;
        }

        exportLog.info(String.format("Export is enabled and can overflow to %s.", cluster.getExportoverflow()));

        m_loaderClass = conn.getLoaderclass();

        try {
            exportLog.info("Creating connector " + m_loaderClass);
            ExportDataProcessor newProcessor = null;
            final Class<?> loaderClass = Class.forName(m_loaderClass);
            newProcessor = (ExportDataProcessor)loaderClass.newInstance();
            m_processor.set(newProcessor);
            newProcessor.addLogger(exportLog);
            File exportOverflowDirectory = new File(catalogContext.cluster.getExportoverflow());
            initializePersistedGenerations(
                    exportOverflowDirectory,
                    catalogContext, conn);
            ExportGeneration currentGeneration =
                new ExportGeneration( catalogContext.m_transactionId, m_onGenerationDrained, exportOverflowDirectory);
            currentGeneration.initializeGenerationFromCatalog(catalogContext, conn, m_hostId);
            m_generations.get().put( catalogContext.m_transactionId, currentGeneration);
            newProcessor.setExportGeneration(m_generations.get().firstEntry().getValue());
            newProcessor.readyForData();
        }
        catch (final ClassNotFoundException e) {
            exportLog.l7dlog( Level.ERROR, LogKeys.export_ExportManager_NoLoaderExtensions.name(), e);
            throw new ExportManager.SetupException(e.getMessage());
        }
        catch (final Exception e) {
            throw new ExportManager.SetupException(e.getMessage());
        }
    }



    private void initializePersistedGenerations(
            File exportOverflowDirectory, CatalogContext catalogContext,
            Connector conn) throws IOException {
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
            ExportGeneration generation =
                new ExportGeneration(
                        m_onGenerationDrained,
                        generationDirectory,
                        Long.valueOf(generationDirectory.getName()));
            generation.initializeGenerationFromDisk(conn);
            m_generations.get().put( Long.valueOf(generationDirectory.getName()), generation);
        }
    }

    public void notifyOfClusterTopologyChange() {
        exportLog.info("Attempting to boot export client due to rejoin or other cluster topology change");
        if (m_loaderClass == null) {
            return;
        }
        ExportDataProcessor proc = m_processor.get();
        while (proc == null) {
            Thread.yield();
            proc = m_processor.get();
        }
        proc.bootClient();
    }

    public void updateCatalog(CatalogContext catalogContext)
    {
        final Cluster cluster = catalogContext.catalog.getClusters().get("cluster");
        final Database db = cluster.getDatabases().get("database");
        final Connector conn= db.getConnectors().get("0");
        if (conn == null) {
            m_loaderClass = null;
            return;
        }

        m_loaderClass = conn.getLoaderclass();

        File exportOverflowDirectory = new File(catalogContext.cluster.getExportoverflow());

        ExportGeneration newGeneration = null;
        try {
            newGeneration = new ExportGeneration(
                    catalogContext.m_transactionId,
                    m_onGenerationDrained,
                    exportOverflowDirectory);
        } catch (IOException e1) {
            exportLog.error(e1);
            VoltDB.crashVoltDB();
        }
        newGeneration.initializeGenerationFromCatalog(catalogContext, conn, m_hostId);

        while (true) {
            TreeMap<Long, ExportGeneration> oldGenerations = m_generations.get();
            TreeMap<Long, ExportGeneration> generations = new TreeMap<Long, ExportGeneration>(oldGenerations);
            generations.put(catalogContext.m_transactionId, newGeneration);
            if (m_generations.compareAndSet( oldGenerations, generations)) {
                break;
            }
        }
        //
        //        Runnable switchToNextGeneration = new Runnable() {
        //            @Override
        //            public void run() {
        //                ExportDataProcessor processor = m_processors.peek();
        //                processor.shutdown();
        //
        //                ExportGeneration currentGeneration = m_generations.lastEntry().getValue();
        //                try {
        //                    currentGeneration.closeAndDelete();
        //                } catch (IOException e) {
        //                    exportLog.error(e);
        //                }
        //                m_generations.remove(m_generations.lastEntry().getKey());
        //
        //                ExportGeneration newGeneration = m_generations.firstEntry().getValue();
        //                newGeneration.registerWithProcessor(processor);
        //                processor.readyForData();
        //
        //                if (m_generations.size() > 1) {
        //                    processor.setOnAllSourcesDrained(this);
        //                }
        //            }
        //        };
        //
        //        m_processors.peek().setOnAllSourcesDrained(switchToNextGeneration);
    }

    public void shutdown() {
        ExportDataProcessor proc = m_processor.getAndSet(null);
        if (proc != null) {
            proc.shutdown();
        }
        for (ExportGeneration generation : m_generations.get().values()) {
            generation.close();
        }
    }

    /**
     * Factory for input handlers
     * @return InputHandler for new client connection
     */
    public InputHandler createInputHandler(String service, boolean isAdminPort)
    {
        ExportDataProcessor proc = m_processor.get();
        if (proc != null) {
            return proc.createInputHandler(service, isAdminPort);
        }
        return null;
    }


    /**
     * Map service strings to connector class names
     * @param service
     * @return classname responsible for service
     */
    public String getConnectorForService(String service) {
        ExportDataProcessor proc = m_processor.get();
        if (proc != null && proc.isConnectorForService(service)) {
            return proc.getClass().getCanonicalName();
        }
        return null;
    }

    public static long getQueuedExportBytes(int partitionId, String signature) {
        ExportManager instance = instance();
        try {
            TreeMap<Long, ExportGeneration> generations = instance.m_generations.get();
            if (generations.isEmpty()) {
                assert(false);
                return -1;
            }

            long exportBytes = 0;
            for (ExportGeneration generation : generations.values()) {
                exportBytes += generation.getQueuedExportBytes( partitionId, signature);
            }

            return exportBytes;
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error(e);
        }
        return 0;
    }

    /*
     * This method pulls double duty as a means of pushing export buffers
     * and "syncing" export data to disk. Syncing doesn't imply fsync, it just means
     * writing the data to a file instead of keeping it all in memory.
     * End of stream indicates that no more data is coming from this source
     * for this generation.
     */
    public static void pushExportBuffer(
            long exportGeneration,
            int partitionId,
            String signature,
            long uso,
            long bufferPtr,
            ByteBuffer buffer,
            boolean sync,
            boolean endOfStream) {
        ExportManager instance = instance();
        try {
            ExportGeneration generation = instance.m_generations.get().get(exportGeneration);
            if (generation == null) {
                assert(false);
                DBBPool.deleteCharArrayMemory(bufferPtr);
                exportLog.error("Could not a find an export generation " + exportGeneration +
                ". Should be impossible. Discarding export data");
                return;
            }

            generation.pushExportBuffer(partitionId, signature, uso, bufferPtr, buffer, sync, endOfStream);
        } catch (Exception e) {
            //Don't let anything take down the execution site thread
            exportLog.error(e);
        }
    }

    public void truncateExportToTxnId(long snapshotTxnId) {
        exportLog.info("Truncating export data after txnId " + snapshotTxnId);
        for (ExportGeneration generation : m_generations.get().values()) {
            generation.truncateExportToTxnId( snapshotTxnId);
        }
    }
}
