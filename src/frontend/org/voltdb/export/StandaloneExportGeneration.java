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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.DBBPool;
import org.voltdb.catalog.Connector;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import java.util.Map;

/**
 * Export data from a single catalog version and database instance.
 *
 */
public class StandaloneExportGeneration implements Generation {
    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    public Long m_timestamp;
    public final File m_directory;

    /**
     * Data sources, one per table per site, provide the interface to
     * poll() and ack() Export data from the execution engines. Data sources
     * are configured by the Export manager at initialization time.
     * partitionid : <tableid : datasource>.
     */
    public final Map<Integer, Map<String, ExportDataSource>> m_dataSourcesByPartition =
        new HashMap<Integer, Map<String, ExportDataSource>>();

    @Override
    public Map<Integer, Map<String, ExportDataSource>> getDataSourceByPartition() {
        return m_dataSourcesByPartition;
    }

    private int m_numSources = 0;
    private final AtomicInteger m_drainedSources = new AtomicInteger(0);

    private Runnable m_onAllSourcesDrained = null;

    private final Runnable m_onSourceDrained = new Runnable() {
        @Override
        public void run() {
            if (m_onAllSourcesDrained == null) {
                System.out.println("No export generation roller found.");
                System.exit(1);
            }
            int numSourcesDrained = m_drainedSources.incrementAndGet();
            exportLog.info("Drained source in generation " + m_timestamp + " with " + numSourcesDrained + " of " + m_numSources + " drained");
            if (numSourcesDrained == m_numSources) {
                m_onAllSourcesDrained.run();
            }
        }
    };

    private volatile boolean shutdown = false;

    /**
     * Constructor to create a new generation of export data
     * @param exportOverflowDirectory
     * @throws IOException
     */
    public StandaloneExportGeneration(long txnId, File exportOverflowDirectory, boolean isRejoin) throws IOException {
        m_timestamp = txnId;
        m_directory = new File(exportOverflowDirectory, Long.toString(txnId));
        if (!isRejoin) {
            if (!m_directory.mkdirs()) {
                throw new IOException("Could not create " + m_directory);
            }
        } else {
            if (!m_directory.canWrite()) {
                if (!m_directory.mkdirs()) {
                    throw new IOException("Could not create " + m_directory);
                }
            }
        }
        exportLog.info("Creating new export generation " + m_timestamp);
    }

    /**
     * Constructor to create a generation based on one that has been persisted to disk
     * @param generationDirectory
     * @param generationTimestamp
     * @throws IOException
     */
    public StandaloneExportGeneration(File generationDirectory) throws IOException {
        m_directory = generationDirectory;
    }

    @Override
    public boolean isContinueingGeneration() {
        return false;
    }

    boolean initializeGenerationFromDisk(final Connector conn, HostMessenger ignored) {
        Set<Integer> partitions = new HashSet<Integer>();

        /*
         * Find all the advertisements. Once one is found, extract the nonce
         * and check for any data files related to the advertisement. If no data files
         * exist ignore the advertisement.
         */
        boolean hadValidAd = false;
        for (File f : m_directory.listFiles()) {
            if (f.getName().endsWith(".ad")) {
                boolean haveDataFiles = false;
                String nonce = f.getName().substring(0, f.getName().length() - 3);
                for (File dataFile : m_directory.listFiles()) {
                    if (dataFile.getName().startsWith(nonce) && !dataFile.getName().equals(f.getName())) {
                        haveDataFiles = true;
                        break;
                    }
                }

                if (haveDataFiles) {
                    try {
                        addDataSource(f, partitions);
                        hadValidAd = true;
                    } catch (IOException e) {
                        System.out.println("Error intializing export datasource " + f);
                        System.exit(1);
                    }
                } else {
                    //Delete ads that have no data
                    f.delete();
                }
            }
        }
        return hadValidAd;
    }


    /*
     * Run a leader election for every partition to determine who will
     * start consuming the export data.
     *
     */
    @Override
    public void kickOffLeaderElection() {
        for (Map<String, ExportDataSource> sources : getDataSourceByPartition().values()) {

            for (final ExportDataSource source : sources.values()) {
                try {
                    source.acceptMastership();
                } catch (Exception e) {
                    exportLog.error("Unable to start exporting", e);
                }
            }
        }
    }

    @Override
    public long getQueuedExportBytes(int partitionId, String signature) {
        //assert(m_dataSourcesByPartition.containsKey(partitionId));
        //assert(m_dataSourcesByPartition.get(partitionId).containsKey(delegateId));
        Map<String, ExportDataSource> sources = m_dataSourcesByPartition.get(partitionId);

        if (sources == null) {
            /*
             * This is fine. If the table is dropped it won't have an entry in the generation created
             * after the table was dropped.
             */
            //            exportLog.error("Could not find export data sources for generation " + m_timestamp + " partition "
            //                    + partitionId);
            return 0;
        }
        long qb = 0;
        for (ExportDataSource source : sources.values()) {
            if (source == null) continue;
            qb += source.sizeInBytes();
        }
        return qb;
    }

    /*
     * Create a datasource based on an ad file
     */
    private void addDataSource(
            File adFile,
            Set<Integer> partitions) throws IOException {
        m_numSources++;
        ExportDataSource source = new ExportDataSource( m_onSourceDrained, adFile, false);
        partitions.add(source.getPartitionId());
        m_timestamp = source.getGeneration();
        exportLog.info("Creating ExportDataSource for " + adFile + " table " + source.getTableName() +
                " signature " + source.getSignature() + " partition id " + source.getPartitionId() +
                " bytes " + source.sizeInBytes());
        Map<String, ExportDataSource> dataSourcesForPartition =
            m_dataSourcesByPartition.get(source.getPartitionId());
        if (dataSourcesForPartition == null) {
            dataSourcesForPartition = new HashMap<String, ExportDataSource>();
            m_dataSourcesByPartition.put(source.getPartitionId(), dataSourcesForPartition);
        }
        if (dataSourcesForPartition.get(source.getSignature()) != null) {
            exportLog.info("Existing ExportDataSource for " + adFile + " table " + source.getTableName()
                    + " signature " + source.getSignature() + " partition id " + source.getPartitionId()
                    + " bytes " + source.sizeInBytes());

            dataSourcesForPartition.put(source.getSignature(), source);
        } else {
            dataSourcesForPartition.put(source.getSignature(), source);
        }
    }

    /*
     * An unfortunate test only method for supplying a mock source
     */
    public void addDataSource(ExportDataSource source) {
        Map<String, ExportDataSource> dataSourcesForPartition =
            m_dataSourcesByPartition.get(source.getPartitionId());
        if (dataSourcesForPartition == null) {
            dataSourcesForPartition = new HashMap<String, ExportDataSource>();
            m_dataSourcesByPartition.put(source.getPartitionId(), dataSourcesForPartition);
        }
        dataSourcesForPartition.put(source.getSignature(), source);
    }

    @Override
    public void pushExportBuffer(int partitionId, String signature, long uso, ByteBuffer buffer, boolean sync, boolean endOfStream) {
        //        System.out.println("In generation " + m_timestamp + " partition " + partitionId + " signature " + signature + (buffer == null ? " null buffer " : (" buffer length " + buffer.remaining())));
        //        for (Integer i : m_dataSourcesByPartition.keySet()) {
        //            System.out.println("Have partition " + i);
        //        }
        assert(m_dataSourcesByPartition.containsKey(partitionId));
        assert(m_dataSourcesByPartition.get(partitionId).containsKey(signature));
        Map<String, ExportDataSource> sources = m_dataSourcesByPartition.get(partitionId);

        if (sources == null) {
            exportLog.error("Could not find export data sources for partition "
                    + partitionId + " generation " + m_timestamp + " the export data is being discarded");
            if (buffer != null) {
                DBBPool.wrapBB(buffer).discard();
            }
            return;
        }

        ExportDataSource source = sources.get(signature);
        if (source == null) {
            exportLog.error("Could not find export data source for partition " + partitionId +
                    " signature " + signature + " generation " +
                    m_timestamp + " the export data is being discarded");
            if (buffer != null) {
                DBBPool.wrapBB(buffer).discard();
            }
            return;
        }

        source.pushExportBuffer(uso, buffer, sync, endOfStream);
    }

    public void closeAndDelete() throws IOException {
        List<ListenableFuture<?>> tasks = new ArrayList<ListenableFuture<?>>();
        for (Map<String, ExportDataSource> map : m_dataSourcesByPartition.values()) {
            for (ExportDataSource source : map.values()) {
                tasks.add(source.closeAndDelete());
            }
        }
        try {
            Futures.allAsList(tasks).get();
        } catch (Exception e) {
            Throwables.propagateIfPossible(e, IOException.class);
        }
        shutdown = true;
        VoltFile.recursivelyDelete(m_directory);

    }

    /*
     * Returns true if the generatino was completely truncated away
     */
    @Override
    public boolean truncateExportToTxnId(long txnId, long[] perPartitionTxnIds) {
        return false;
    }

    @Override
    public void close() {
        List<ListenableFuture<?>> tasks = new ArrayList<ListenableFuture<?>>();
        for (Map<String, ExportDataSource> sources : m_dataSourcesByPartition.values()) {
            for (ExportDataSource source : sources.values()) {
                tasks.add(source.close());
            }
        }
        try {
            Futures.allAsList(tasks).get();
        } catch (Exception e) {
            //Logging of errors  is done inside the tasks so nothing to do here
            //intentionally not failing if there is an issue with close
            exportLog.error("Error closing export data sources", e);
        }
        shutdown = true;
    }

    /**
     * Indicate to all associated {@link ExportDataSource}to assume
     * mastership role for the given partition id
     * @param partitionId
     */
    @Override
    public void acceptMastershipTask( int partitionId) {
        Map<String, ExportDataSource> partitionDataSourceMap =
                m_dataSourcesByPartition.get(partitionId);

        // this case happens when there are no export tables
        if (partitionDataSourceMap == null) {
            return;
        }

        exportLog.info("Export generation " + m_timestamp + " accepting mastership for partition " + partitionId);
        for( ExportDataSource eds: partitionDataSourceMap.values()) {
            try {
                eds.acceptMastership();
            } catch (Exception e) {
                exportLog.error("Unable to start exporting", e);
            }
        }
    }

    @Override
    public String toString() {
        return "Export Generation - " + m_timestamp.toString();
    }

    public void setGenerationDrainRunnable(Runnable onGenerationDrained) {
        m_onAllSourcesDrained = onGenerationDrained;
    }

}
