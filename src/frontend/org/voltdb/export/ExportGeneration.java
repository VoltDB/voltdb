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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.ConnectorTableInfo;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.VoltFile;

/**
 * Export data from a single catalog version and database instance.
 *
 */
public class ExportGeneration {
    /**
     * Processors also log using this facility.
     */
    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    public final Long m_timestamp;
    public final File m_directory;

    /**
     * Data sources, one per table per site, provide the interface to
     * poll() and ack() Export data from the execution engines. Data sources
     * are configured by the Export manager at initialization time.
     * partitionid : <tableid : datasource>.
     */
    public final HashMap<Integer, HashMap<String, ExportDataSource>> m_dataSourcesByPartition =
        new HashMap<Integer, HashMap<String, ExportDataSource>>();

    private int m_numSources = 0;
    private final AtomicInteger m_drainedSources = new AtomicInteger(0);

    private final Runnable m_onAllSourcesDrained;

    private final Runnable m_onSourceDrained = new Runnable() {
        @Override
        public void run() {
            int numSourcesDrained = m_drainedSources.incrementAndGet();
            exportLog.info("Drained source in generation " + m_timestamp + " with " + numSourcesDrained + " of " + m_numSources + " drained");
            if (numSourcesDrained == m_numSources) {
                m_onAllSourcesDrained.run();
            }
        }
    };

    /**
     * Constructor to create a new generation of export data
     * @param exportOverflowDirectory
     * @throws IOException
     */
    public ExportGeneration(long txnId, Runnable onAllSourcesDrained, File exportOverflowDirectory) throws IOException {
        m_onAllSourcesDrained = onAllSourcesDrained;
        m_timestamp = txnId;
        m_directory = new File(exportOverflowDirectory, Long.toString(txnId) );
        if (!m_directory.mkdir()) {
            throw new IOException("Could not create " + m_directory);
        }
        exportLog.info("Creating new export generation " + m_timestamp);
    }

    /**
     * Constructor to create a generation based on one that has been persisted to disk
     * @param generationDirectory
     * @param generationTimestamp
     * @throws IOException
     */
    public ExportGeneration(
            Runnable onAllSourcesDrained,
            File generationDirectory,
            long generationTimestamp) throws IOException {
        m_onAllSourcesDrained = onAllSourcesDrained;
        m_timestamp = generationTimestamp;
        m_directory = generationDirectory;
        exportLog.info("Restoring export generation " + generationTimestamp);
    }

    void initializeGenerationFromDisk(final Connector conn) {
        /*
         * Find all the advertisements. Once one is found, extract the nonce
         * and check for any data files related to the advertisement. If no data files
         * exist ignore the advertisement.
         */
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
                        addDataSource(f);
                    } catch (IOException e) {
                        exportLog.fatal(e);
                        VoltDB.crashVoltDB();
                    }
                } else {
                    //Delete ads that have no data
                    f.delete();
                }
            }
        }
    }


    void initializeGenerationFromCatalog(CatalogContext catalogContext,
            final Connector conn, int hostId)
    {
        /*
         * Now create datasources based on the catalog
         */
        Iterator<ConnectorTableInfo> tableInfoIt = conn.getTableinfo().iterator();
        while (tableInfoIt.hasNext()) {
            ConnectorTableInfo next = tableInfoIt.next();
            Table table = next.getTable();
            addDataSources(table, hostId, catalogContext);
        }

    }

    public long getQueuedExportBytes(int partitionId, String signature) {
        //assert(m_dataSourcesByPartition.containsKey(partitionId));
        //assert(m_dataSourcesByPartition.get(partitionId).containsKey(delegateId));
        HashMap<String, ExportDataSource> sources = m_dataSourcesByPartition.get(partitionId);

        if (sources == null) {
            /*
             * This is fine. If the table is dropped it won't have an entry in the generation created
             * after the table was dropped.
             */
            //            exportLog.error("Could not find export data sources for generation " + m_timestamp + " partition "
            //                    + partitionId);
            return 0;
        }

        ExportDataSource source = sources.get(signature);
        if (source == null) {
            /*
             * This is fine. If the table is dropped it won't have an entry in the generation created
             * after the table was dropped.
             */
            //exportLog.error("Could not find export data source for generation " + m_timestamp + " partition " + partitionId +
            //        " signature " + signature);
            return 0;
        }
        return source.sizeInBytes();
    }

    /*
     * Create a datasource based on an ad file
     */
    private void addDataSource(
            File adFile) throws IOException {
        m_numSources++;
        ExportDataSource source = new ExportDataSource( m_onSourceDrained, adFile);
        exportLog.info("Creating ExportDataSource for " + adFile + " table " + source.getTableName() +
                " signature " + source.getSignature() + " partition id " + source.getPartitionId() +
                " bytes " + source.sizeInBytes());
        HashMap<String, ExportDataSource> dataSourcesForPartition =
            m_dataSourcesByPartition.get(source.getPartitionId());
        if (dataSourcesForPartition == null) {
            dataSourcesForPartition = new HashMap<String, ExportDataSource>();
            m_dataSourcesByPartition.put(source.getPartitionId(), dataSourcesForPartition);
        }
        dataSourcesForPartition.put( source.getSignature(), source);
    }

    /*
     * An unfortunate test only method for supplying a mock source
     */
    public void addDataSource(ExportDataSource source) {
        HashMap<String, ExportDataSource> dataSourcesForPartition =
            m_dataSourcesByPartition.get(source.getPartitionId());
        if (dataSourcesForPartition == null) {
            dataSourcesForPartition = new HashMap<String, ExportDataSource>();
            m_dataSourcesByPartition.put(source.getPartitionId(), dataSourcesForPartition);
        }
        dataSourcesForPartition.put(source.getSignature(), source);
    }

    // silly helper to add datasources for a table catalog object
    private void addDataSources(
            Table table, int hostId, CatalogContext catalogContext)
    {
        SiteTracker siteTracker = catalogContext.siteTracker;
        ArrayList<Integer> sites = siteTracker.getLiveExecutionSitesForHost(hostId);

        for (Integer site : sites) {
            Integer partition = siteTracker.getPartitionForSite(site);
            /*
             * IOException can occur if there is a problem
             * with the persistent aspects of the datasource storage
             */
            try {
                HashMap<String, ExportDataSource> dataSourcesForPartition = m_dataSourcesByPartition.get(partition);
                if (dataSourcesForPartition == null) {
                    dataSourcesForPartition = new HashMap<String, ExportDataSource>();
                    m_dataSourcesByPartition.put(partition, dataSourcesForPartition);
                }
                ExportDataSource exportDataSource = new ExportDataSource(
                        m_onSourceDrained,
                        "database",
                        table.getTypeName(),
                        partition,
                        site,
                        table.getSignature(),
                        m_timestamp,
                        table.getColumns(),
                        m_directory.getPath());
                m_numSources++;
                exportLog.info("Creating ExportDataSource for table " + table.getTypeName() +
                        " signature " + table.getSignature() + " partition id " + partition);
                dataSourcesForPartition.put(table.getSignature(), exportDataSource);
            } catch (IOException e) {
                exportLog.fatal(e);
                VoltDB.crashVoltDB();
            }
        }
    }

    public void pushExportBuffer(int partitionId, String signature, long uso,
            long bufferPtr, ByteBuffer buffer, boolean sync, boolean endOfStream) {
        //        System.out.println("In generation " + m_timestamp + " partition " + partitionId + " signature " + signature + (buffer == null ? " null buffer " : (" buffer length " + buffer.remaining())));
        //        for (Integer i : m_dataSourcesByPartition.keySet()) {
        //            System.out.println("Have partition " + i);
        //        }
        assert(m_dataSourcesByPartition.containsKey(partitionId));
        assert(m_dataSourcesByPartition.get(partitionId).containsKey(signature));
        HashMap<String, ExportDataSource> sources = m_dataSourcesByPartition.get(partitionId);

        if (sources == null) {
            exportLog.error("Could not find export data sources for partition "
                    + partitionId + " generation " + m_timestamp + " the export data is being discarded");
            DBBPool.deleteCharArrayMemory(bufferPtr);
            return;
        }

        ExportDataSource source = sources.get(signature);
        if (source == null) {
            exportLog.error("Could not find export data source for partition " + partitionId +
                    " signature " + signature + " generation " +
                    m_timestamp + " the export data is being discarded");
            DBBPool.deleteCharArrayMemory(bufferPtr);
            return;
        }

        source.pushExportBuffer(uso, bufferPtr, buffer, sync, endOfStream);
    }

    public void closeAndDelete() throws IOException {
        for (HashMap<String, ExportDataSource> map : m_dataSourcesByPartition.values()) {
            for (ExportDataSource source : map.values()) {
                source.closeAndDelete();
            }
        }
        VoltFile.recursivelyDelete(m_directory);
    }

    public void truncateExportToTxnId(long txnId) {
        for (HashMap<String, ExportDataSource> dataSources : m_dataSourcesByPartition.values()) {
            for (ExportDataSource source : dataSources.values()) {
                source.truncateExportToTxnId(txnId);
            }
        }
    }

    public void close() {
        for (HashMap<String, ExportDataSource> sources : m_dataSourcesByPartition.values()) {
            for (ExportDataSource source : sources.values()) {
                source.close();
            }
        }
    }
}
