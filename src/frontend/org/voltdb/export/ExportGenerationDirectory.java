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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.VoltFile;

/**
 *  An interface to list, claim and unclaim, create and delete
 *  export generations.
 */
public class ExportGenerationDirectory {

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    // Map generation id to generation.
    private final AtomicReference<TreeMap<Long, ExportGeneration>> m_library =
            new AtomicReference<TreeMap<Long, ExportGeneration>>(new TreeMap<Long, ExportGeneration>());

    // user-configured export overflow path
    final String m_overflowPath;

    // File representation of m_overflowPath
    final File m_exportOverflowDirectory;

    /** Create a new directory and correctly initialize on-disk content */
    public ExportGenerationDirectory(boolean isRejoin, CatalogContext context) throws IOException
    {
        m_overflowPath = context.cluster.getExportoverflow();
        m_exportOverflowDirectory = new File(m_overflowPath);

        // If a node is rejoining it is because it crashed. Export overflow isn't crash
        // safe. It isn't possible to recover valid/consistent data. Delete it instead.
        if (isRejoin) {
            deletePersistedGenerations();
        }
    }

    /** Produce a description of the available data sources. */
    List<ExportAdvertisement> createListing()
    {
        // for each generation, for each partition, for each table...
        TreeMap<Long,ExportGeneration> gens = m_library.get();
        LinkedList<ExportAdvertisement> list = new LinkedList<ExportAdvertisement>();
        for (Entry<Long, ExportGeneration> e : gens.entrySet()) {
            for(Entry<Integer, HashMap<String, ExportDataSource>> ds :
                e.getValue().m_dataSourcesByPartition.entrySet()) {
                for(Entry<String, ExportDataSource> d : ds.getValue().entrySet()) {
                    String signature = d.getKey();
                    int partition = d.getValue().getPartitionId();
                    long generation = d.getValue().getGeneration();
                    list.add(new ExportAdvertisement(generation, partition, signature));
                }
            }
        }
        return list;
    }

    /** Remove all on-disk generations or die trying. */
    public void deletePersistedGenerations()
    {
        File exportOverflowDirectory = new File(m_overflowPath);
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
                    VoltDB.crashLocalVoltDB("Failed to remove overflow directory", false, e);
                }
            }
        }
    }

    /** Initialize the directory from the on-disk contents */
    void initializePersistedWindows(Runnable onDrained) throws IOException
    {
        TreeSet<File> generationDirectories = new TreeSet<File>();
        for (File f : m_exportOverflowDirectory.listFiles()) {
            if (f.isDirectory()) {
                if (!f.canRead() || !f.canWrite() || !f.canExecute()) {
                    throw new RuntimeException("Can't one of read/write/execute directory " + f);
                }
                generationDirectories.add(f);
            }
        }

        for (File generationDirectory : generationDirectories) {
            ExportGeneration generation =
                new ExportGeneration(
                        onDrained,
                        generationDirectory,
                        Long.valueOf(generationDirectory.getName()));
            generation.initializeGenerationFromDisk();
            offer(Long.valueOf(generationDirectory.getName()), generation);
        }
    }

    /** Close all generations. */
    public void closeAllGenerations()
    {
        for (ExportGeneration generation : m_library.get().values()) {
            generation.close();
        }
    }

    /** Estimate total queued bytes in all generations. */
    public long estimateQueuedBytes(int partitionId, String signature)
    {
        TreeMap<Long, ExportGeneration> generations = m_library.get();
        if (generations.isEmpty()) {
            assert(false);
            return -1L;
        }

        long exportBytes = 0;
        for (ExportGeneration generation : generations.values()) {
            exportBytes += generation.getQueuedExportBytes( partitionId, signature);
        }
        return exportBytes;
    }

    /** Forget data that follows snapshotTxnId */
    public void truncateExportToTxnId(long snapshotTxnId)
    {
        for (ExportGeneration generation : m_library.get().values()) {
            generation.truncateExportToTxnId( snapshotTxnId);
        }
    }


    /** Get reference to a specific generation in the directory. */
    public ExportGeneration get(long id)
    {
        return m_library.get().get(id);
    }

    /** Get reference to first generation in the directory */
    public ExportGeneration peek()
    {
        return m_library.get().firstEntry().getValue();
    }

    /** Pop and return the oldest generation from the directory */
    public ExportGeneration pop()
    {
        ExportGeneration head = null;
        while (true) {
            TreeMap<Long, ExportGeneration> current = m_library.get();
            TreeMap<Long, ExportGeneration> copy = new TreeMap<Long, ExportGeneration>(current);
            head = copy.firstEntry().getValue();
            copy.remove(copy.firstEntry().getKey());

            if (m_library.compareAndSet(current, copy)) {
                break;
            }
        }
        return head;
    }

    /** Add a new generation to the directory */
    public void offer(long txnId, ExportGeneration exportGeneration)
    {
        while(true) {
            TreeMap<Long, ExportGeneration> current = m_library.get();
            TreeMap<Long, ExportGeneration> copy = new TreeMap<Long, ExportGeneration>(current);
            copy.put(txnId, exportGeneration);

            if (m_library.compareAndSet(current, copy)) {
                break;
            }
        }
    }

}
